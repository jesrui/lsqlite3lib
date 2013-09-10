#include "lsqlite3lib.h"

#include "lauxlib.h"
#include "sqlite3.h"
#include <string.h>

#define LUA_FUNC(f) static int f(lua_State* L)

typedef struct lsqlite3lib_conn conn;
typedef struct lsqlite3lib_stmt stmt;
typedef struct lsqlite3lib_func func;

#define MT_CONN "sqlite3:connection"
#define MT_STMT "sqlite3:prepared_statement"

#define IDX_STMT_TABLE     1
#define IDX_FUNCTION_TABLE 2
#define IDX_CALLBACK_TABLE 3

#define IDX_FUNC_ROLLBACK_HOOK    1
#define IDX_FUNC_COMMIT_HOOK      2
#define IDX_FUNC_TRACE_CALLBACK   3
#define IDX_FUNC_PROFILE_CALLBACK 4

#define IDX_FUNC_XFUNC            1
#define IDX_FUNC_XSTEP            2
#define IDX_FUNC_XFINAL           3

struct lsqlite3lib_conn {
	sqlite3* handle;
	lua_State* L;
	int ref;
};

struct lsqlite3lib_stmt {
	sqlite3_stmt* handle;
	conn* c;
};

struct lsqlite3lib_func {
	conn* c;
	char* func_name;
};

static int conn_open(lua_State* L, const char* filename) {
	conn* c = (conn*)lua_newuserdata(L, sizeof(conn));
	int ret = sqlite3_open(filename, &c->handle);

	if(ret != SQLITE_OK) {
		lua_pushfstring(L, "[%d] %s", ret, sqlite3_errmsg(c->handle));
		sqlite3_close(c->handle);
		return lua_error(L);
	}

	lua_createtable(L, 3, 0);

	lua_newtable(L);
	lua_rawseti(L, -2, IDX_STMT_TABLE);

	lua_newtable(L);
	lua_rawseti(L, -2, IDX_FUNCTION_TABLE);

	lua_newtable(L);
	lua_rawseti(L, -2, IDX_CALLBACK_TABLE);

	c->L = L;
	c->ref = luaL_ref(L, LUA_REGISTRYINDEX);


	luaL_setmetatable(L, MT_CONN);
	return 1;
}

LUA_FUNC(sqlite3lib_open) {
	const char* filename = luaL_checkstring(L, 1);
	return conn_open(L, filename);
}

LUA_FUNC(sqlite3lib_open_memory) {
	return conn_open(L, ":memory:");
}

LUA_FUNC(sqlite3lib_memory_used) {
	lua_pushinteger(L, sqlite3_memory_used());
	return 1;
}

LUA_FUNC(sqlite3lib_complete) {
	const char* sql = luaL_checkstring(L, 1);
	lua_pushboolean(L, sqlite3_complete(sql));
	return 1;
}

static const luaL_Reg sqlite3lib[] = {
	{"open", sqlite3lib_open},
	{"open_memory", sqlite3lib_open_memory},
	{"memory_used", sqlite3lib_memory_used},
	{"complete", sqlite3lib_complete},
	{NULL, NULL}
};

LUA_FUNC(connlib_close) {
	int ret;
	conn* c = (conn*)luaL_checkudata(L, 1, MT_CONN);

	lua_rawgeti(L, LUA_REGISTRYINDEX, c->ref);
	lua_rawgeti(L, -1, IDX_STMT_TABLE);

	lua_pushnil(L);
	while(lua_next(L, -2)) {
		stmt* s = luaL_testudata(L, -1, MT_STMT);
		if(s && s->handle) {
			if((ret = sqlite3_finalize(s->handle)) != SQLITE_OK) {
				return luaL_error(L, "[%d] %s", ret, sqlite3_errmsg(c->handle));
			}
			s->handle = NULL;
			s->c = NULL;
		}
		lua_pop(L, 1);
	}

	if((ret = sqlite3_close(c->handle)) != SQLITE_OK) {
		return luaL_error(L, "[%d] %s", ret, sqlite3_errmsg(c->handle));
	}
	luaL_unref(L, LUA_REGISTRYINDEX, c->ref);

	return 0;
}

LUA_FUNC(connlib_prepare) {
	conn* c = (conn*)luaL_checkudata(L, 1, MT_CONN);
	const char* sql = luaL_checkstring(L, 2);
	stmt* s = (stmt*)lua_newuserdata(L, sizeof(stmt));
	int ret;

	if((ret = sqlite3_prepare_v2(c->handle, sql, -1, &s->handle, NULL)) != SQLITE_OK) {
		lua_pushfstring(L, "[%d] %s", ret, sqlite3_errmsg(c->handle));
		sqlite3_finalize(s->handle);
		return lua_error(L);
	}
	s->c = c;

	lua_rawgeti(L, LUA_REGISTRYINDEX, c->ref);
	lua_rawgeti(L, -1, IDX_STMT_TABLE);

	lua_pushlightuserdata(L, s->handle);
	lua_pushvalue(L, -4);
	lua_rawset(L, -3);

	lua_pop(L, 2);

	luaL_setmetatable(L, MT_STMT);
	return 1;
}

int lsqlite3lib_exec_callback(void* p, int n,char** argv,char** colname) {
	int i;
	conn* c = (conn*)p;

	lua_pushvalue(c->L, 3); /* push callback function */

	lua_createtable(c->L, 0, n);
	for(i=0;i<n;i++) {
		lua_pushstring(c->L, colname[i]);
		lua_pushstring(c->L, argv[i]);
		lua_rawset(c->L, -3);
	}
	lua_call(c->L, 1, 1);
	int ret = lua_tointeger(c->L, -1);
	lua_pop(c->L, 1);
	return ret;
}

static int exec(lua_State* L, const char* sql) {
	conn* c = (conn*)luaL_checkudata(L, 1, MT_CONN);
	int has_callback = lua_isfunction(L, 3);
	char* errmsg;
	int ret;
	if(has_callback) {
		ret = sqlite3_exec(c->handle, sql, lsqlite3lib_exec_callback, c, &errmsg);
	} else {
		ret = sqlite3_exec(c->handle, sql, NULL, NULL, &errmsg);
	}
	if(ret != SQLITE_OK) {
		lua_pushfstring(L, "[%d] %s", ret, sqlite3_errmsg(c->handle));
		sqlite3_free(errmsg);
		return lua_error(L);
	}
	return 0;
}

LUA_FUNC(connlib_exec) {
	const char* sql = luaL_checkstring(L, 2);
	return exec(L, sql);
}

LUA_FUNC(connlib_run_script) {
	const char* filename = luaL_checkstring(L, 2);
	FILE* fp;
	luaL_Buffer b;
	char buf[256];

	if((fp = fopen(filename, "r")) == NULL) return luaL_error(L, "can't open %s", filename);
	lua_pop(L, 1);

	luaL_buffinit(L, &b);
	while(!feof(fp)) {
		int readsize = fread(buf, 1, sizeof(buf), fp);
		luaL_addlstring(&b, buf, readsize);
	}
	luaL_pushresult(&b);
	fclose(fp);
	return connlib_exec(L);
}

LUA_FUNC(connlib_begin) {
	return exec(L, "BEGIN");
}

LUA_FUNC(connlib_commit) {
	return exec(L, "COMMIT");
}

LUA_FUNC(connlib_rollback) {
	return exec(L, "ROLLBACK");
}


LUA_FUNC(connlib_in_transaction) {
	conn* c = (conn*)luaL_checkudata(L, 1, MT_CONN);
	lua_pushboolean(L, sqlite3_get_autocommit(c->handle) == 0 ? 1 : 0);
	return 1;
}


void lsqlite3lib_rollback_callback(void* p) {
	conn* c = (conn*)p;
	lua_rawgeti(c->L, LUA_REGISTRYINDEX, c->ref);
	lua_rawgeti(c->L, -1, IDX_CALLBACK_TABLE);

	lua_pushinteger(c->L, IDX_FUNC_ROLLBACK_HOOK);
	lua_rawget(c->L, -2); /* fnction */
	lua_call(c->L, 0, 0);
	lua_pop(c->L, 2);
}

LUA_FUNC(connlib_set_rollback_hook) {
	conn* c = (conn*)luaL_checkudata(L, 1, MT_CONN);

	if(lua_gettop(L) > 1 && lua_isfunction(L, 2)) {
		sqlite3_rollback_hook(c->handle, lsqlite3lib_rollback_callback, c);

		lua_rawgeti(L, LUA_REGISTRYINDEX, c->ref);
		lua_rawgeti(L, -1, IDX_CALLBACK_TABLE);

		lua_pushinteger(L, IDX_FUNC_ROLLBACK_HOOK);
		lua_pushvalue(L, 2); /* push function */
		lua_rawset(L, -3);

	} else {
		sqlite3_rollback_hook(c->handle, NULL, NULL);
	}
	return 0;
}

int lsqlite3lib_commit_callback(void* p) {
	int ret = 0;
	conn* c = (conn*)p;
	lua_rawgeti(c->L, LUA_REGISTRYINDEX, c->ref);
	lua_rawgeti(c->L, -1, IDX_CALLBACK_TABLE);

	lua_pushinteger(c->L, IDX_FUNC_COMMIT_HOOK);
	lua_rawget(c->L, -2); /* fnction */
	lua_call(c->L, 0, 1);
	ret = lua_tointeger(c->L, -1);

	lua_pop(c->L, 3);
	return ret;
}

LUA_FUNC(connlib_set_commit_hook) {
	conn* c = (conn*)luaL_checkudata(L, 1, MT_CONN);

	if(lua_gettop(L) > 1 && lua_isfunction(L, 2)) {
		sqlite3_commit_hook(c->handle, lsqlite3lib_commit_callback, c);

		lua_rawgeti(L, LUA_REGISTRYINDEX, c->ref);
		lua_rawgeti(L, -1, IDX_CALLBACK_TABLE);

		lua_pushinteger(L, IDX_FUNC_COMMIT_HOOK);
		lua_pushvalue(L, 2); /* push function */
		lua_rawset(L, -3);

	} else {
		sqlite3_commit_hook(c->handle, NULL, NULL);
	}
	return 0;
}

void lsqlite3lib_trace_callback(void* p, const char* sql) {
	conn* c = (conn*)p;
	lua_rawgeti(c->L, LUA_REGISTRYINDEX, c->ref);
	lua_rawgeti(c->L, -1, IDX_CALLBACK_TABLE);

	lua_pushinteger(c->L, IDX_FUNC_TRACE_CALLBACK);
	lua_rawget(c->L, -2); /* fnction */
	lua_pushstring(c->L, sql); /* arg */
	lua_call(c->L, 1, 0);

	lua_pop(c->L, 2);
}

LUA_FUNC(connlib_set_trace_callback) {
	conn* c = (conn*)luaL_checkudata(L, 1, MT_CONN);
	lua_rawgeti(L, LUA_REGISTRYINDEX, c->ref);
	if(lua_gettop(L) > 1 && lua_isfunction(L, 2)) {
		sqlite3_trace(c->handle, lsqlite3lib_trace_callback, c);

		lua_rawgeti(L, LUA_REGISTRYINDEX, c->ref);
		lua_rawgeti(L, -1, IDX_CALLBACK_TABLE);

		lua_pushinteger(L, IDX_FUNC_TRACE_CALLBACK);
		lua_pushvalue(L, 2); /* push function */
		lua_rawset(L, -3);

	} else {
		sqlite3_trace(c->handle, NULL, NULL);
	}
	return 0;
}

void lsqlite3lib_profile_callback(void* p,const char* sql,sqlite3_uint64 time) {
	conn* c = (conn*)p;
	lua_rawgeti(c->L, LUA_REGISTRYINDEX, c->ref);
	lua_rawgeti(c->L, -1, IDX_CALLBACK_TABLE);

	lua_pushinteger(c->L, IDX_FUNC_PROFILE_CALLBACK);
	lua_rawget(c->L, -2); /* fnction */
	lua_pushstring(c->L, sql); /* arg1 */
	lua_pushunsigned(c->L, time); /* arg2 */
	lua_call(c->L, 2, 0);

	lua_pop(c->L, 2);
}
LUA_FUNC(connlib_set_profile_callback) {
	conn* c = (conn*)luaL_checkudata(L, 1, MT_CONN);
	lua_rawgeti(L, LUA_REGISTRYINDEX, c->ref);
	if(lua_gettop(L) > 1 && lua_isfunction(L, 2)) {
		sqlite3_profile(c->handle, lsqlite3lib_profile_callback, c);

		lua_rawgeti(L, LUA_REGISTRYINDEX, c->ref);
		lua_rawgeti(L, -1, IDX_CALLBACK_TABLE);

		lua_pushinteger(L, IDX_FUNC_PROFILE_CALLBACK);
		lua_pushvalue(L, 2); /* push function */
		lua_rawset(L, -3);

	} else {
		sqlite3_profile(c->handle, NULL, NULL);
	}
	return 0;
}

void lsqlite3lib_xfunc_callback(sqlite3_context* ctx,int n, sqlite3_value** value) {
	int i;
	func* f = (func*)sqlite3_user_data(ctx);
	conn* c = f->c;
	lua_rawgeti(c->L, LUA_REGISTRYINDEX, c->ref);
	lua_rawgeti(c->L, -1, IDX_FUNCTION_TABLE);

	lua_pushstring(c->L, f->func_name);
	lua_rawget(c->L, -2);

	lua_rawgeti(c->L, -1, IDX_FUNC_XFUNC);

	lua_createtable(c->L, n, 0);
	for(i = 0; i < n; i++) {
		lua_pushinteger(c->L, i + 1);

		switch(sqlite3_value_type(*(value+i))) {
		case SQLITE_INTEGER:
			lua_pushinteger(c->L, sqlite3_value_int(*(value+i)));
			break;
		case SQLITE_FLOAT:
			lua_pushnumber(c->L, sqlite3_value_double(*(value+i)));
			break;
		case SQLITE3_TEXT:
			lua_pushstring(c->L, (const char*)sqlite3_value_text(*(value+i)));
			break;
		case SQLITE_BLOB:
		case SQLITE_NULL:
		default:
			lua_pushnil(c->L);
			break;
		}
		lua_rawset(c->L, -3);
	}

	lua_call(c->L, 1, 1);

	switch(lua_type(c->L, -1)) {
	case LUA_TBOOLEAN:
		sqlite3_result_int(ctx, lua_tointeger(c->L, -1));
		break;
	case LUA_TNUMBER:
		if(luaL_checknumber(c->L, -1) - luaL_checklong(c->L, -1) > 0) {
			sqlite3_result_double(ctx, lua_tonumber(c->L, -1));
		} else {
			sqlite3_result_int(ctx, lua_tointeger(c->L, -1));
		}
		break;
	case LUA_TSTRING: {
			const char* ret = lua_tostring(c->L, -1);
			sqlite3_result_text(ctx, ret, strlen(ret), SQLITE_TRANSIENT);
			break;
		}
	case LUA_TNIL:
	default:
		sqlite3_result_null(ctx);
		break;
	}
	lua_pop(c->L, 3);

}

void destroy_struct_func(void* p) {
	func* f = (func*)p;
	sqlite3_free(f->func_name);
	sqlite3_free(f);
}

LUA_FUNC(connlib_set_function) {
	func* f;
	conn* c = (conn*)luaL_checkudata(L, 1, MT_CONN);
	const char* func_name = luaL_checkstring(L, 2);

	if(lua_gettop(L) < 4 || lua_isnil(L, 3) || lua_isnil(L, 4)) {
		sqlite3_create_function_v2(c->handle,
				func_name,
				-1,
				SQLITE_ANY,
				NULL,
				NULL,
				NULL,
				NULL,
				NULL
		);
	} else {
		int n = lua_tointeger(L, 3);

		luaL_checktype(L, 4, LUA_TFUNCTION);
		lua_rawgeti(L, LUA_REGISTRYINDEX, c->ref);
		lua_rawgeti(L, -1, IDX_FUNCTION_TABLE);

		lua_pushstring(L, func_name);

		lua_newtable(L);
		lua_pushinteger(L, IDX_FUNC_XFUNC);
		lua_pushvalue(L, 4);

		lua_rawset(L, -3);
		lua_rawset(L, -3);

		f = sqlite3_malloc(sizeof(func));
		f->c = c;
		f->func_name = sqlite3_malloc(strlen(func_name) + 1);
		strcpy(f->func_name, func_name);

		sqlite3_create_function_v2(c->handle,
				func_name,
				n,
				SQLITE_UTF8,
				f,
				lsqlite3lib_xfunc_callback,
				NULL,
				NULL,
				destroy_struct_func
		);
	}

	return 0;
}


void lsqlite3lib_xstep_callback(sqlite3_context* ctx,int n, sqlite3_value** value) {
	int i;
	func* f = (func*)sqlite3_user_data(ctx);
	conn* c = f->c;
	int*  ref = sqlite3_aggregate_context(ctx, sizeof(int));
	if(!ref) sqlite3_result_error_nomem(ctx);

	if(*ref == 0) {
		lua_newtable(c->L);
		*ref = luaL_ref(c->L, LUA_REGISTRYINDEX);
	}

	lua_rawgeti(c->L, LUA_REGISTRYINDEX, c->ref);
	lua_rawgeti(c->L, -1, IDX_FUNCTION_TABLE);

	lua_pushstring(c->L, f->func_name);
	lua_rawget(c->L, -2);

	lua_rawgeti(c->L, -1, IDX_FUNC_XSTEP);


	lua_createtable(c->L, n, 0);
	for(i = 0; i < n; i++) {
		lua_pushinteger(c->L, i + 1);

		switch(sqlite3_value_type(*(value+i))) {
		case SQLITE_INTEGER:
			lua_pushinteger(c->L, sqlite3_value_int(*(value+i)));
			break;
		case SQLITE_FLOAT:
			lua_pushnumber(c->L, sqlite3_value_double(*(value+i)));
			break;
		case SQLITE3_TEXT:
			lua_pushstring(c->L, (const char*)sqlite3_value_text(*(value+i)));
			break;
		case SQLITE_BLOB:
		case SQLITE_NULL:
		default:
			lua_pushnil(c->L);
			break;
		}
		lua_rawset(c->L, -3);
	}
	lua_rawgeti(c->L, LUA_REGISTRYINDEX, *ref);

	lua_call(c->L, 2, 0);
}

void lsqlite3lib_xfinal_callback(sqlite3_context* ctx) {
	func* f = (func*)sqlite3_user_data(ctx);
	conn* c = f->c;
	int*  ref = sqlite3_aggregate_context(ctx, sizeof(int));
	if(!ref) sqlite3_result_error_nomem(ctx);

	lua_rawgeti(c->L, LUA_REGISTRYINDEX, c->ref);
	lua_rawgeti(c->L, -1, IDX_FUNCTION_TABLE);

	lua_pushstring(c->L, f->func_name);
	lua_rawget(c->L, -2);

	lua_rawgeti(c->L, -1, IDX_FUNC_XFINAL);

	lua_rawgeti(c->L, LUA_REGISTRYINDEX, *ref);

	lua_call(c->L, 1, 1);


	if(*ref != 0) {
		luaL_unref(c->L, LUA_REGISTRYINDEX, *ref);
		*ref = 0;
	}

	switch(lua_type(c->L, -1)) {
	case LUA_TBOOLEAN:
		sqlite3_result_int(ctx, lua_tointeger(c->L, -1));
		break;
	case LUA_TNUMBER:
		if(luaL_checknumber(c->L, -1) - luaL_checklong(c->L, -1) > 0) {
			sqlite3_result_double(ctx, lua_tonumber(c->L, -1));
		} else {
			sqlite3_result_int(ctx, lua_tointeger(c->L, -1));
		}
		break;
	case LUA_TSTRING: {
			const char* ret = lua_tostring(c->L, -1);
			sqlite3_result_text(ctx, ret, strlen(ret), SQLITE_TRANSIENT);
			break;
		}
	case LUA_TNIL:
	default:
		sqlite3_result_null(ctx);
		break;
	}
	lua_pop(c->L, 2);

}

LUA_FUNC(connlib_set_aggregate) {
	func* f;
	conn* c = (conn*)luaL_checkudata(L, 1, MT_CONN);
	const char* func_name = luaL_checkstring(L, 2);

	if(lua_gettop(L) < 5 || (lua_isnil(L, 4) && lua_isnil(L, 5))) {
		sqlite3_create_function_v2(c->handle,
				func_name,
				-1,
				SQLITE_ANY,
				NULL,
				NULL,
				NULL,
				NULL,
				NULL
		);

	} else {

		int n = lua_tointeger(L, 3);
		luaL_checktype(L, 4, LUA_TFUNCTION);
		luaL_checktype(L, 5, LUA_TFUNCTION);
		lua_rawgeti(L, LUA_REGISTRYINDEX, c->ref);
		lua_rawgeti(L, -1, IDX_FUNCTION_TABLE);

		lua_pushstring(L, func_name);

		lua_newtable(L);
		lua_pushinteger(L, IDX_FUNC_XSTEP);
		lua_pushvalue(L, 4);
		lua_rawset(L, -3);

		lua_pushinteger(L, IDX_FUNC_XFINAL);
		lua_pushvalue(L, 5);
		lua_rawset(L, -3);
		lua_rawset(L, -3);


		f = sqlite3_malloc(sizeof(func));
		f->c = c;
		f->func_name = sqlite3_malloc(strlen(func_name) + 1);
		strcpy(f->func_name, func_name);

		sqlite3_create_function_v2(c->handle,
				func_name,
				n,
				SQLITE_UTF8,
				f,
				NULL,
				lsqlite3lib_xstep_callback,
				lsqlite3lib_xfinal_callback,
				destroy_struct_func
		);
	}
	return 0;
}


LUA_FUNC(connlib_tostring) {
	conn* c = (conn*)luaL_checkudata(L, 1, MT_CONN);
	if (!c->handle)
		lua_pushfstring(L, "%s (closed)", MT_CONN);
	else
		lua_pushfstring(L, "%s (%p)", MT_CONN, c->handle);
	return 1;
}

static const luaL_Reg connlib[] = {
	{"close", connlib_close},

	{"prepare", connlib_prepare},
	{"exec", connlib_exec},
	{"run_script", connlib_run_script},

	{"begin", connlib_begin},
	{"commit", connlib_commit},
	{"rollback", connlib_rollback},
	{"in_transaction", connlib_in_transaction},

	{"set_rollback_hook", connlib_set_rollback_hook},
	{"set_commit_hook", connlib_set_commit_hook},
	{"set_trace_callback", connlib_set_trace_callback},
	{"set_profile_callback", connlib_set_profile_callback},

	{"set_function", connlib_set_function},
	{"set_aggregate", connlib_set_aggregate},

	{"__gc", connlib_close},
	{"__tostring", connlib_tostring},

	{NULL, NULL}
};

LUA_FUNC(stmtlib_sql) {
	stmt* s = (stmt*)luaL_checkudata(L, 1, MT_STMT);
	lua_pushstring(L, sqlite3_sql(s->handle));
	return 1;
}

LUA_FUNC(stmtlib_reset) {
	stmt* s = (stmt*)luaL_checkudata(L, 1, MT_STMT);
	sqlite3_reset(s->handle);
	return 0;
}

LUA_FUNC(stmtlib_bind) {
	int i;
	int param_count;
	stmt* s = (stmt*)luaL_checkudata(L, 1, MT_STMT);

	if(!lua_istable(L, 2)) {
		char msg[256];
		sprintf(msg, "table expected, got %s", lua_typename(L, 2));
		return luaL_argerror(L, 2, msg);
	}

	sqlite3_reset(s->handle);
	sqlite3_clear_bindings(s->handle);
	param_count = sqlite3_bind_parameter_count(s->handle);

	lua_pushnil(L);
	while (lua_next(L, 2) != 0) {
		int index = 0;
		const char* str;

		if(lua_isnumber(L, -2)) {
			index = lua_tointeger(L, -2);
		} else if(lua_isstring(L, -2)) {
			str = lua_tostring(L, -2);
			if(*str == ':' || *str == '@' || *str == '$') {
				index = sqlite3_bind_parameter_index(s->handle, str);
			} else {
				for(i=0;i<param_count;i++) {
					if(strcmp(sqlite3_bind_parameter_name(s->handle, i+1)+1, str) == 0) {
						index = i + 1;
						break;
					}
				}
			}
		}

		if(index != 0) {
			switch(lua_type(L, -1)) {
			case LUA_TNUMBER:
				sqlite3_bind_double(s->handle, index, lua_tonumber(L, -1));
				break;
			case LUA_TSTRING:
				str = lua_tostring(L, -1);
				sqlite3_bind_text(s->handle, index, str, strlen(str), SQLITE_TRANSIENT);
				break;
			case LUA_TNIL:
			default:
				sqlite3_bind_null(s->handle, index);
				break;
			}
		}
		lua_pop(L, 1);
	}
	return 0;
}

LUA_FUNC(stmtlib_exec_update) {
	stmt* s = (stmt*)luaL_checkudata(L, 1, MT_STMT);
	sqlite3* db = sqlite3_db_handle(s->handle);
	int ret;
	while((ret = sqlite3_step(s->handle)) == SQLITE_SCHEMA) {};
	if(ret != SQLITE_DONE && ret != SQLITE_ROW) {
		return luaL_error(L, "[%d] %s", ret, sqlite3_errmsg(db));
	}
	lua_pushinteger(L, sqlite3_changes(db));
	return 1;
}


LUA_FUNC(stmtlib_column_names) {
	stmt* s = (stmt*)luaL_checkudata(L, 1, MT_STMT);
	int col_count = sqlite3_column_count(s->handle);
	int i;
	lua_createtable(L, col_count, 0);
	for(i=0;i<col_count;i++) {
		lua_pushinteger(L, i + 1);
		lua_pushstring(L, sqlite3_column_name(s->handle, i));
		lua_settable(L, -3);
	}
	return 1;
}


static int column_types(lua_State* L, int mode) {
	stmt* s = (stmt*)luaL_checkudata(L, 1, MT_STMT);
	int col_count = sqlite3_column_count(s->handle);
	int i;

	lua_createtable(L, col_count, 0);
	for(i=0;i<col_count;i++) {
		if(mode == 0) {
			/* column_types */
			lua_pushstring(L, sqlite3_column_name(s->handle, i));
		} else {
			/* icolumn_types */
			lua_pushinteger(L, i + 1);
		}
		lua_pushinteger(L, sqlite3_column_type(s->handle, i));
		lua_settable(L, -3);
	}
	return 1;
}

LUA_FUNC(stmtlib_column_types) {
	return column_types(L, 0);
}

LUA_FUNC(stmtlib_icolumn_types) {
	return column_types(L, 1);
}

static int fetch(lua_State* L, int mode) {
	stmt* s = (stmt*)luaL_checkudata(L, 1, MT_STMT);
	sqlite3* db = sqlite3_db_handle(s->handle);
	int ret;
	while((ret = sqlite3_step(s->handle)) == SQLITE_SCHEMA) {}
	if(ret == SQLITE_DONE) {
		lua_pushnil(L);
		return 1;
	} else if(ret == SQLITE_ROW) {
		int i;
		int col_count = sqlite3_column_count(s->handle);
		lua_createtable(L, col_count, 0);
		for(i = 0; i < col_count; i++) {

			if(mode == 0) {
				/* fetch, rows */
				lua_pushstring(L, sqlite3_column_name(s->handle, i));
			} else if(mode == 1) {
				/* ifetch, irows */
				lua_pushinteger(L, i + 1);
			}

			switch(sqlite3_column_type(s->handle, i)) {
			case SQLITE_INTEGER:
				lua_pushinteger(L, sqlite3_column_int(s->handle, i));
				break;
			case SQLITE_FLOAT:
				lua_pushnumber(L, sqlite3_column_double(s->handle, i));
				break;
			case SQLITE_TEXT:
				lua_pushstring(L, (const char*)sqlite3_column_text(s->handle, i));
				break;
			case SQLITE_BLOB:
			case SQLITE_NULL:
				lua_pushnil(L);
				break;
			}
			lua_settable(L, -3);
		}
		return 1;

	}
	return luaL_error(L, "[%d] %s", ret, sqlite3_errmsg(db));
}

LUA_FUNC(stmtlib_fetch) {
	return fetch(L, 0);
}

LUA_FUNC(stmtlib_ifetch) {
	return fetch(L, 1);
}

LUA_FUNC(stmtlib_rows) {
	lua_pushcfunction(L, stmtlib_fetch);
	lua_pushvalue(L, 1);
	lua_pushnil(L);
	return 3;
}

LUA_FUNC(stmtlib_irows) {
	lua_pushcfunction(L, stmtlib_ifetch);
	lua_pushvalue(L, 1);
	lua_pushnil(L);
	return 3;
}

LUA_FUNC(stmtlib_finalize) {
	stmt* s = (stmt*)luaL_checkudata(L, 1, MT_STMT);

	if(s->handle) {
		sqlite3* db = sqlite3_db_handle(s->handle);
		int ret = sqlite3_finalize(s->handle);
		if(ret != SQLITE_OK) {
			return luaL_error(L, "[%d] %s", ret, sqlite3_errmsg(db));
		}

		lua_rawgeti(L, LUA_REGISTRYINDEX, s->c->ref);
		lua_rawgeti(L, -1, IDX_STMT_TABLE);

		lua_pushlightuserdata(L, s->handle);
		lua_pushnil(L);
		lua_rawset(L, -3);

		lua_pop(L, 2);

		s->handle = NULL;
		s->c = NULL;
	}

	return 0;
}

LUA_FUNC(stmtlib_tostring) {
	stmt* s = (stmt*)luaL_checkudata(L, 1, MT_STMT);
	if (!s->handle)
		lua_pushfstring(L, "%s (finalized)", MT_STMT);
	else
		lua_pushfstring(L, "%s (%p)", MT_STMT, s->handle);
	return 1;
}

static const luaL_Reg stmtlib[] = {
	{"sql", stmtlib_sql},
	{"reset", stmtlib_reset},

	{"bind", stmtlib_bind},

	{"exec_update", stmtlib_exec_update},

	{"column_names", stmtlib_column_names},

	{"column_types", stmtlib_column_types},
	{"icolumn_types", stmtlib_icolumn_types},
	{"fetch", stmtlib_fetch},
	{"ifetch", stmtlib_ifetch},
	{"rows", stmtlib_rows},
	{"irows", stmtlib_irows},

	{"finalize", stmtlib_finalize},

	{"__gc", stmtlib_finalize},
	{"__tostring", stmtlib_tostring},
	{NULL, NULL}
};

static void createmeta(lua_State* L, const char* mtname, const luaL_Reg funcs[]) {
	luaL_newmetatable(L, mtname); /* create metatable */
	lua_pushvalue(L, -1); /* push metatable */
	lua_setfield(L, -2, "__index"); /* metatable.__index = metatable */
	luaL_setfuncs(L, funcs, 0); /* add methods to new metatable */
	lua_pop(L, 1); /* pop new metatable */
}

int luaopen_sqlite3(lua_State* L) {

	luaL_newlib(L, sqlite3lib);

	lua_pushinteger(L, SQLITE_INTEGER);
	lua_setfield(L, -2, "INTEGER");
	lua_pushinteger(L, SQLITE_FLOAT);
	lua_setfield(L, -2, "FLOAT");
	lua_pushinteger(L, SQLITE_BLOB);
	lua_setfield(L, -2, "BLOB");
	lua_pushinteger(L, SQLITE_NULL);
	lua_setfield(L, -2, "NULL");
	lua_pushinteger(L, SQLITE3_TEXT);
	lua_setfield(L, -2, "TEXT");

	createmeta(L, MT_CONN, connlib);
	createmeta(L, MT_STMT, stmtlib);
	return 1;
}
