#include <stdio.h>
#include <stdlib.h>
#include "lsqlite3lib.h"
#include "lualib.h"
#include "lauxlib.h"

int main(int argc, char** argv) {
	lua_State* L = luaL_newstate();

	luaL_openlibs(L);


	luaL_requiref(L, "sqlite3", luaopen_sqlite3, 1);
	lua_pop(L, 1);  /* remove lib */

	/* add open functions from 'preloadedlibs' into 'package.preload' table */
	luaL_getsubtable(L, LUA_REGISTRYINDEX, "_PRELOAD");

	lua_pushcfunction(L, luaopen_sqlite3);
	lua_setfield(L, -2, "sqlite3");
	lua_pop(L, 1);  /* remove _PRELOAD table */



	/* luaL_dofile(L, "test.lua"); */
	luaL_loadfile(L, "test.lua");
	lua_call(L, 0, LUA_MULTRET);

	lua_close(L);
	return EXIT_SUCCESS;
}
