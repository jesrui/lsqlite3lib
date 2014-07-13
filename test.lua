local sqlite3 = require "sqlite3"

c = sqlite3.open('test.sqlite')

c:exec[[
	create table if not exists aaa(a number, b text);
	insert into aaa(a, b) values(9999, '*****');
	insert into aaa(a, b) values(1111, '@@@@@');
]]

c:set_trace_callback(function(s) print('log    :'.. s) end)
c:set_profile_callback(function(s, t) print('profile:'.. s .. ' [' .. t .. ']') end)

c:set_function("test1", 1, function(s)
	return s[1] .. '@'
end)

c:set_function("test2", 1, function(s)
	return s[1] .. '*'
end)

c:set_function("test2", 1, function(s)
	return s[1] .. '?'
end)

c:set_function("concat", -1, function(s)
	local ret = ''
	for k, v in pairs(s) do
		ret = ret .. v
	end
	return ret
end)

function f_step(s, r)
	for k, v in pairs(s) do
		r[1] = r[1] or 0
		r[1] = r[1] + v
	end
end

function f_final(r)
	return r[1]
end

c:set_aggregate("agg", -1, f_step, f_final)
--c:set_aggregate("agg")


c:exec("select test1('AAA'), test2('AAA'), concat('con', 'c', 'a', 't', 1 ,234, 5, NULL, '', ' aa'), agg(a) \"aaa\", agg(a+a) \"bbb\" from aaa;",
function(t)
	for k, v in pairs(t) do print(k, v) end
	return 0
end)

c:exec("select agg(a), agg(a,a) from aaa;",
function(t)
	for k, v in pairs(t) do print(k, v) end
	return 0
end)

c:begin()

p = c:prepare("insert into  aaa(a,b) values (:A, $B)")

p:bind {A = 100, B = 'hoge'}
print(p:exec_update())

p:bind {A = 200, B = 'foo'}
print(p:exec_update())

p:bind {300, 'bar'}
print(p:exec_update())

p:bind {[':A'] = 400, ['$B'] = '@@@'}
print(p:exec_update())

c:run_script('test.sql')

for row in c:prepare("select *, rowid from aaa"):rows() do
	print(" rows: "..row.a, row.b, row.rowid)
end

for row in c:prepare("select *, rowid from aaa"):irows() do
	print("irows: "..row[1], row[2], row[3])
end

p = c:prepare("select * from aaa")
row = p:fetch()
while row ~= nil do
	print(" fetch: " .. row.a, row.b)
	row = p:fetch()
end

p = c:prepare("select *, agg(a) from aaa")
row = p:ifetch()
while row do -- == while row ~= nil do
	print("ifetch: " .. row[1], row[2])
	row = p:ifetch()
end


c:exec([[
	select * from aaa
]],
function(t)
	for k, v in pairs(t) do print(' - ' .. k, v) end
	return 0
end)

c:exec('drop table aaa')

c:set_rollback_hook(function() print('rollback hook : rollback!') end)
c:set_commit_hook(function()
	print('commit hook : commit!')
	return 0
end)
--c:rollback()
c:commit()




c:close()
print(sqlite3.memory_used())
