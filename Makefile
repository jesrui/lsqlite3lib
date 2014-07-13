# makefile for lsqlite3lib library for Lua

ifneq "$(shell pkg-config --version)" ""
  # automagic setup (OS X fink, Linux apt-get, ..)
  #
  LUAINC= $(shell pkg-config --cflags lua)
  LUALIB= $(shell pkg-config --libs lua)
  LUAEXE= lua
  # Now, we actually want to _not_ push in stuff to the distro Lua CMOD directory,
  # way better to play within /usr/local/
  #LUACMOD= $(shell pkg-config --variable=INSTALL_CMOD lua)
  LUACMOD= /usr/local/lib/lua/5.2/
  #
  SQLITE3INC= $(shell pkg-config --cflags sqlite3)
  SQLITE3LIB= $(shell pkg-config --libs sqlite3)
else
  # manual setup (change these to reflect your Lua installation)
  #
  BASE= /usr/local
  LUAINC= -I$(BASE)/include
  LUAEXE= $(BASE)/bin/lua.exe
#  LUALIB= -L$(BASE)/lib -llua52
#  LUACMOD= $(BASE)/lib/lua/5.2/
#  Windows' LUA_CDIR and LUALIB are both the same as the Lua executable's directory...
  LUALIB= -L$(BASE)/bin -llua52
  LUACMOD= $(BASE)/bin
  #
  SQLITE3INC= -I$(BASE)/include
  SQLITE3LIB= -L$(BASE)/bin -lsqlite3
  #
  POD2HTML= perl -x -S doc/pod2html.pl
endif

TMP=./tmp
DISTDIR=./archive

# OS detection
#
SHFLAGS=-shared
UNAME= $(shell uname)
ifeq "$(UNAME)" "Linux"
  _SO=so
  SHFLAGS= -fPIC -shared
endif
ifneq "" "$(findstring BSD,$(UNAME))"
  _SO=so
endif
ifeq "$(UNAME)" "Darwin"
  _SO=bundle
  SHFLAGS= -bundle
endif
ifneq "" "$(findstring msys,$(OSTYPE))"		# 'msys'
  _SO=dll
endif

ifndef _SO
  $(error $(UNAME))
  $(error Unknown OS)
endif

# no need to change anything below here - HAH!
CFLAGS= $(INCS) $(DEFS) $(WARN) -O0 -g $(SHFLAGS)
WARN= -Wall #-ansi -pedantic -Wall
INCS= $(LUAINC) $(SQLITE3INC)
LIBS= $(LUALIB) $(SQLITE3LIB)

MYNAME= sqlite3
MYLIB= $(MYNAME)

OBJS= src/l$(MYLIB)lib.o
T= $(MYLIB).$(_SO)

all: $T

test: $T
	LUA_CPATH='./?.so' $(LUAEXE) test.lua

$T:	$(OBJS)
	$(CC) $(SHFLAGS) -o $@ $(OBJS) $(LIBS)

install:
	cp $T $(LUACMOD)

clean:
	rm -f $(OBJS) $T core core.* a.out test.db

.PHONY: all test clean install
