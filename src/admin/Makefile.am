MAINTAINERCLEANFILES = Makefile.in

AM_CPPFLAGS = -I $(top_srcdir)/src

AM_CFLAGS = -Wall -Wshadow
AM_CFLAGS += -Wno-unused-parameter -Wno-unused-value

noinst_LIBRARIES = libadmin.a

noinst_HEADERS = nc_admin.h

libadmin_a_SOURCES =	\
	nc_admin.c	\
	nc_admin_connection.c \
	nc_admin_poll.c
