ACLOCAL_AMFLAGS=-I m4

AM_CPPFLAGS = -DDATA_PATH=\"$(datadir)\"

noinst_HEADERS = which.hpp runProgram.hpp FDIO.hpp RunInfo.hpp

MANPAGES = 

man_MANS = ${MANPAGES}

data_DATA =

EXTRA_DIST = ${MANPAGES}
EXTRA_PROGRAMS =

bin_PROGRAMS = hpcschedcontrol hpcschedmake hpcschedworker hpcschedshowcdl hpcschedprocesslogs hpcscheddaligner

hpcsched_modules_LTLIBRARIES = hpcsched_mkdir.la hpcsched_rmdir.la
hpcsched_modulesdir = $(libdir)/hpcsched/$(PACKAGE_VERSION)

hpcsched_mkdir_la_CPPFLAGS = ${LIBMAUS2CPPFLAGS}
hpcsched_mkdir_la_CXXFLAGS = ${LIBMAUS2CXXFLAGS} ${AM_CXXFLAGS} -fPIC
hpcsched_mkdir_la_LDFLAGS  = ${LIBMAUS2LDFLAGS}  ${LIBMAUS2LIBS}     -module  -shared -export-dynamic ${AM_LDFLAGS} -avoid-version
hpcsched_mkdir_la_LIBADD   = ${LIBMAUS2LIBS}
hpcsched_mkdir_la_SOURCES  = hpcsched_mkdir.cpp

hpcsched_rmdir_la_CPPFLAGS = ${LIBMAUS2CPPFLAGS}
hpcsched_rmdir_la_CXXFLAGS = ${LIBMAUS2CXXFLAGS} ${AM_CXXFLAGS} -fPIC
hpcsched_rmdir_la_LDFLAGS  = ${LIBMAUS2LDFLAGS}  ${LIBMAUS2LIBS}     -module  -shared -export-dynamic ${AM_LDFLAGS} -avoid-version
hpcsched_rmdir_la_LIBADD   = ${LIBMAUS2LIBS}
hpcsched_rmdir_la_SOURCES  = hpcsched_rmdir.cpp

hpcschedcontrol_SOURCES = hpcschedcontrol.cpp which.cpp runProgram.cpp
hpcschedcontrol_LDADD = ${LIBMAUS2LIBS}
hpcschedcontrol_LDFLAGS = ${AM_CPPFLAGS} ${LIBMAUS2CPPFLAGS} ${LIBMAUS2LDFLAGS} ${AM_LDFLAGS}
hpcschedcontrol_CPPFLAGS = ${AM_CPPFLAGS} ${LIBMAUS2CPPFLAGS}

hpcschedmake_SOURCES = hpcschedmake.cpp which.cpp runProgram.cpp
hpcschedmake_LDADD = ${LIBMAUS2LIBS}
hpcschedmake_LDFLAGS = ${AM_CPPFLAGS} ${LIBMAUS2CPPFLAGS} ${LIBMAUS2LDFLAGS} ${AM_LDFLAGS}
hpcschedmake_CPPFLAGS = ${AM_CPPFLAGS} ${LIBMAUS2CPPFLAGS}

hpcschedworker_SOURCES = hpcschedworker.cpp which.cpp runProgram.cpp
hpcschedworker_LDADD = ${LIBMAUS2LIBS}
hpcschedworker_LDFLAGS = ${AM_CPPFLAGS} ${LIBMAUS2CPPFLAGS} ${LIBMAUS2LDFLAGS} ${AM_LDFLAGS}
hpcschedworker_CPPFLAGS = ${AM_CPPFLAGS} ${LIBMAUS2CPPFLAGS}

hpcschedshowcdl_SOURCES = hpcschedshowcdl.cpp
hpcschedshowcdl_LDADD = ${LIBMAUS2LIBS}
hpcschedshowcdl_LDFLAGS = ${AM_CPPFLAGS} ${LIBMAUS2CPPFLAGS} ${LIBMAUS2LDFLAGS} ${AM_LDFLAGS}
hpcschedshowcdl_CPPFLAGS = ${AM_CPPFLAGS} ${LIBMAUS2CPPFLAGS}

hpcschedprocesslogs_SOURCES = hpcschedprocesslogs.cpp
hpcschedprocesslogs_LDADD = ${LIBMAUS2LIBS}
hpcschedprocesslogs_LDFLAGS = ${AM_CPPFLAGS} ${LIBMAUS2CPPFLAGS} ${LIBMAUS2LDFLAGS} ${AM_LDFLAGS}
hpcschedprocesslogs_CPPFLAGS = ${AM_CPPFLAGS} ${LIBMAUS2CPPFLAGS}

hpcscheddaligner_SOURCES = hpcscheddaligner.cpp
hpcscheddaligner_LDADD = ${LIBMAUS2LIBS}
hpcscheddaligner_LDFLAGS = ${AM_CPPFLAGS} ${LIBMAUS2CPPFLAGS} ${LIBMAUS2LDFLAGS} ${AM_LDFLAGS}
hpcscheddaligner_CPPFLAGS = ${AM_CPPFLAGS} ${LIBMAUS2CPPFLAGS}
