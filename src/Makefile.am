ACLOCAL_AMFLAGS=-I m4

AM_CPPFLAGS = -DDATA_PATH=\"$(datadir)\"

noinst_HEADERS = which.hpp runProgram.hpp FDIO.hpp RunInfo.hpp

MANPAGES = 

man_MANS = ${MANPAGES}

data_DATA = derive.rc

EXTRA_DIST = ${MANPAGES} derive.rc
EXTRA_PROGRAMS =

bin_PROGRAMS = commandderive commandpack hpcschedcontrol hpcschedmake hpcschedworker showcdl hpcschedprocesslogs hpcscheddaligner

commandderive_SOURCES = commandderive.cpp
commandderive_LDADD = ${LIBMAUS2LIBS}
commandderive_LDFLAGS = ${AM_CPPFLAGS} ${LIBMAUS2CPPFLAGS} ${LIBMAUS2LDFLAGS} ${AM_LDFLAGS}
commandderive_CPPFLAGS = ${AM_CPPFLAGS} ${LIBMAUS2CPPFLAGS}

commandpack_SOURCES = commandpack.cpp
commandpack_LDADD = ${LIBMAUS2LIBS}
commandpack_LDFLAGS = ${AM_CPPFLAGS} ${LIBMAUS2CPPFLAGS} ${LIBMAUS2LDFLAGS} ${AM_LDFLAGS}
commandpack_CPPFLAGS = ${AM_CPPFLAGS} ${LIBMAUS2CPPFLAGS}

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

showcdl_SOURCES = showcdl.cpp
showcdl_LDADD = ${LIBMAUS2LIBS}
showcdl_LDFLAGS = ${AM_CPPFLAGS} ${LIBMAUS2CPPFLAGS} ${LIBMAUS2LDFLAGS} ${AM_LDFLAGS}
showcdl_CPPFLAGS = ${AM_CPPFLAGS} ${LIBMAUS2CPPFLAGS}

hpcschedprocesslogs_SOURCES = hpcschedprocesslogs.cpp
hpcschedprocesslogs_LDADD = ${LIBMAUS2LIBS}
hpcschedprocesslogs_LDFLAGS = ${AM_CPPFLAGS} ${LIBMAUS2CPPFLAGS} ${LIBMAUS2LDFLAGS} ${AM_LDFLAGS}
hpcschedprocesslogs_CPPFLAGS = ${AM_CPPFLAGS} ${LIBMAUS2CPPFLAGS}

hpcscheddaligner_SOURCES = hpcscheddaligner.cpp
hpcscheddaligner_LDADD = ${LIBMAUS2LIBS}
hpcscheddaligner_LDFLAGS = ${AM_CPPFLAGS} ${LIBMAUS2CPPFLAGS} ${LIBMAUS2LDFLAGS} ${AM_LDFLAGS}
hpcscheddaligner_CPPFLAGS = ${AM_CPPFLAGS} ${LIBMAUS2CPPFLAGS}
