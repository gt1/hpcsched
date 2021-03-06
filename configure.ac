AC_INIT(hpcsched,0.0.23,[tischler@mpi-cbg.de],[hpcsched],[http://www.mpi-cbg.de])
AC_CANONICAL_SYSTEM
AC_PROG_LIBTOOL

AM_INIT_AUTOMAKE([std-options subdir-objects parallel-tests])
AM_CONFIG_HEADER(config.h)
AC_CONFIG_MACRO_DIR([m4])

AC_CANONICAL_SYSTEM

AC_ARG_ENABLE(optimization,
        AS_HELP_STRING([--enable-optimization],[use compiler optimization (default yes)]),
        [optimization=${enableval}],[optimization=yes])
AC_ARG_ENABLE(debug,
        AS_HELP_STRING([--enable-debug],[use compiler debug flags (default no)]),
        [debug=${enableval}],[debug=no])
AC_ARG_ENABLE(profile,
        AS_HELP_STRING([--enable-profile],[use compiler profiling flags (default no)]),
        [profile=${enableval}],[profile=no])
AC_ARG_ENABLE(fast,
        AS_HELP_STRING([--enable-fast],[disable evaluation of assertions (default no)]),
        [fast=${enableval}],[fast=no])

# check for C compiler
CFLAGS_SAVE="${CFLAGS}"
CFLAGS=""
AC_PROG_CC
CFLAGS="${CFLAGS_SAVE}"

CFLAGS_SAVE="${CFLAGS}"
CFLAGS=""
CXXFLAGS_SAVE="${CXXFLAGS}"
CXXFLAGS=""
AM_PROG_CC_C_O
CFLAGS="${CFLAGS_SAVE}"
CXXFLAGS="${CXXFLAGS_SAVE}"

# check for C++ compiler
CXXFLAGS_SAVE="${CXXFLAGS}"
CXXFLAGS=""
AC_PROG_CXX
CXXFLAGS="${CXXFLAGS_SAVE}"

if test \( ! -z "${GXX}" \) -a \( "${GXX}" = "yes" \) ; then
	GCC_VERSION=$(LC_ALL=C ${CXX} -v 2>&1 | awk '/gcc version/ {print $3}')
	GCC_VERSION_MAJOR=$(echo $GCC_VERSION | cut -d'.' -f1)
	GCC_VERSION_MINOR=$(echo $GCC_VERSION | cut -d'.' -f2)
	GCC_VERSION_PATCH=$(echo $GCC_VERSION | cut -d'.' -f3)
	AC_MSG_NOTICE([Found GNU c++ compiler ${GXX} version ${GCC_VERSION} major ${GCC_VERSION_MAJOR} minor ${GCC_VERSION_MINOR} patch level ${GCC_VERSION_PATCH}])

	if test \( ${GCC_VERSION_MAJOR} -lt 4 \) ; then
		AC_MSG_ERROR([GNU compiler version not supported (broken regex support).])
	elif test \( ${GCC_VERSION_MAJOR} -eq 4 \) -a \( ${GCC_VERSION_MINOR} -lt 9 \) ; then
		AC_MSG_ERROR([GNU compiler version not supported (broken regex support).])
	fi
fi

if test "${debug}" = "yes" ; then
	CXXDEBUGFLAGS="-g -O0 -rdynamic"
	CDEBUGFLAGS="-g -O0 -rdynamic"
        CXXFLAGS="${CXXFLAGS} ${CXXDEBUGFLAGS}"
        CFLAGS="${CFLAGS} ${CDEBUGFLAGS}"
	openmp="no"

	AC_MSG_NOTICE([Adding debug options ${CDEBUGFLAGS} to C compiler ${CC} flags, full list now ${CFLAGS}])
	AC_MSG_NOTICE([Adding debug options ${CXXDEBUGFLAGS} to C compiler ${CXX} flags, full list now ${CXXFLAGS}])
else
	if test "${profile}" = "yes" ; then
	        CXXPROFFLAGS="-g -pg -rdynamic"
	        CPROFFLAGS="-g -pg -rdynamic"

	        CXXFLAGS="${CXXPROFFLAGS} -g -pg -rdynamic"
	        CFLAGS="${CPROFFLAGS} -g -pg -rdynamic"

		AC_MSG_NOTICE([Adding profiling options ${CPROFFLAGS} to C compiler ${CC} flags, full list now ${CFLAGS}])
		AC_MSG_NOTICE([Adding profiling options ${CXXPROFFLAGS} to C compiler ${CXX} flags, full list now ${CXXFLAGS}])
	else
		if test "${optimization}" = "yes" ; then
			COPTFLAGS=
			case ${CC} in
				gcc)
					COPTFLAGS="-O3 -rdynamic"
					;;
				*-mingw32msvc-gcc)
					COPTFLAGS="-O3 -rdynamic"
					;;
				cl.exe)
					COPTFLAGS="-O2 -Ob2 -Ot -Oy"
					;;
			esac
			CFLAGS="${CFLAGS} ${COPTFLAGS}"
			AC_MSG_NOTICE([Adding optimisation options ${COPTFLAGS} to C compiler ${CC} flags, full list now ${CFLAGS}])

			CXXOPTFLAGS=
			case ${CXX} in
				g++)
					CXXOPTFLAGS="-O3 -rdynamic"
					;;
				*-mingw32msvc-g++)
					CXXOPTFLAGS="-O3 -rdynamic"
					;;
				cl.exe)
					CXXOPTFLAGS="-O2 -Ob2 -Ot -Oy"
					;;
			esac
			CXXFLAGS="${CXXFLAGS} ${CXXOPTFLAGS}"

			AC_MSG_NOTICE([Adding optimisation options ${CXXOPTFLAGS} to C++ compiler ${CXX} flags, full list now ${CXXFLAGS}])
		fi
	fi
fi


# check for pkg-config tool
PKG_PROG_PKG_CONFIG

if test "${fast}" = "yes" ; then
	CPPFLAGS="${CPPFLAGS} -DNDEBUG"
fi

# check for standard headers
AC_LANG_PUSH([C++])
AC_HEADER_STDC
AC_LANG_POP

if test "${CXX}" = "g++" ; then
	CPPFLAGS="${CPPFLAGS} -W -Wall"
fi

AC_ARG_WITH([libmaus2],
            [AS_HELP_STRING([--with-libmaus2@<:@=PATH@:>@], [path to installed libmaus2 library @<:@default=@:>@])],
            [with_libmaus2=${withval}],
            [with_libmaus2=])

if test ! -z "${with_libmaus2}" ; then
	PKGCONFIGPATHSAVE="${PKG_CONFIG_PATH}"
	if test -z "${PKG_CONFIG_PATH}" ; then
		export PKG_CONFIG_PATH="${with_libmaus2}/lib/pkgconfig"
	else
		export PKG_CONFIG_PATH="${with_libmaus2}/lib/pkgconfig:${PKG_CONFIG_PATH}"
	fi
fi

PKG_CHECK_MODULES([libmaus2],[libmaus2 >= 2.0.432])

if test ! -z "${with_libmaus2}" ; then
	if test ! -z "${PKGCONFIGPATHSAVE}" ; then
		export PKG_CONFIG_PATH="${PKGCONFIGPATHSAVE}"
	fi
fi

LIBMAUS2CPPFLAGS="${libmaus2_CFLAGS} ${libmaus2seqchksumsfactory_CFLAGS} ${libmaus2digests_CFLAGS}"
LIBMAUS2LIBS="${libmaus2_LIBS} ${libmaus2seqchksumsfactory_LIBS} ${libmaus2digests_LIBS}"

CPPFLAGS_SAVE="${CPPFLAGS}"
LDFLAGS_SAVE="${LDFLAGS}"
LIBS_SAVE="${LIBS}"

CPPFLAGS="${CPPFLAGS} ${libmaus2_CFLAGS}"
LIBS="${LIBS} ${libmaus2_LIBS}"

AC_LANG_PUSH([C++])
AC_MSG_CHECKING([whether we can compile a program using libmaus2])
	AC_LINK_IFELSE([AC_LANG_SOURCE([
#include <libmaus2/util/ArgInfo.hpp>

				int main(int argc, char * argv[[]]) {
					::libmaus2::util::ArgInfo const arginfo(argc,argv);
					return 0;
			}])],
			have_libmaus2=yes,
			have_libmaus2=no
		)
AC_LANG_POP
AC_MSG_RESULT($have_libmaus2)

if test "${have_libmaus2}" = "no" ; then
	AC_MSG_ERROR([Required libmaus2 is not available.])
fi

AC_ARG_WITH([slurm],
            [AS_HELP_STRING([--with-slurm@<:@=PATH@:>@], [path to installed slurm library @<:@default=@:>@])],
            [with_slurm=${withval}],
            [with_slurm=])

SLURMCPPFLAGS=
SLURMLDFLAGS=

if test "${with_slurm}" != "no" ; then
	CPPFLAGS_SAVE=${CPPFLAGS}
	LDFLAGS_SAVE=${LDFLAGS}
	

	if test "${with_slurm}" != "yes" ; then
		SLURMCPPFLAGS="${CPPFLAGS} -I${with_slurm}/include"
		SLURMLDFLAGS="${LDFLAGS} -L${with_slurm}/lib -Wl,-rpath=${with_slurm}/lib"
	fi

	CPPFLAGS="${CPPFLAGS} ${SLURMCPPFLAGS}"
	LDFLAGS="${LDFLAGS} ${SLURMLDFLAGS}"

	AC_CHECK_LIB([slurm],[slurm_api_version],[SLURMLIB=YES],[SLURMLIB=NO])

	if test "${SLURMLIB}" = "YES" ; then
		SLURMTEST=""
	fi
	
	CPPFLAGS=${CPPFLAGS_SAVE}
	LDFLAGS=${LDFLAGS_SAVE}
fi

AC_LANG_PUSH([C++])
AC_CHECK_FUNC(epoll_create1,have_epoll_create1=yes,have_epoll_create1=no)
AC_CHECK_FUNC(epoll_create,have_epoll_create=yes,have_epoll_create=no)
AC_LANG_POP

if test "${have_epoll_create1}" = "yes" ; then
	AC_DEFINE([HAVE_EPOLL_CREATE1],[1],[Define if epoll_create1 exists])
fi
if test "${have_epoll_create}" = "yes" ; then
	AC_DEFINE([HAVE_EPOLL_CREATE],[1],[Define if epoll_create exists])
fi

AC_MSG_NOTICE([Using flags ${CFLAGS} for C compiler ${CC}])
AC_MSG_NOTICE([Using flags ${CXXFLAGS} for C++ compiler ${CXX}])

PACKAGE_NAME=${PACKAGE}
PACKAGE_VERSION=${VERSION}

AC_SUBST([PACKAGE_NAME])
AC_SUBST([PACKAGE_VERSION])
# libmaus2
AC_SUBST([LIBMAUS2CPPFLAGS])
AC_SUBST([LIBMAUS2LIBS])
AC_SUBST([SLURMTEST])
AC_SUBST([SLURMCPPFLAGS])
AC_SUBST([SLURMLDFLAGS])
# 
AC_OUTPUT(Makefile src/Makefile)
