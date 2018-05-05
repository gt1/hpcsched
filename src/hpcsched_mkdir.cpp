/*
    libmaus2
    Copyright (C) 2018 German Tischler-Höhle

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
#include <libmaus2/types/types.hpp>
#include <libmaus2/exception/LibMausException.hpp>
#include <string>
#include <cstring>

#include <sys/stat.h>
#include <sys/types.h>

void smkdir(std::string const & sdir)
{
	int r = -1;

	while ( r < 0 )
	{
		r = mkdir(sdir.c_str(),0755);

		if ( r < 0 )
		{
			int const error = errno;

			switch ( error )
			{
				case EINTR:
				case EAGAIN:
					break;
				default:
				{
					libmaus2::exception::LibMausException lme;
					lme.getStream() << "[E] mkdir(" << sdir << "): " << strerror(error) << std::endl;
					lme.finish();
					throw lme;
				}
			}
		}
	}

}

int hpcsched_mkdir_cpp(char const * c, uint64_t const n)
{
	try
	{
		std::string const sdir(c,c+n);
		smkdir(sdir);
		return EXIT_SUCCESS;
	}
	catch(std::exception const & ex)
	{
		char const * what = ex.what();
		fprintf(stderr,"%s",what);
		return EXIT_FAILURE;
	}
}

extern "C" {

	int hpcsched_mkdir(char const * c, uint64_t const n)
	{
		return hpcsched_mkdir_cpp(c,n);
	}
}
