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
#include <unistd.h>

void srmdir(std::string const & sdir)
{
	int r = -1;

	while ( r < 0 )
	{
		r = rmdir(sdir.c_str());

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
					lme.getStream() << "[E] rmdir(" << sdir << "): " << strerror(error) << std::endl;
					lme.finish();
					throw lme;
				}
			}
		}
	}

}

int hpcsched_rmdir_cpp(char const * c, uint64_t const n)
{
	try
	{
		std::string const sdir(c,c+n);
		srmdir(sdir);
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

	int hpcsched_rmdir(char const * c, uint64_t const n)
	{
		return hpcsched_rmdir_cpp(c,n);
	}
}
