/*
    hpcsched
    Copyright (C) 2017 German Tischler

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

#include <config.h>

#include <which.hpp>
#include <RunInfo.hpp>

#include <libmaus2/util/ArgParser.hpp>
#include <libmaus2/util/ArgInfo.hpp>
#include <libmaus2/util/TempFileNameGenerator.hpp>
#include <libmaus2/aio/OutputStreamInstance.hpp>
#include <libmaus2/aio/OutputStreamFactoryContainer.hpp>
#include <libmaus2/util/WriteableString.hpp>
#include <libmaus2/network/Socket.hpp>
#include <libmaus2/util/GetFileSize.hpp>
#include <libmaus2/util/ContainerDescriptionList.hpp>
#include <libmaus2/util/CommandContainer.hpp>
#include <libmaus2/util/TarWriter.hpp>

int processlogs(libmaus2::util::ArgParser const & arg)
{
	std::string const cdl = arg[0];
	std::string const cdlmeta = cdl + ".meta";
	
	if ( libmaus2::util::GetFileSize::fileExists(cdlmeta) )
	{
		std::string const logtar = cdl + ".log.tar";
		libmaus2::util::TarWriter TW(logtar);
		
		libmaus2::aio::InputStreamInstance ISI(cdlmeta);
		
		std::map < std::pair<uint64_t,uint64_t>, uint64_t > IDM;
		
		while ( ISI && ISI.peek() != std::istream::traits_type::eof() )
		{
			RunInfo RI;
			RI.deserialise(ISI);

			libmaus2::autoarray::AutoArray<char> Aout(libmaus2::autoarray::AutoArray<char>::readFilePortion(RI.outfn,RI.outstart,RI.outend-RI.outstart));
			libmaus2::autoarray::AutoArray<char> Aerr(libmaus2::autoarray::AutoArray<char>::readFilePortion(RI.errfn,RI.errstart,RI.errend-RI.errstart));
			int const status = RI.status;
			
			uint64_t const fid = IDM[std::pair<uint64_t,uint64_t>(RI.containerid,RI.subid)]++;
			
			std::ostringstream fnostr;
			
			fnostr << "log_" << RI.containerid << "_" << RI.subid << "_" << fid;
			std::string const fnpref = fnostr.str();
			
			std::ostringstream statusstr;
			statusstr << status;
			
			TW.addFile(fnpref + ".out", std::string(Aout.begin(),Aout.end()));
			TW.addFile(fnpref + ".err", std::string(Aerr.begin(),Aerr.end()));
			TW.addFile(fnpref + ".status", statusstr.str());
		}
	}

	return EXIT_SUCCESS;
}

std::string getUsage(libmaus2::util::ArgParser const & arg)
{
	std::ostringstream ostr;
	ostr << "usage: " << arg.progname << " <cdl>" << std::endl;
	return ostr.str();
}

int main(int argc, char * argv[])
{
	try
	{
		libmaus2::util::ArgParser const arg(argc,argv);


		if ( arg.argPresent("h") || arg.argPresent("help") )
		{
			std::cerr << getUsage(arg);
			return EXIT_SUCCESS;
		}
		else if ( arg.argPresent("version") )
		{
			std::cerr << "This is " << PACKAGE_NAME << " version " << PACKAGE_VERSION << std::endl;
			return EXIT_SUCCESS;
		}
		else if ( arg.size() < 1 )
		{
			std::cerr << getUsage(arg);
			return EXIT_FAILURE;
		}

		int const r = processlogs(arg);

		return r;
	}
	catch(std::exception const & ex)
	{
		std::cerr << "[E] exception in main: " << ex.what() << std::endl;
		return EXIT_FAILURE;
	}
}
