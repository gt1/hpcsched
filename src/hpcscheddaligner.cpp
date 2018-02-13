/*
    hpcsched
    Copyright (C) 2018 German Tischler

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
#include <regex>
#include <getopt.h>
#include <iostream>
#include <libmaus2/dazzler/db/DatabaseFile.hpp>
#include <libmaus2/util/ArgParser.hpp>

int main(int argc, char * argv[])
{
	try
	{
		libmaus2::util::ArgParser const arg(argc,argv);
		std::string const tmpfilebase =
			/* arg.uniqueArgPresent("T") ? arg["T"] : */ libmaus2::util::ArgInfo::getDefaultTmpFileName(arg.progname);

		char const * optlist = "vbAIk:w:h:t:M:P:e:l:s:H:T:m:B:";

		int BUNIT = 4;
		int optargc = argc;
		char ** optargv = argv;
		int opt = -1;
		int64_t numthreads = -1;
		int64_t maxmem = -1;

		std::ostringstream optsstr;
		std::string space;
		while ( (opt = getopt(optargc, optargv, optlist)) != -1 )
		{
			if ( opt == 'T' )
				numthreads = atol(optarg);
			if ( opt == 'M' )
				maxmem = atol(optarg) * 1024ull * 1024ull * 1024ull;

			switch ( opt )
			{
				case 'v':
				case 'b':
				case 'A':
				case 'I':
					optsstr << space << "-" << static_cast<char>(opt);
					space = std::string(" ");
					break;
				case 'k':
				case 'w':
				case 'h':
				case 't':
				case 'M':
				case 'P':
				case 'e':
				case 'l':
				case 's':
				case 'H':
				case 'T':
				case 'm':
					optsstr << space << "-" << static_cast<char>(opt) << optarg;
					space = std::string(" ");
					break;
				case 'B':
					BUNIT = atol(optarg);
					break;
				default:
				{
					std::cerr << "[E] unknown option or missing argument " << static_cast<char>(optopt) << std::endl;
					return EXIT_FAILURE;
				}
			}
		}

		if ( numthreads < 0 )
		{
			std::cerr << "[E] required argument -T not given" << std::endl;
			return EXIT_FAILURE;
		}

		if ( maxmem < 0 )
		{
			std::cerr << "[E] required argument -M not given" << std::endl;
			return EXIT_FAILURE;
		}

		std::string const opts = optsstr.str();

		std::vector < std::string > Varg;

		for ( int i = optind; i < argc; ++i )
			Varg.push_back(argv[optind]);

		if ( ! Varg.size() )
		{
			std::cerr << "[E] missing database option" << std::endl;
			return EXIT_FAILURE;
		}
		if ( Varg.size() > 2 )
		{
			std::cerr << "[E] more than two database arguments given" << std::endl;
			return EXIT_FAILURE;
		}

		libmaus2::dazzler::db::DatabaseFile::unique_ptr_type Pdba(
			new libmaus2::dazzler::db::DatabaseFile(Varg[0])
		);
		libmaus2::dazzler::db::DatabaseFile * pdba = Pdba.get();

		if ( pdba->cutoff < 0 )
		{
			std::cerr << "[E] database " << Varg[0] << " is not split, please run DBsplit" << std::endl;
			return EXIT_FAILURE;
		}
		pdba->computeTrimVector();

		libmaus2::dazzler::db::DatabaseFile::unique_ptr_type Pdbb;
		libmaus2::dazzler::db::DatabaseFile * pdbb = NULL;

		if ( Varg.size() > 1 )
		{
			libmaus2::dazzler::db::DatabaseFile::unique_ptr_type Tdbb(
				new libmaus2::dazzler::db::DatabaseFile(Varg[1])
			);
			Pdbb = UNIQUE_PTR_MOVE(Tdbb);
			pdbb = Pdbb.get();

			if ( pdbb->cutoff < 0 )
			{
				std::cerr << "[E] database " << Varg[0] << " is not split, please run DBsplit" << std::endl;
				return EXIT_FAILURE;
			}
			pdbb->computeTrimVector();

		}
		else
		{
			pdbb = Pdba.get();
		}

		std::string const dbapath = (pdba->path == ".") ? pdba->root : (pdba->path + "/" + pdba->root);
		std::string const dbbpath = (pdbb->path == ".") ? pdbb->root : (pdbb->path + "/" + pdbb->root);
		uint64_t nexttmpid = 0;

		if ( dbapath == dbbpath )
		{
			std::map < uint64_t, std::vector<std::string> > mergemap;

			std::cout << "#{{hpcschedflags}} {{deepsleep}} {{threads" << numthreads << "}}" << " {{mem" << maxmem/(1024*1024) << "}}\n";

			for ( uint64_t aid = 1; aid <= pdba->numblocks; ++aid )
			{
				// number of b blocks to be processed
				uint64_t const bnum = aid;
				// number of runs
				uint64_t const numsub = (bnum + BUNIT - 1)/BUNIT;
				assert ( numsub );
				// interval size
				uint64_t const bsub = (bnum + numsub - 1)/numsub;

				// std::cerr << aid << " bnum=" << bnum << " numsub=" << numsub << " bsub=" << bsub << std::endl;

				for ( uint64_t i = 0; i < numsub; ++i )
				{
					uint64_t const low = i * bsub;
					uint64_t const high = std::min(low + bsub, bnum);

					std::ostringstream ostr;
					ostr << "daligner " << opts << " " << dbapath << "." << aid;

					std::ostringstream prodostr;
					std::string prodspace;

					for ( uint64_t i = low; i < high; ++i )
					{
						uint64_t bid = i+1;
						ostr << " " << dbbpath << "." << bid;

						std::ostringstream fn0ostr;
						fn0ostr
							<< pdba->root << "." << aid
							<< "."
							<< pdbb->root << "." << bid
							<< ".las";
						std::string const fn0 = fn0ostr.str();

						mergemap[aid].push_back(fn0);

						prodostr << prodspace << fn0;
						prodspace = std::string(" ");

						std::ostringstream fn1ostr;
						fn1ostr
							<< pdbb->root << "." << bid
							<< "."
							<< pdba->root << "." << aid
							<< ".las";
						std::string const fn1 = fn1ostr.str();

						if ( fn1 != fn0 )
						{
							mergemap[bid].push_back(fn1);

							prodostr << prodspace << fn1;
							prodspace = std::string(" ");
						}
					}

					std::cout << prodostr.str() << ":\n";
					std::cout << "\t" << ostr.str() << "\n";
				}
			}

			std::cout << "#{{hpcschedflags}} {{deepsleep}}\n";

			typedef std::map < uint64_t, std::vector<std::string> >::iterator it;
			uint64_t const maxmerge = 3;

			for ( it itc = mergemap.begin(); itc != mergemap.end(); ++itc )
			{
				uint64_t const id = itc->first;
				std::vector < std::string > & V = itc->second;

				assert ( V.size() );

				while ( V.size() > maxmerge )
				{
					std::vector < std::string > Vout;
					uint64_t const numblocks = (V.size() + maxmerge - 1)/maxmerge;
					assert ( numblocks > 1 );

					uint64_t const fanin = (V.size() + numblocks - 1)/numblocks;

					for ( uint64_t i = 0; i < numblocks; ++i )
					{
						uint64_t const low = i * fanin;
						uint64_t const high = std::min(low+fanin,V.size());

						std::ostringstream fnostr;
						fnostr << tmpfilebase << "_" << nexttmpid++ << ".las";
						std::string const fn = fnostr.str();

						for ( uint64_t j = low; j < high; ++j )
							std::cout << V[j] << ".check:" << V[j] << "\n\tLAcheck -v " << pdba->dbpath << " " << pdbb->dbpath << " " << V[j] << "\n";

						std::cout << fn << ":";
						for ( uint64_t j = low; j < high; ++j )
							std::cout << " " << V[j] << ".check";
						std::cout << "\n\tLAmerge " << fn;
						for ( uint64_t j = low; j < high; ++j )
							std::cout << " " << V[j];
						std::cout << "\n";

						std::cout << "LAmerge_" << fn << "_cleanup: " << fn << "\n";
						for ( uint64_t j = low; j < high; ++j )
							std::cout << "\trm " << V[j] << "\n";

						Vout.push_back(fn);
					}

					V = Vout;
				}

				assert ( V.size() <= maxmerge );
				assert ( V.size() );

				{
					std::vector < std::string > Vout;

					uint64_t const low = 0;
					uint64_t const high = V.size();

					std::ostringstream fnostr;
					fnostr << pdba->root << "." << id << ".las";
					std::string const fn = fnostr.str();

					for ( uint64_t j = low; j < high; ++j )
						std::cout << V[j] << ".check:" << V[j] << "\n\tLAcheck -v " << pdba->dbpath << " " << pdbb->dbpath << " " << V[j] << "\n";

					std::cout << fn << ":";
					for ( uint64_t j = low; j < high; ++j )
						std::cout << " " << V[j] << ".check";
					std::cout << "\n\tLAmerge " << fn;
					for ( uint64_t j = low; j < high; ++j )
						std::cout << " " << V[j];
					std::cout << "\n";

					std::cout << "LAmerge_" << fn << "_cleanup: " << fn << "\n";
					for ( uint64_t j = low; j < high; ++j )
						std::cout << "\trm " << V[j] << "\n";

					Vout.push_back(fn);
					V = Vout;
				}

				assert ( V.size() == 1 );

				std::cout << V[0] << ".check:" << V[0] << "\n\tLAcheck -v " << pdba->dbpath << " " << pdbb->dbpath << " " << V[0] << "\n";
			}

			#if 0
			std::cout << "files:";
			for ( it itc = mergemap.begin(); itc != mergemap.end(); ++itc )
				std::cout << " " << itc->second.front() << ".check";
			std::cout << std::endl;
			#endif
		}
		else
		{
			std::cerr << "[E] asymmetric case not supported yet" << std::endl;
			return EXIT_FAILURE;
		}
	}
	catch(std::exception const & ex)
	{
		std::cerr << ex.what() << std::endl;
		return EXIT_FAILURE;
	}
}
