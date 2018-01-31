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
#include <libmaus2/util/ArgParser.hpp>
#include <libmaus2/util/ArgInfo.hpp>
#include <libmaus2/util/LineBuffer.hpp>
#include <libmaus2/util/TempFileNameGenerator.hpp>
#include <libmaus2/util/Command.hpp>
#include <libmaus2/util/CommandContainer.hpp>
#include <libmaus2/util/ContainerDescriptionList.hpp>
#include <sstream>
#include <regex>

struct Token
{
	enum token_type { TOKEN_NEWLINE, TOKEN_SYMBOL, TOKEN_EOF, TOKEN_ESCAPE };
	token_type token;
	char sym;

	Token()
	{

	}

	Token(token_type const rtoken, char const c) : token(rtoken), sym(c) {}
};

Token getNextToken(std::string const & s, uint64_t & i)
{
	while ( i < s.size() )
	{
		// skip newline
		if ( s[i] == '\\' && i+1 < s.size() && s[i+1] == '\n' )
		{
			i += 2;
		}
		else if ( s[i] == '\\' && i+1 < s.size() && s[i+1] != '\n' )
		{
			Token tok(Token::TOKEN_ESCAPE,s[i+1]);
			i += 2;
			return tok;
		}
		else if ( s[i] == '\n' )
		{
			i += 1;
			return Token(Token::TOKEN_NEWLINE,'\n');
		}
		else
		{
			Token tok(Token::TOKEN_SYMBOL,s[i]);
			i += 1;
			return tok;
		}
	}

	return Token(Token::TOKEN_EOF,0);
}

struct Rule
{
	std::vector<std::string> produced;
	std::vector<std::string> dependencies;
	std::vector<std::string> commands;
	bool ignorefail;
	bool deepsleep;
	int64_t maxattempt;
	int64_t numthreads;
	int64_t mem;

	void clear()
	{
		produced.resize(0);
		dependencies.resize(0);
		commands.resize(0);
	}
};

std::ostream & operator<<(std::ostream & out, Rule const & R)
{
	out << "Rule:" << std::endl;
	for ( uint64_t i = 0; i < R.produced.size(); ++i )
		out << "\tproduced[" << i << "]=" << R.produced[i] << std::endl;
	for ( uint64_t i = 0; i < R.dependencies.size(); ++i )
		out << "\tdependencies[" << i << "]=" << R.dependencies[i] << std::endl;
	for ( uint64_t i = 0; i < R.commands.size(); ++i )
		out << "\t\tcommands[" << i << "]=" << R.commands[i] << std::endl;
	return out;
}

static std::vector<std::string> splitSpace(std::string const & s)
{
	uint64_t i = 0;

	std::vector<std::string> V;
	while ( i < s.size() )
	{
		while ( i < s.size() && isspace(s[i]) )
			++i;

		uint64_t const j = i;

		while ( i < s.size() && !isspace(s[i]) )
			++i;

		if ( i > j )
		{
			std::string const t(
				s.begin()+j,
				s.begin()+i
			);
			V.push_back(t);
		}
	}

	return V;
}

static uint64_t getDefaultMaxTry()
{
	return 2;
}

static uint64_t checkMaxTry(std::string const & s)
{
	std::regex R("\\{\\{maxtry(\\d+)\\}\\}");

	std::smatch sm;
	if ( ::std::regex_search(s, sm, R) )
	{
		std::istringstream istr(sm[1]);
		uint64_t i;
		istr >> i;

		if ( istr && istr.peek() == std::istream::traits_type::eof() )
		{
			return i;
		}
		else
		{
			libmaus2::exception::LibMausException lme;
			lme.getStream() << "[E] cannot parse maxtry parameter in " << s << std::endl;
			lme.finish();
			throw lme;
		}
	}
	else
	{
		return getDefaultMaxTry();
	}
}

static uint64_t getDefaultNumThreads()
{
	return 1;
}

static uint64_t checkNumThreads(std::string const & s)
{
	std::regex R("\\{\\{threads(\\d+)\\}\\}");

	std::smatch sm;
	if ( ::std::regex_search(s, sm, R) )
	{
		std::istringstream istr(sm[1]);
		uint64_t i;
		istr >> i;

		if ( istr && istr.peek() == std::istream::traits_type::eof() )
		{
			return i;
		}
		else
		{
			libmaus2::exception::LibMausException lme;
			lme.getStream() << "[E] cannot parse threads parameter in " << s << std::endl;
			lme.finish();
			throw lme;
		}
	}
	else
	{
		return getDefaultNumThreads();
	}
}

static uint64_t getDefaultMem()
{
	return 1*1024;
}

static uint64_t checkMem(std::string const & s)
{
	std::regex R("\\{\\{mem(\\d+)\\}\\}");

	std::smatch sm;
	if ( ::std::regex_search(s, sm, R) )
	{
		std::istringstream istr(sm[1]);
		uint64_t i;
		istr >> i;

		if ( istr && istr.peek() == std::istream::traits_type::eof() )
		{
			return i;
		}
		else
		{
			libmaus2::exception::LibMausException lme;
			lme.getStream() << "[E] cannot parse mem parameter in " << s << std::endl;
			lme.finish();
			throw lme;
		}
	}
	else
	{
		return getDefaultMem();
	}
}

static std::string getDefaultD(libmaus2::util::ArgParser const & arg)
{
	return libmaus2::util::ArgInfo::getDefaultTmpFileName(arg.progname);
}

std::vector<Rule> parseFile(std::string const fn)
{
	libmaus2::aio::InputStreamInstance ISI(fn);

	std::ostringstream ostr;
	while ( ISI )
	{
		std::string line;
		std::getline(ISI,line);
		ostr << line << '\n';
	}

	std::string const s = ostr.str();

	std::vector<std::string> V;

	uint64_t l = 0;
	ostr.str(std::string());

	while ( l < s.size() )
	{
		Token tok = getNextToken(s,l);

		if ( tok.token == Token::TOKEN_NEWLINE )
		{
			std::string const line = ostr.str();
			ostr.str(std::string());
			if ( line.size() )
				V.push_back(line);
		}
		else if ( tok.token == Token::TOKEN_ESCAPE )
		{
			ostr << '\\' << tok.sym;
		}
		else if ( tok.token == Token::TOKEN_SYMBOL )
		{
			ostr << tok.sym;
		}
	}

	if ( ostr.str().size() )
		V.push_back(ostr.str());

	Rule R;
	bool rulevalid = false;
	std::vector < Rule > VR;

	bool ignorefail = false;
	bool deepsleep = false;
	int64_t maxattempt = getDefaultMaxTry();
	int64_t numthreads = getDefaultNumThreads();
	int64_t mem = getDefaultMem();

	for ( uint64_t i = 0; i < V.size(); ++i )
	{
		std::string const s = V[i];

		if ( s.size() )
		{
			// rule command line
			if ( s[0] == '\t' )
			{
				if ( rulevalid )
				{
					R.commands.push_back(s.substr(1));
				}
				else
				{
					libmaus2::exception::LibMausException lme;
					lme.getStream() << "[E] command line " << s << " outside any rule" << std::endl;
					lme.finish();
					throw lme;
				}
			}
			// comment
			else if ( s[0] == '#' )
			{
				std::string const flagprefix = "#{{hpcscheflags}}";

				if ( s.size() >= flagprefix.size() && s.substr(0,flagprefix.size()) == flagprefix )
				{
					std::string const f = s.substr(flagprefix.size());
					// std::cerr << "flags:\t" << f << std::endl;

					ignorefail = false;
					deepsleep = false;
					maxattempt = checkMaxTry(f);
					numthreads = checkNumThreads(f);
					mem = checkMem(f);

					if ( f.find("{{ignorefail}}") != std::string::npos )
					{
						ignorefail = true;
					}
					if ( f.find("{{deepsleep}}") != std::string::npos )
					{
						deepsleep = true;
					}
				}
			}
			else
			{
				if ( rulevalid )
				{
					VR.push_back(R);
					R.clear();
				}

				if ( s.find(':') != std::string::npos )
				{
					int64_t const p = s.find(':');

					std::string const prefix = s.substr(0,p);
					std::string const suffix = s.substr(p+1);

					R.produced = splitSpace(prefix);
					R.dependencies = splitSpace(suffix);

					R.ignorefail = ignorefail;
					R.deepsleep = deepsleep;
					R.maxattempt = maxattempt;
					R.numthreads = numthreads;
					R.mem = mem;

					rulevalid = true;
				}
				else
				{
					libmaus2::exception::LibMausException lme;
					lme.getStream() << "[E] cannot parse line " << s << std::endl;
					lme.finish();
					throw lme;
				}
			}
		}
	}

	if ( rulevalid )
	{
		VR.push_back(R);
		R.clear();
	}

	return VR;
}

int hpcschedmake(libmaus2::util::ArgParser const & arg)
{
	std::string const fn = arg[0];

	std::vector<Rule> const VL = parseFile(fn);

	std::string const dn = arg.uniqueArgPresent("d") ? arg["d"] : getDefaultD(arg);
	libmaus2::util::TempFileNameGenerator tgen(dn,4,16 /* dirmod */, 16 /* filemod */);

	struct ProducedInfo
	{
		uint64_t id;
		std::vector < uint64_t > producers;

		ProducedInfo() {}
		ProducedInfo(uint64_t const rid) : id(rid) {}
	};

	std::map<std::string,ProducedInfo> targetmap;
	for ( uint64_t i = 0; i < VL.size(); ++i )
	{
		Rule const & R = VL[i];

		for ( uint64_t j = 0; j < R.produced.size(); ++j )
		{
			std::map<std::string,ProducedInfo>::iterator it = targetmap.find(R.produced[j]);

			if ( it == targetmap.end() )
			{
				uint64_t const nextid = targetmap.size();
				targetmap[R.produced[j]] = ProducedInfo(nextid);
				it = targetmap.find(R.produced[j]);
			}

			assert ( it != targetmap.end() );

			it->second.producers.push_back(i);
		}
	}

	for ( uint64_t i = 0; i < VL.size(); ++i )
	{
		Rule const & R = VL[i];

		for ( uint64_t j = 0; j < R.dependencies.size(); ++j )
		{
			std::map<std::string,ProducedInfo>::iterator it = targetmap.find(R.dependencies[j]);

			if ( it == targetmap.end() )
			{
				libmaus2::exception::LibMausException lme;
				lme.getStream() << "[E] dependency " << R.dependencies[j] << " is not produced by any rule" << std::endl;
				lme.finish();
				throw lme;
			}
		}
	}

	std::vector < libmaus2::util::CommandContainer > VCC;

	for ( uint64_t id = 0; id < VL.size(); ++id )
	{
		Rule const R = VL[id];

		std::string const in = "/dev/null";

		std::ostringstream ostr;
		ostr << tgen.getFileName() << "_" << id;

		std::string const filebase = ostr.str();
		std::string const out = filebase + ".out";
		std::string const err = filebase + ".err";
		std::string const script = filebase + ".script";
		std::string const code = filebase + ".returncode";
		std::string const command = filebase + ".com";

		{
			libmaus2::aio::OutputStreamInstance OSI(script);
			OSI << "#! /bin/bash\n";
			for ( uint64_t i = 0; i < R.commands.size(); ++i )
				OSI << R.commands[i] << '\n';
			OSI << "RT=$?\n";
			OSI << "echo ${RT} >" << code << "\n";
			OSI << "exit ${RT}\n";
			OSI.flush();
		}

		{
			libmaus2::aio::OutputStreamInstance OSI(command);
			for ( uint64_t i = 0; i < R.commands.size(); ++i )
				OSI << R.commands[i] << '\n';
			OSI.flush();
		}

		std::vector<std::string> tokens;

		tokens.push_back("/bin/bash");
		tokens.push_back(script);

		libmaus2::util::Command C(in,out,err,code,tokens);
		C.numattempts = 0;
		C.maxattempts = R.maxattempt;
		C.completed = false;
		C.ignorefail = R.ignorefail;
		C.deepsleep = R.deepsleep;
		// CN.V[i] = C;

		libmaus2::util::CommandContainer CN;
		CN.id = id;
		CN.threads = R.numthreads;
		CN.mem = R.mem;

		std::vector<uint64_t> depid;
		for ( uint64_t j = 0; j < R.dependencies.size(); ++j )
		{
			std::map<std::string,ProducedInfo>::iterator it = targetmap.find(R.dependencies[j]);
			assert ( it != targetmap.end() );

			ProducedInfo const & PI = it->second;

			for ( uint64_t k = 0; k < PI.producers.size(); ++k )
				depid.push_back(PI.producers[k]);
		}
		std::sort(depid.begin(),depid.end());
		depid.resize(std::unique(depid.begin(),depid.end()) - depid.begin());

		CN.depid = depid;
		CN.attempt = 0;
		CN.maxattempt = R.maxattempt;
		CN.V.push_back(C);

		VCC.push_back(CN);
	}

	for ( uint64_t id = 0; id < VCC.size(); ++id )
	{
		libmaus2::util::CommandContainer & CN = VCC[id];

		for ( uint64_t j = 0; j < CN.depid.size(); ++j )
		{
			uint64_t const dep = CN.depid[j];
			VCC[dep].rdepid.push_back(id);
		}

	}

	libmaus2::util::ContainerDescriptionList CDL;
	for ( uint64_t id = 0; id < VCC.size(); ++id )
	{
		// std::cerr << VCC[id] << std::endl;

		std::ostringstream ostr;
		ostr << tgen.getFileName() << ".cc";
		std::string const fn = ostr.str();

		// VFN.push_back(fn);

		libmaus2::aio::OutputStreamInstance OSI(fn);
		VCC[id].serialise(OSI);

		libmaus2::util::ContainerDescription const CD(fn, false, VCC[id].rdepid.size());

		CDL.V.push_back(CD);
	}

	{
		std::ostringstream ostr;
		ostr << tgen.getFileName() << ".cdl";
		std::string const fn = ostr.str();

		libmaus2::aio::OutputStreamInstance OSI(fn);
		CDL.serialise(OSI);

		std::cout << fn << std::endl;
	}

	return EXIT_SUCCESS;
}

int main(int argc, char * argv[])
{
	try
	{
		libmaus2::util::ArgParser const arg(argc,argv);

		if ( arg.size() < 1 )
		{
			std::cerr << "usage: " << argv[0] << " <options> Makefile" << std::endl;
			return EXIT_FAILURE;
		}
		else
		{
			int const r = hpcschedmake(arg);

			return r;
		}
	}
	catch(std::exception const & ex)
	{
		std::cerr << ex.what() << std::endl;
		return EXIT_FAILURE;
	}
}
