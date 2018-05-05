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

#include <runProgram.hpp>
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
#include <libmaus2/util/Command.hpp>
#include <libmaus2/util/TarWriter.hpp>
#include <libmaus2/parallel/PosixThread.hpp>
#include <libmaus2/util/DynamicLoading.hpp>
#include <libmaus2/util/PathTools.hpp>
#include <FDIO.hpp>
#include <sys/wait.h>

#include <sys/types.h>
#include <pwd.h>
#include <libgen.h>

static int doClose(int const fd)
{
	while ( true )
	{
		int const r = ::close(fd);

		if ( r == 0 )
			return 0;
		else
		{
			int const error = errno;

			switch ( error )
			{
				case EINTR:
				case EAGAIN:
					break;
				default:
					return r;
			}
		}
	}
}

static int doDup2(int const fd0, int const fd1)
{
	while ( true )
	{
		int const r = ::dup2(fd0,fd1);

		if ( r == -1 )
		{
			int const error = errno;

			switch ( error )
			{
				case EINTR:
				case EAGAIN:
					break;
				default:
					return r;
			}
		}
		else
		{
			return r;
		}
	}
}

std::pair<std::string,std::string> split(std::string const & s)
{
	uint64_t p = 0;
	while ( p < s.size() && !isspace(s[p]) )
		++p;

	std::string const first = s.substr(0,p);

	while ( p < s.size() && isspace(s[p]) )
		++p;

	std::string const second = s.substr(p);

	return std::pair<std::string,std::string>(first,second);
}

extern "C" {
	void dlopen_dummy()
	{

	}
}


static std::string getExecPath()
{
	Dl_info libinfo;
	int const r = dladdr(reinterpret_cast<void*>(dlopen_dummy), &libinfo);

	if ( ! r )
	{
		::libmaus2::exception::LibMausException se;
		se.getStream() << "dladdr failed: " << dlerror() << std::endl;
		se.finish();
		throw se;
	}

	libmaus2::autoarray::AutoArray<char> D(strlen(libinfo.dli_fname)+1,true);
	std::copy(libinfo.dli_fname,libinfo.dli_fname + strlen(libinfo.dli_fname),D.begin());

	char * dn = dirname(D.begin());

	return libmaus2::util::PathTools::getAbsPath(std::string(dn));
}

struct Module
{
	typedef int(*function_type)(char const *, uint64_t const);

	libmaus2::util::DynamicLibrary::shared_ptr_type library;
	libmaus2::util::DynamicLibraryFunction<function_type>::shared_ptr_type function;
};

std::map < std::string, Module > modules;

bool canLoadLibrary(std::string const & name)
{
	try
	{
		libmaus2::util::DynamicLibrary lib(name);
		return true;
	}
	catch(...)
	{
		return false;
	}
}

pid_t startCommand(libmaus2::util::Command C, std::string const & scriptname, int const outfd = -1, int const errfd = -1)
{
	if ( C.modcall )
	{
		std::pair<std::string,std::string> const P = split(C.script);

		if ( modules.find(P.first) == modules.end() )
		{
			try
			{
				Module module;
				std::string const functionname = "hpcsched_" + P.first;
				std::string modname;

				if ( canLoadLibrary(functionname + ".so") )
					modname = functionname + ".so";
				else if ( canLoadLibrary(getExecPath() + "/../lib/hpcsched/" + PACKAGE_VERSION + "/" + functionname + ".so") )
					modname = getExecPath() + "/../lib/hpcsched/" + PACKAGE_VERSION + "/" + functionname + ".so";
				else
				{
					libmaus2::exception::LibMausException lme;
					lme.getStream() << "[E] unable to find module for " << P.first << std::endl;
					lme.finish();
					throw lme;
				}

				libmaus2::util::DynamicLibrary::shared_ptr_type library(
					new libmaus2::util::DynamicLibrary(modname)
				);
				module.library = library;

				libmaus2::util::DynamicLibraryFunction<Module::function_type>::shared_ptr_type function(
					new libmaus2::util::DynamicLibraryFunction<Module::function_type>(
						*(module.library),
						functionname
					)
				);
				module.function = function;

				modules [ P.first ] = module;
			}
			catch(std::exception const & ex)
			{
				std::cerr << "[E] trying to load module for " << P.first << " failed:\n" << ex.what() << std::endl;
			}
		}
	}

	pid_t const pid = fork();

	if ( pid == 0 )
	{
		try
		{
			if ( outfd >= 0 )
			{
				C.out = "/dev/stdout";
				if ( doClose(STDOUT_FILENO) != 0 )
					_exit(EXIT_FAILURE);
				if ( doDup2(outfd,STDOUT_FILENO) == -1 )
					_exit(EXIT_FAILURE);
				if ( doClose(outfd) != 0 )
					_exit(EXIT_FAILURE);
			}
			if ( errfd >= 0 )
			{
				C.err = "/dev/stderr";
				if ( doClose(STDERR_FILENO) != 0 )
					_exit(EXIT_FAILURE);
				if ( doDup2(errfd,STDERR_FILENO) == -1 )
					_exit(EXIT_FAILURE);
				if ( doClose(errfd) != 0 )
					_exit(EXIT_FAILURE);
			}
			if ( C.modcall )
			{
				std::pair<std::string,std::string> const P = split(C.script);
				std::map < std::string, Module >::iterator it = modules.find(P.first);

				if ( it == modules.end() )
				{
					_exit(EXIT_FAILURE);
				}
				else
				{
					Module & mod = it->second;
					libmaus2::util::DynamicLibraryFunction<Module::function_type> & function = *(mod.function);
					char const * p = P.second.c_str();
					uint64_t const n = P.second.size();
					int const r = function.func(p,n);
					_exit(r);
				}
			}
			else
			{
				int const r = C.dispatch(scriptname);
				_exit(r);
			}
		}
		catch(...)
		{
			_exit(EXIT_FAILURE);
		}
	}
	else if ( pid == static_cast<pid_t>(-1) )
	{
		int const error = errno;
		libmaus2::exception::LibMausException lme;
		lme.getStream() << "[V] fork failed: " << strerror(error) << std::endl;
		lme.finish();
		throw lme;
	}
	else
	{
		return pid;
	}
}

pid_t startSleep(int const len)
{
	pid_t const pid = fork();

	if ( pid == 0 )
	{
		sleep(len);
		_exit(0);
	}
	else if ( pid == static_cast<pid_t>(-1) )
	{
		int const error = errno;
		libmaus2::exception::LibMausException lme;
		lme.getStream() << "[V] fork failed: " << strerror(error) << std::endl;
		lme.finish();
		throw lme;
	}
	else
	{
		return pid;
	}
}

std::pair<pid_t,int> waitWithTimeout(int const timeout)
{
	pid_t const sleeppid = startSleep(timeout);

	while ( true )
	{
		int status = 0;
		pid_t const wpid = waitpid(-1, &status, 0);

		if ( wpid == static_cast<pid_t>(-1) )
		{
			int const error = errno;

			switch ( error )
			{
				case EAGAIN:
				case EINTR:
					break;
				default:
				{
					int const error = errno;
					libmaus2::exception::LibMausException lme;
					lme.getStream() << "[V] waitpid failed in waitWithTimeout: " << strerror(error) << std::endl;
					lme.finish();
					throw lme;
				}
			}
		}
		else if ( wpid == sleeppid )
		{
			return std::pair<pid_t,int>(static_cast<pid_t>(0),0);
		}
		else
		{
			kill(sleeppid,SIGTERM);
			int sleepstatus = 0;

			while ( true )
			{
				pid_t const wpid = waitpid(sleeppid,&sleepstatus,0);

				if ( wpid == sleeppid )
					break;

				int const error = errno;

				switch ( error )
				{
					case EAGAIN:
					case EINTR:
						break;
					default:
					{
						int const error = errno;
						libmaus2::exception::LibMausException lme;
						lme.getStream() << "[V] waitpid(sleeppid) failed in waitWithTimeout: " << strerror(error) << std::endl;
						lme.finish();
						throw lme;
					}
				}
			}

			return std::pair<pid_t,int>(wpid,status);
		}
	}
}

struct PosixOutput
{
	typedef PosixOutput this_type;
	typedef libmaus2::util::unique_ptr<this_type>::type unique_ptr_type;

	std::string fn;
	int fd;

	PosixOutput(std::string const & rfn)
	: fn(rfn), fd(libmaus2::aio::PosixFdOutputStreamBuffer::doOpen(fn))
	{

	}

	~PosixOutput()
	{
		libmaus2::aio::PosixFdOutputStreamBuffer::doFlush(fd,fn);
		libmaus2::aio::PosixFdOutputStreamBuffer::doClose(fd,fn);
	}

	void flush()
	{
		libmaus2::aio::PosixFdOutputStreamBuffer::doFlush(fd,fn);
	}

	uint64_t getFileSize()
	{
		uint64_t const off = libmaus2::aio::PosixFdOutputStreamBuffer::doSeekAbsolute(fd,fn,0,SEEK_END);
		return off;
	}
};


struct CopyThread : public libmaus2::parallel::PosixThread
{
	typedef CopyThread this_type;
	typedef libmaus2::util::unique_ptr<this_type>::type unique_ptr_type;

	int const infd;
	std::ostream & out;
	bool const immediateFlush;

	CopyThread(int const rinfd, std::ostream & rout, bool rimmediateFlush) : infd(rinfd), out(rout), immediateFlush(rimmediateFlush) {}

	virtual void * run()
	{
		libmaus2::autoarray::AutoArray<char> C(8192,false);

		try
		{
			while ( true )
			{
				::ssize_t r = ::read(infd,C.begin(),C.size());

				if ( r > 0 )
				{
					out.write(C.begin(),r);

					if ( ! out )
					{
						libmaus2::exception::LibMausException lme;
						lme.getStream() << "[E] CopyThread::run: failed to copy " << r << " bytes" << std::endl;
						lme.finish();
						throw lme;
					}

					if ( immediateFlush )
					{
						out.flush();

						if ( ! out )
						{
							libmaus2::exception::LibMausException lme;
							lme.getStream() << "[E] CopyThread::run: failed to copy " << r << " bytes" << std::endl;
							lme.finish();
							throw lme;
						}
					}
				}
				else if ( r == 0 )
				{
					break;
				}
				else
				{
					int const error = errno;

					switch ( error )
					{
						case EAGAIN:
						case EINTR:
							break;
						default:
						{
							libmaus2::exception::LibMausException lme;
							lme.getStream() << "[E] CopyThread::run: failed to read: " << strerror(error) << std::endl;
							lme.finish();
							throw lme;
						}
					}
				}
			}
		}
		catch(std::exception const & ex)
		{
			libmaus2::parallel::ScopePosixSpinLock slock(libmaus2::aio::StreamLock::cerrlock);
			std::cerr << ex.what() << std::endl;
			return 0;
		}

		return 0;
	}
};

struct Pipe
{
	typedef Pipe this_type;
	typedef libmaus2::util::unique_ptr<this_type>::type unique_ptr_type;

	int fd[2];

	Pipe()
	{
		while ( true )
		{
			int const r = pipe(&fd[0]);

			if ( r != 0 )
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
						lme.getStream() << "[E] pipe failed: " << strerror(error) << std::endl;
						lme.finish();
						throw lme;
					}
				}
			}
			else
			{
				break;
			}
		}
	}

	~Pipe()
	{
		closeReadEnd();
		closeWriteEnd();
	}

	int getReadEnd()
	{
		return fd[0];
	}

	int getWriteEnd()
	{
		return fd[1];
	}

	void closeReadEnd()
	{
		if ( fd[0] >= 0 )
		{
			doClose(fd[0]);
			fd[0] = -1;
		}
	}

	void closeWriteEnd()
	{
		if ( fd[1] >= 0 )
		{
			doClose(fd[1]);
			fd[1] = -1;
		}
	}
};

int slurmworker(libmaus2::util::ArgParser const & arg)
{
	RunInfo RI;

	std::string const tmpfilebase = arg.uniqueArgPresent("T") ? arg["T"] : libmaus2::util::ArgInfo::getDefaultTmpFileName(arg.progname);

	std::string const hostname = arg[0];
	uint64_t const port = arg.getParsedRestArg<uint64_t>(1);

	char const * jobid_s = getenv("SLURM_JOB_ID");

	if ( ! jobid_s )
	{
		std::cerr << "[E] job id not found in SLURM_JOB_ID" << std::endl;
		return EXIT_FAILURE;
	}

	std::istringstream jobidistr(jobid_s);
	uint64_t jobid;
	jobidistr >> jobid;
	if ( ! jobidistr || jobidistr.peek() != std::istream::traits_type::eof() )
	{
		std::cerr << "[E] job id " << jobid_s << " found in SLURM_JOB_ID not parseable" << std::endl;
		return EXIT_FAILURE;
	}

	libmaus2::network::ClientSocket sockA(port,hostname.c_str());

	FDIO fdio(sockA.getFD());
	fdio.writeNumber(jobid);
	/* uint64_t const workerid = */ fdio.readNumber();
	std::string expcurdir = fdio.readString();
	std::string curdir = libmaus2::util::ArgInfo::getCurDir();

	bool const curdirok = (curdir == expcurdir);

	fdio.writeNumber(curdirok);

	if ( !curdirok )
	{
		std::cerr << "[E] current directory " << curdir << " does not match expected current directory " << expcurdir << std::endl;
		return EXIT_FAILURE;
	}

	std::string const remotetmpbase = fdio.readString();

	std::string const outbase = remotetmpbase + "_out";
	std::string const outdata = outbase + ".data";
	std::string const errbase = remotetmpbase + "_err";
	std::string const errdata = errbase + ".data";
	std::string const metafn = remotetmpbase + ".meta";
	std::string const scriptbase = remotetmpbase + ".script";

	std::cerr << "[V] using outdata=" << outdata << std::endl;
	std::cerr << "[V] using errdata=" << errdata << std::endl;

	fdio.writeString(outdata);
	fdio.writeString(errdata);
	fdio.writeString(metafn);

	libmaus2::aio::OutputStreamInstance metaOSI(metafn);
	libmaus2::aio::OutputStreamInstance outData(outdata);
	libmaus2::aio::OutputStreamInstance errData(errdata);

	CopyThread::unique_ptr_type outCopy;
	CopyThread::unique_ptr_type errCopy;
	Pipe::unique_ptr_type outPipe;
	Pipe::unique_ptr_type errPipe;
	// (int const rinfd, std::ostream & rout) : infd(rinfd), out(rout) {}

	bool running = true;

	enum state_type
	{
		state_idle = 0,
		state_running = 1
	};

	state_type state = state_idle;
	pid_t workpid = static_cast<pid_t>(-1);

	try
	{
		while ( running )
		{
			switch ( state )
			{
				case state_idle:
				{
					std::cerr << "[V] telling control we are idle" << std::endl;
					// tell control we are idle
					fdio.writeNumber(0);
					std::cerr << "[V] waiting for acknowledgement" << std::endl;
					// get reply
					uint64_t const rep = fdio.readNumber();
					std::cerr << "[V] got acknowledgement with code " << rep << std::endl;

					// execute command
					if ( rep == 0 )
					{
						std::string const jobdesc = fdio.readString();
						std::istringstream jobdescistr(jobdesc);
						libmaus2::util::Command const com(jobdescistr);
						uint64_t const containerid = fdio.readNumber();
						uint64_t const subid = fdio.readNumber();

						std::ostringstream scriptnamestr;
						scriptnamestr << scriptbase + "_" << containerid << "_" << subid << ".sh";

						std::cerr << "[V] starting command " << com << " (" << containerid << "," << subid << ")" << std::endl;

						RI.containerid = containerid;
						RI.subid = subid;
						RI.outstart = outData.tellp();
						RI.errstart = errData.tellp();
						RI.outend = std::numeric_limits<uint64_t>::max();
						RI.errend = std::numeric_limits<uint64_t>::max();
						RI.outfn = outdata;
						RI.errfn = errdata;
						RI.scriptname = scriptnamestr.str();

						fdio.writeString(RI.serialise());

						Pipe::unique_ptr_type toutPipe(new Pipe());
						outPipe = UNIQUE_PTR_MOVE(toutPipe);
						Pipe::unique_ptr_type terrPipe(new Pipe());
						errPipe = UNIQUE_PTR_MOVE(terrPipe);


						workpid = startCommand(com,scriptnamestr.str(),outPipe->getWriteEnd(),errPipe->getWriteEnd());
						outPipe->closeWriteEnd();
						errPipe->closeWriteEnd();

						CopyThread::unique_ptr_type toutCopy(new CopyThread(outPipe->getReadEnd(),outData,false /* im flush */));
						outCopy = UNIQUE_PTR_MOVE(toutCopy);
						outCopy->start();
						CopyThread::unique_ptr_type terrCopy(new CopyThread(errPipe->getReadEnd(),errData,true /* im flush */));
						errCopy = UNIQUE_PTR_MOVE(terrCopy);
						errCopy->start();

						state = state_running;
					}
					// terminate
					else if ( rep == 2 )
					{
						std::cerr << "[V] terminating" << std::endl;
						running = false;
					}
					else
					{

					}
					break;
				}
				case state_running:
				{
					std::pair<pid_t,int> const P = waitWithTimeout(60 /* timeout */);

					pid_t const wpid = P.first;
					int const status = P.second;

					if ( wpid == workpid )
					{
						workpid = static_cast<pid_t>(-1);

						RI.outend = outData.tellp();
						RI.errend = errData.tellp();
						RI.status = status;
						RI.serialise(metaOSI);
						metaOSI.flush();

						if ( status == 0 )
							libmaus2::aio::FileRemoval::removeFile(RI.scriptname);

						// tell control we finished a job
						fdio.writeNumber(1);
						fdio.writeNumber(status);
						fdio.writeString(RI.serialise());
						// wait for acknowledgement
						fdio.readNumber();

						outCopy->join();
						outCopy.reset();
						errCopy->join();
						errCopy.reset();
						outPipe.reset();
						errPipe.reset();

						outData.flush();
						errData.flush();

						std::cerr << "[V] finished with status " << status << std::endl;

						state = state_idle;
					}
					else
					{
						// tell control we are still running our job
						fdio.writeNumber(2);
						// wait for acknowledgement
						fdio.readNumber();
					}
					break;
				}
			}
		}
	}
	catch(std::exception const & ex)
	{
		std::cerr << ex.what() << std::endl;

		/*
		 * kill worker process if it is still running
		 *
		 * first try SIGTERM to allow for "gracious" failure with possible cleanup activity
		 *
		 * if process does not end after SIGTERM then send SIGKILL
		 */
		if ( workpid != static_cast<pid_t>(-1) )
		{
			kill(workpid,SIGTERM);

			for ( uint64_t i = 0; workpid != static_cast<pid_t>(-1) && i < 10; ++i )
			{
				std::pair<pid_t,int> const P = waitWithTimeout(60 /* timeout */);

				pid_t const wpid = P.first;
				//int const status = P.second;

				if ( wpid == workpid )
				{
					workpid = static_cast<pid_t>(-1);

					RI.outend = outData.tellp();
					RI.errend = errData.tellp();
					RI.status = std::numeric_limits<int>::min();
					RI.serialise(metaOSI);
					metaOSI.flush();

					outCopy->join();
					outCopy.reset();
					errCopy->join();
					errCopy.reset();
					outPipe.reset();
					errPipe.reset();

					outData.flush();
					errData.flush();
				}
			}
		}
		if ( workpid != static_cast<pid_t>(-1) )
		{
			kill(workpid,SIGKILL);

			for ( uint64_t i = 0; workpid != static_cast<pid_t>(-1) && i < 10; ++i )
			{
				std::pair<pid_t,int> const P = waitWithTimeout(60 /* timeout */);

				pid_t const wpid = P.first;
				//int const status = P.second;

				if ( wpid == workpid )
				{
					workpid = static_cast<pid_t>(-1);

					RI.outend = outData.tellp();
					RI.errend = errData.tellp();
					RI.serialise(metaOSI);
					RI.status = std::numeric_limits<int>::min();
					metaOSI.flush();

					outCopy->join();
					outCopy.reset();
					errCopy->join();
					errCopy.reset();
					outPipe.reset();
					errPipe.reset();

					outData.flush();
					errData.flush();
				}
			}
		}
	}

	metaOSI.flush();
	outData.flush();
	errData.flush();

	return EXIT_SUCCESS;
}

int main(int argc, char * argv[])
{
	try
	{
		libmaus2::util::ArgParser const arg(argc,argv);

		int const r = slurmworker(arg);

		return r;
	}
	catch(std::exception const & ex)
	{
		std::cerr << ex.what() << std::endl;
		return EXIT_FAILURE;
	}
}
