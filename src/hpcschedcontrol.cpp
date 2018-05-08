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
#include <libmaus2/aio/InputOutputStreamInstance.hpp>
#include <libmaus2/parallel/TerminatableSynchronousQueue.hpp>
#include <libmaus2/digest/md5.hpp>
#include <RunInfo.hpp>
#include <sys/wait.h>

#if defined(HAVE_EPOLL_CREATE) || defined(HAVE_EPOLL_CREATE1)
#include <sys/epoll.h>
#endif

#include <sys/types.h>
#include <pwd.h>

#if 0
#include <slurm/slurm.h>

struct SlurmControlConfig
{
	slurm_ctl_conf_t * conf;

	SlurmControlConfig()
	: conf(0)
	{
		int const r = slurm_load_ctl_conf(static_cast<time_t>(0),&conf);

		if ( r != 0 )
		{
			int const error = slurm_get_errno();
			libmaus2::exception::LibMausException lme;
			lme.getStream() << "[E] slurm_load_ctl_conf failed: " << slurm_strerror(error) << std::endl;
			lme.finish();
			throw lme;
		}
	}

	~SlurmControlConfig()
	{
		slurm_free_ctl_conf(conf);
	}

	uint64_t getMaxArraySize() const
	{
		return conf->max_array_sz;
	}
};

struct SlurmPartitions
{
	partition_info_msg_t * partitions;

	SlurmPartitions()
	{
		int const r = slurm_load_partitions(static_cast<time_t>(0),&partitions,0/*flags*/);

		if ( r != 0 )
		{
			int const error = slurm_get_errno();
			libmaus2::exception::LibMausException lme;
			lme.getStream() << "[E] slurm_load_partitions failed: " << slurm_strerror(error) << std::endl;
			lme.finish();
			throw lme;
		}
	}

	~SlurmPartitions()
	{
		slurm_free_partition_info_msg(partitions);
	}

	uint64_t size() const
	{
		return partitions->record_count;
	}

	std::string getName(uint64_t const i) const
	{
		assert ( i < size() );
		return partitions->partition_array[i].name;
	}

	uint64_t getIdForName(std::string const & name) const
	{
		for ( uint64_t i = 0; i < size(); ++i )
			if ( getName(i) == name )
				return i;

		libmaus2::exception::LibMausException lme;
		lme.getStream() << "[E] partition named " << name << " not found" << std::endl;
		lme.finish();
		throw lme;
	}

	std::string getNodes(uint64_t const i) const
	{
		assert ( i < size() );
		return partitions->partition_array[i].nodes;
	}

	uint64_t getTotalCpus(uint64_t const i) const
	{
		assert ( i < size() );
		return partitions->partition_array[i].total_cpus;
	}

	uint64_t getTotalNodes(uint64_t const i) const
	{
		assert ( i < size() );
		return partitions->partition_array[i].total_nodes;
	}

	uint64_t getMaxTime(uint64_t const i) const
	{
		assert ( i < size() );
		return partitions->partition_array[i].max_time;
	}

	uint64_t getMaxMemPerCpu(uint64_t const i) const
	{
		assert ( i < size() );
		return partitions->partition_array[i].max_mem_per_cpu;
	}

	uint64_t getMaxCpusPerNode(uint64_t const i) const
	{
		assert ( i < size() );
		return partitions->partition_array[i].max_cpus_per_node;
	}
};

struct SlurmJobs
{
	job_info_msg_t * jobs;

	SlurmJobs()
	: jobs(0)
	{
		int const r = slurm_load_jobs(0,&jobs,0 /* showflags */);

		if ( r != 0 )
		{
			int const error = slurm_get_errno();
			libmaus2::exception::LibMausException lme;
			lme.getStream() << "[E] slurm_load_jobs failed: " << slurm_strerror(error) << std::endl;
			lme.finish();
			throw lme;
		}
	}

	~SlurmJobs()
	{
		slurm_free_job_info_msg(jobs);
	}

	uint64_t size() const
	{
		return jobs->record_count;
	}

	uint64_t getUserId(uint64_t const i) const
	{
		return jobs->job_array[i].user_id;
	}

	std::string getUserName(uint64_t const i) const
	{
		uint64_t const uid = getUserId(i);
		struct passwd * pw = getpwuid(uid);

		if ( pw )
		{
			return std::string(pw->pw_name);
		}
		else
		{
			libmaus2::exception::LibMausException lme;
			lme.getStream() << "[E] no user name found for uid " << uid << std::endl;
			lme.finish();
			throw lme;
		}
	}

	char const * getName(uint64_t const i) const
	{
		return jobs->job_array[i].name;
	}

	uint64_t getJobId(uint64_t const i) const
	{
		return jobs->job_array[i].job_id;
	}
};
#endif

#include <FDIO.hpp>

std::string getUsage(libmaus2::util::ArgParser const & arg)
{
	std::ostringstream ostr;

	ostr << "usage: " << arg.progname << " [<parameters>] <container.cdl>" << std::endl;
	ostr << "\n";
	ostr << "parameters:\n";
	ostr << " -t          : number of threads (defaults to number of cores on machine)\n";
	ostr << " -T          : prefix for temporary files (default: create files in current working directory)\n";
	ostr << " --workertime: time for workers in (default: 1440)\n";
	ostr << " --workermem : memory for workers (default: 40000)\n";
	ostr << " -p          : cluster partition (default: haswell)\n";
	ostr << " --workers   : number of workers (default: 16)\n";

	return ostr.str();
}


struct SlurmControl
{
	struct JobDescription
	{
		int64_t containerid;
		int64_t subid;

		JobDescription() : containerid(-1), subid(-1)
		{
		}
		JobDescription(uint64_t const rcontainerid, uint64_t const rsubid)
		: containerid(rcontainerid), subid(rsubid)
		{

		}

		bool operator<(JobDescription const & O) const
		{
			if ( containerid != O.containerid )
				return containerid < O.containerid;
			else if ( subid != O.subid )
				return subid < O.subid;
			else
				return false;
		}

		void reset()
		{
			containerid = -1;
			subid = -1;
		}
	};

	struct WorkerInfo
	{
		int64_t id;
		libmaus2::network::SocketBase::unique_ptr_type Asocket;
		bool active;
		JobDescription packageid;
		uint64_t workerid;
		std::string wtmpbase;

		std::string outdatafn;
		std::string errdatafn;
		std::string metafn;

		WorkerInfo() { reset(); }

		void reset()
		{
			id = -1;
			Asocket.reset();
			active = false;
			workerid = std::numeric_limits<uint64_t>::max();
			outdatafn = std::string();
			errdatafn = std::string();
			metafn = std::string();
			resetPackageId();
		}

		void resetPackageId()
		{
			packageid.reset();
		}
	};

	struct StartWorkerRequest
	{
		uint64_t * nextworkerid;
		std::string tmpfilebase;
		std::string hostname;
		uint64_t serverport;
		uint64_t workertime;
		uint64_t workermem;
		uint64_t workerthreads;
		std::string partition;
		libmaus2::util::ArgParser const * arg;
		WorkerInfo * AW;
		uint64_t i;
		std::map<uint64_t,uint64_t> * idToSlot;
		uint64_t workers;
		libmaus2::util::TempFileNameGenerator * tmpgen;

		StartWorkerRequest() {}
		StartWorkerRequest(
			uint64_t & rnextworkerid,
			std::string rtmpfilebase,
			std::string rhostname,
			uint64_t rserverport,
			uint64_t rworkertime,
			uint64_t rworkermem,
			uint64_t rworkerthreads,
			std::string rpartition,
			libmaus2::util::ArgParser const & rarg,
			WorkerInfo * rAW,
			uint64_t ri,
			std::map<uint64_t,uint64_t> & ridToSlot,
			uint64_t rworkers,
			libmaus2::util::TempFileNameGenerator * rtmpgen
		) :
			nextworkerid(&rnextworkerid),
			tmpfilebase(rtmpfilebase),
			hostname(rhostname),
			serverport(rserverport),
			workertime(rworkertime),
			workermem(rworkermem),
			workerthreads(rworkerthreads),
			partition(rpartition),
			arg(&rarg),
			AW(rAW),
			i(ri),
			idToSlot(&ridToSlot),
			workers(rworkers),
			tmpgen(rtmpgen)
		{

		}

		void dispatch()
		{
			uint64_t const workerid = (*nextworkerid)++;
			std::ostringstream workernamestr;
			workernamestr << "worker_" << workerid;
			std::string const workername = workernamestr.str();

			std::ostringstream wtmpbasestr;
			wtmpbasestr << tmpgen->getFileName() << "_" << workerid;
			std::string const wtmpbase = wtmpbasestr.str();

			std::ostringstream outfnstr;
			outfnstr << wtmpbase << ".out";
			std::string const outfn = outfnstr.str();

			std::string const descname = wtmpbase + "_worker.sbatch";

			std::ostringstream commandstr;
			commandstr << "hpcschedworker " << hostname << " " << serverport;
			std::string command = commandstr.str();

			writeJobDescription(
				descname,
				workername,
				outfn,
				workertime,
				workermem,
				workerthreads,
				partition,
				command
			);

			std::vector<std::string> Varg;
			Varg.push_back("sbatch");
			Varg.push_back(descname);

			std::string const jobid_s = runProgram(Varg,*arg);

			std::deque<std::string> Vtoken = libmaus2::util::stringFunctions::tokenize(jobid_s,std::string(" "));

			if ( Vtoken.size() >= 4 )
			{
				std::istringstream istr(Vtoken[3]);
				uint64_t id;
				istr >> id;

				if ( istr && istr.peek() == '\n' )
				{
					// std::cerr << "got job id " << id << std::endl;

					AW [ i ].id = id;
					AW [ i ].workerid = workerid;
					AW [ i ].wtmpbase = wtmpbase;
					(*idToSlot)[id] = i;
				}
				else
				{
					libmaus2::exception::LibMausException lme;
					lme.getStream() << "[E] unable to find job id in " << jobid_s << std::endl;
					lme.finish();
					throw lme;
				}
			}
			else
			{
				libmaus2::exception::LibMausException lme;
				lme.getStream() << "[E] unable to find job id in " << jobid_s << std::endl;
				lme.finish();
				throw lme;
			}

			libmaus2::aio::FileRemoval::removeFile(descname);

			std::cerr << "[V] started job " << (i+1) << " out of " << workers << " with id " << AW[i].id << std::endl;
		}
	};


	struct EPoll
	{
		int fd;
		std::set<int> activeset;

		EPoll(int const
		#if defined(HAVE_EPOLL_CREATE) && !defined(HAVE_EPOLL_CREATE1)
			size
		#endif
		) : fd(-1), activeset()
		{
			#if defined(HAVE_EPOLL_CREATE1)
			fd = epoll_create1(0);

			std::cerr << "epoll_create1 returned " << fd << std::endl;

			if ( fd < 0 )
			{
				int const error = errno;

				libmaus2::exception::LibMausException lme;
				lme.getStream() << "[E] epoll_create1() failed: " << strerror(error) << std::endl;
				lme.finish();
				throw lme;
			}
			#elif defined(HAVE_EPOLL_CREATE)
			fd = epoll_create(size);

			std::cerr << "epoll_create(" << size << ") returned " << fd << std::endl;

			if ( fd < 0 )
			{
				int const error = errno;

				libmaus2::exception::LibMausException lme;
				lme.getStream() << "[E] epoll_create1() failed: " << strerror(error) << std::endl;
				lme.finish();
				throw lme;
			}

			#else
			libmaus2::exception::LibMausException lme;
			lme.getStream() << "[E] EPoll: epoll interface not supported " << std::endl;
			lme.finish();
			throw lme;
			#endif
		}

		~EPoll()
		{
			if ( fd >= 0 )
			{
				::close(fd);
				fd = -1;
			}
		}

		#if defined(HAVE_EPOLL_CREATE) || defined(HAVE_EPOLL_CREATE1)
		void add(int const addfd)
		{
			struct epoll_event ev;
			ev.events = EPOLLIN
				#if defined(EPOLLRDHUP)
				| EPOLLRDHUP
				#endif
				;
			ev.data.fd = addfd;

			while ( true )
			{
				int const r = epoll_ctl(
					fd,
					EPOLL_CTL_ADD,
					addfd,
					&ev
				);

				if ( r == 0 )
				{
					libmaus2::parallel::ScopePosixSpinLock slock(libmaus2::aio::StreamLock::cerrlock);
					std::cerr << "[I] adding file descriptor " << addfd << " to epoll set" << std::endl;

					activeset.insert(addfd);
					break;
				}

				int const error = errno;

				switch ( error )
				{
					case EINTR:
					case EAGAIN:
						break;
					default:
					{

						libmaus2::exception::LibMausException lme;
						lme.getStream() << "[E] EPoll:add: epoll_ctl() failed: " << strerror(error) << std::endl;
						lme.finish();
						throw lme;
					}
				}
			}
		}

		void remove(int const remfd)
		{
			while ( true )
			{
				int const r = epoll_ctl(
					fd,
					EPOLL_CTL_DEL,
					remfd,
					NULL
				);

				if ( r == 0 )
				{
					libmaus2::parallel::ScopePosixSpinLock slock(libmaus2::aio::StreamLock::cerrlock);
					std::cerr << "[I] removing file descriptor " << remfd << " from epoll set" << std::endl;

					activeset.erase(remfd);
					break;
				}
				else
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
							lme.getStream() << "[E] EPoll:remove: epoll_ctl() failed: " << strerror(error) << std::endl;
							lme.finish();
							throw lme;
						}
					}
				}
			}
		}

		bool wait(int & rfd, int const timeout = 1000 /* milli seconds */)
		{
			rfd = -1;

			struct epoll_event events[1];

			while ( true )
			{
				int const nfds = epoll_wait(fd, &events[0], sizeof(events)/sizeof(events[0]), timeout);

				if ( nfds < 0 )
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
							lme.getStream() << "[E] EPoll:wait: epoll_wait() failed: " << strerror(error) << std::endl;
							lme.finish();
							throw lme;
						}
					}
				}

				if ( nfds == 0 )
				{
					return false;
				}
				else
				{
					rfd = events[0].data.fd;

					if ( activeset.find(rfd) != activeset.end() )
					{
						return true;
					}
					else
					{
						libmaus2::parallel::ScopePosixSpinLock slock(libmaus2::aio::StreamLock::cerrlock);
						std::cerr << "[W] warning: epoll returned inactive file descriptor " << rfd << std::endl;
						return false;
					}
				}
			}
		}
		#else
		void add(int const)
		{
		}
		void remove(int const)
		{
		}
		bool wait(int &, int const = 1000 /* milli seconds */)
		{
			return false;
		}
		#endif
	};

	struct ProgState
	{
		uint64_t numunfinished;
		uint64_t numpending;

		ProgState() : numunfinished(0), numpending(0) {}
		ProgState(
			uint64_t const rnumunfinished,
			uint64_t const rnumpending
		) : numunfinished(rnumunfinished), numpending(rnumpending)
		{

		}

		bool operator==(ProgState const & o) const
		{
			return numunfinished == o.numunfinished && numpending == o.numpending;
		}

		bool operator!=(ProgState const & o) const
		{
			return !operator==(o);
		}
	};

	struct WriteContainerRequest
	{
		libmaus2::util::ContainerDescription object;
		uint64_t offset;

		WriteContainerRequest() {}
		WriteContainerRequest(
			libmaus2::util::ContainerDescription const & robject,
			uint64_t const roffset
		) : object(robject), offset(roffset) {}
		WriteContainerRequest(std::istream & in)
		{
			deserialise(in);
		}

		void dispatch(std::iostream & cdlstream) const
		{
			cdlstream.clear();
			cdlstream.seekp(offset);
			object.serialise(cdlstream);
			cdlstream.flush();
		}

		std::ostream & serialise(std::ostream & out) const
		{
			object.serialise(out);
			libmaus2::util::NumberSerialisation::serialiseNumber(out,offset);
			return out;
		}

		std::istream & deserialise(std::istream & in)
		{
			object.deserialise(in);
			offset = libmaus2::util::NumberSerialisation::deserialiseNumber(in);
			return in;
		}
	};

	std::string const curdir;
	unsigned short serverport;
	uint64_t const backlog;
	uint64_t const tries;
	uint64_t nextworkerid;
	std::string const hostname;
	std::string const tmpfilebase;
	libmaus2::util::TempFileNameGenerator tmpgen;
	uint64_t const workertime;
	uint64_t const workermem;
	std::string const partition;
	uint64_t const workers;
	std::string const cdl;

	libmaus2::autoarray::AutoArray<WorkerInfo> AW;
	std::map<uint64_t,uint64_t> idToSlot;
	std::map<int,uint64_t> fdToSlot;
	std::map < JobDescription, uint64_t > Mfail;

	libmaus2::util::ContainerDescriptionList CDL;
	std::vector < libmaus2::util::ContainerDescription > & CDLV;
	std::vector < libmaus2::util::CommandContainer > VCC;

	std::set < JobDescription > Sunfinished;
	std::set < JobDescription > Srunning;
	std::set<uint64_t> Sresubmit;
	uint64_t ndeepsleep;
	std::map < uint64_t, uint64_t > Munfinished;

	uint64_t const maxthreads;
	uint64_t const workerthreads;

	EPoll EP;

	libmaus2::network::ServerSocket::unique_ptr_type Pservsock;

	std::set<uint64_t> restartSet;
	std::set<uint64_t> wakeupSet;
	// uint64_t pending;

	ProgState pstate;
	bool failed;

	std::vector < StartWorkerRequest > Vreq;

	libmaus2::aio::InputOutputStreamInstance metastream;
	libmaus2::aio::InputOutputStreamInstance::shared_ptr_type cdlstream;

	libmaus2::parallel::TerminatableSynchronousQueue<WriteContainerRequest> WCRQ;

	struct WCRQWriterThread : public libmaus2::parallel::PosixThread
	{
		libmaus2::parallel::TerminatableSynchronousQueue<WriteContainerRequest> & WCRQ;
		libmaus2::aio::InputOutputStreamInstance::shared_ptr_type cdlstream;
		std::string const cdl;
		std::string const journalfn;
		std::vector < WriteContainerRequest > V;
		bool runok;

		static uint64_t getUpdateThreshold()
		{
			return 64;
		}

		WCRQWriterThread(
			libmaus2::parallel::TerminatableSynchronousQueue<WriteContainerRequest> & rWCRQ,
			libmaus2::aio::InputOutputStreamInstance::shared_ptr_type & rcdlstream,
			std::string const rcdl
		) : WCRQ(rWCRQ), cdlstream(rcdlstream), cdl(rcdl), journalfn(cdl + ".journal"), runok(true)
		{

		}

		static std::string serialiseVector(std::vector < WriteContainerRequest > const & V)
		{
			std::ostringstream ostr;
			libmaus2::util::NumberSerialisation::serialiseNumber(ostr,V.size());
			for ( uint64_t i = 0; i < V.size(); ++i )
				V[i].serialise(ostr);

			return ostr.str();
		}

		static bool loadJournalData(std::string const journalfn, std::string & data)
		{
			try
			{
				uint64_t n = libmaus2::util::GetFileSize::getFileSize(journalfn);
				libmaus2::autoarray::AutoArray<char> A(n,false);
				libmaus2::aio::InputStreamInstance ISI(journalfn);
				ISI.read(A.begin(),n);
				data = std::string(A.begin(),A.end());
				return true;
			}
			catch(std::exception const & ex)
			{
				std::cerr << "[E] failed to load journal data" << std::endl;
				return false;
			}
		}

		static bool parseJournalData(std::vector < WriteContainerRequest > & V, std::string & md5in, std::string const & journaldata)
		{
			try
			{
				std::istringstream ISI(journaldata);
				uint64_t const n = libmaus2::util::NumberSerialisation::deserialiseNumber(ISI);
				V.resize(n);
				for ( uint64_t i = 0; i < n; ++i )
					V[i].deserialise(ISI);
				md5in = libmaus2::util::StringSerialisation::deserialiseString(ISI);
				return true;
			}
			catch(std::exception const & ex)
			{
				std::cerr << "[E] failed to parse journal data" << std::endl;
				return false;
			}
		}

		static bool computeJournalMD5(std::string & md5data, std::vector < WriteContainerRequest > const & V)
		{
			try
			{
				libmaus2::util::MD5::md5(serialiseVector(V),md5data);
				return true;
			}
			catch(std::exception const & ex)
			{
				std::cerr << "[E] failed to compute checksum for journal data:\n" << ex.what() << std::endl;
				return false;
			}
		}

		static bool checkJournalMD5(std::string const & md5data, std::string const & md5in)
		{
			try
			{
				if ( md5data != md5in )
				{
					libmaus2::exception::LibMausException lme;
					lme.getStream() << "[E] check mismatch " << md5data << " != " << md5in << std::endl;
					lme.finish();
					throw lme;
				}

				return true;
			}
			catch(std::exception const & ex)
			{
				std::cerr << "[E] checksum mismatch journal data:\n" << ex.what() << std::endl;
				return false;
			}
		}

		static bool dispatchJournal(std::vector < WriteContainerRequest > const & V, std::string const & cdl)
		{
			try
			{
				libmaus2::aio::InputOutputStreamInstance::shared_ptr_type cdlstream(
					new libmaus2::aio::InputOutputStreamInstance(cdl,std::ios::in | std::ios::out | std::ios::binary)
				);

				// apply updates
				for ( uint64_t i = 0; i < V.size(); ++i )
				{
					V[i].dispatch(*cdlstream);
					if ( ! cdlstream )
					{
						libmaus2::exception::LibMausException lme;
						lme.getStream() << "[E] failed to write update" << std::endl;
						lme.finish();
						throw lme;
					}
				}
				cdlstream->flush();
				if ( ! cdlstream )
				{
					libmaus2::exception::LibMausException lme;
					lme.getStream() << "[E] failed to write update (flush)" << std::endl;
					lme.finish();
					throw lme;
				}

				cdlstream.reset();

				return true;
			}
			catch(std::exception const & ex)
			{
				std::cerr << "[E] failed to apply journal data:\n" << ex.what() << std::endl;
				return false;
			}
		}

		static bool removeJournal(std::string const & journalfn)
		{
			try
			{
				libmaus2::aio::FileRemoval::removeFile(journalfn);
				return true;
			}
			catch(std::exception const & ex)
			{
				std::cerr << "[E] failed to delete journal:\n:" << ex.what() << std::endl;
				return false;
			}
		}

		static bool applyJournal(std::string const & cdl, std::string const & journalfn)
		{
			try
			{
				bool ok = true;

				std::string journaldata;
				std::string md5in;
				std::string md5data;
				std::vector < WriteContainerRequest > V;

				ok = ok && loadJournalData(journalfn,journaldata);
				ok = ok && parseJournalData(V, md5in, journaldata);
				ok = ok && computeJournalMD5(md5data,V);
				ok = ok && checkJournalMD5(md5in,md5data);
				ok = ok && dispatchJournal(V,cdl);
				ok = ok && removeJournal(journalfn);

				return ok;
			}
			catch(std::exception const & ex)
			{
				std::cerr << "[E] applyJournal failed: " << ex.what() << std::endl;
				return false;
			}
		}

		static bool tryApplyJournal(std::string const & cdl, std::string const & journalfn)
		{
			try
			{
				if ( ! libmaus2::util::GetFileSize::fileExists(journalfn) )
					return true;

				return applyJournal(cdl,journalfn);
			}
			catch(std::exception const & ex)
			{
				std::cerr << "[E] tryApplyJournal failed: " << ex.what() << std::endl;
				return false;
			}
		}

		#if 0
		enum journal_update_error
		{
			journal_update_error_none,
			journal_update_error_memory,
			journal_update_error_load,
			journal_update_error_parse,
			journal_update_error_checksum,
			journal_update_error_update,
			journal_update_error_unknown
		};

		static bool loadJournalData(std::string const journalfn, std::string & data)
		{
			try
			{
				uint64_t n = libmaus2::util::GetFileSize::getFileSize(journalfn);
				libmaus2::autoarray::AutoArray<char> A(n,false);
				libmaus2::aio::InputStreamInstance ISI(journalfn);
				ISI.read(A.begin(),n);
				data = std::string(A.begin(),A.end());
				return true;
			}
			catch(std::exception const & ex)
			{
				std::cerr << "[E] failed to load journal data" << std::endl;
				return false;
			}
		}


		static journal_update_error loadJournal(std::vector < WriteContainerRequest > & V, std::string const & journalfn)
		{
			try
			{
				std::string journaldata;
				bool const dataok = loadJournalData(journalfn,journaldata);

				if ( ! dataok )
					return journal_update_error_load;

				std::cerr << "[V] loading journal" << std::endl;

				std::string md5in;

				try
				{
					std::istringstream ISI(journaldata);
					uint64_t const n = libmaus2::util::NumberSerialisation::deserialiseNumber(ISI);
					V.resize(n);
					for ( uint64_t i = 0; i < n; ++i )
						V[i].deserialise(ISI);
					md5in = libmaus2::util::StringSerialisation::deserialiseString(ISI);
				}
				catch(std::bad_alloc const & ex)
				{
					std::cerr << "[E] failed to parse journal data (bad_alloc)" << std::endl;
					return journal_update_error_memory;
				}
				catch(std::exception const & ex)
				{
					std::cerr << "[E] failed to parse journal data" << std::endl;
					return journal_update_error_parse;
				}

				// compute MD5 for data
				std::string md5data;

				try
				{
					libmaus2::util::MD5::md5(serialiseVector(V),md5data);
				}
				catch(std::bad_alloc const & ex)
				{
					std::cerr << "[E] failed to parse journal data (bad_alloc on checksum)" << std::endl;
					return journal_update_error_memory;
				}
				catch(std::exception const & ex)
				{
					std::cerr << "[E] failed to parse journal data (error on checksum):\n" << ex.what() << std::endl;
					return journal_update_error_checksum;
				}

				// check integrity
				if ( md5data == md5in )
				{
					std::cerr << "[V] loaded journal" << std::endl;
					return journal_update_error_none;
				}
				else
				{
					std::cerr << "[E] journal checksum mismatch" << std::endl;
					return journal_update_error_checksum;
				}
			}
			catch(std::bad_alloc const & ex)
			{
				return journal_update_error_memory;
			}
			catch(std::exception const & ex)
			{
				std::cerr << "[E] failed to load journal: " << ex.what() << std::endl;
				return journal_update_error_unknown;
			}
		}

		// apply journal
		static journal_update_error applyJournal(std::iostream & cdlstream, std::string const & journalfn)
		{
			bool ok = false;

			try
			{
				std::cerr << "[V] applying journal" << std::endl;

				// read data
				std::vector < WriteContainerRequest > V;
				journal_update_error const journalok = loadJournal(V,journalfn);

				switch ( journalok )
				{
					case journal_update_error_none:
					{
						try
						{
							// apply journal
							for ( uint64_t i = 0; i < V.size(); ++i )
								V[i].dispatch(cdlstream);
							cdlstream.sync();

							libmaus2::aio::FileRemoval::removeFile(journalfn);

							std::cerr << "[V] applyed journal with " << V.size() << " elements" << std::endl;

							return journal_update_error_none;
						}
						catch(std::exception const & ex)
						{
							std::cerr << "[E] error applying journal updates:\n" << ex.what() << std::endl;
							return journal_update_error_update;
						}
					}
					case journal_update_error_memory:
					case journal_update_error_load:
					{
						return journalok;
					}
					case journal_update_error_parse:
					{
						// remove defective journal file
						libmaus2::aio::FileRemoval::removeFile(journalfn);
						return journalok;
					}

				}
				if ( journalok == journal_update_error_none )
				{
				}
				else
				{
					switch ( journalok )
					{
						// this should not happen
						case journal_update_error_none:
							assert(false);
							break;
						// unable to load journal, may be input error
						case journal_update_error_load:
							return false;
							break;
						libmaus2::aio::FileRemoval::removeFile(journalfn);
					}
				}
			}
			catch(std::exception const & ex)
			{
				std::cerr << "[E] applyJournal failed: " << ex.what() << std::endl;
			}

			return ok;
		}

		// apply journal
		static bool applyJournal(std::string const & cdl, std::string const & journalfn)
		{
			libmaus2::aio::InputOutputStreamInstance::shared_ptr_type cdlstream(
				new libmaus2::aio::InputOutputStreamInstance(cdl,std::ios::in | std::ios::out | std::ios::binary)
			);

			return applyJournal(*cdlstream,journalfn);
		}
		#endif

		// write journal to disk
		bool writeJournal()
		{
			try
			{
				std::cerr << "[V] writing journal" << std::endl;

				// get data
				std::string const data = serialiseVector(V);

				// compute checksum
				std::string md5data;
				libmaus2::util::MD5::md5(data,md5data);

				// write data
				libmaus2::aio::OutputStreamInstance::unique_ptr_type OSI(new libmaus2::aio::OutputStreamInstance(journalfn));
				OSI->write(data.c_str(),data.size());
				libmaus2::util::StringSerialisation::serialiseString(*OSI,md5data);
				OSI->flush();

				if ( ! *OSI )
				{
					OSI.reset();

					libmaus2::exception::LibMausException lme;
					lme.getStream() << "[E] unable to write journal " << journalfn << std::endl;
					lme.finish();
					throw lme;
				}

				OSI.reset();

				std::cerr << "[V] wrote journal" << std::endl;

				return true;
			}
			catch(std::exception const & ex)
			{
				std::cerr << "[E] writeJournal failed:\n" << ex.what() << std::endl;
				libmaus2::aio::FileRemoval::removeFile(journalfn);
				return false;
			}
		}

		bool handleVectorFlush()
		{
			if ( runok )
			{
				std::cerr << "[V] flushing CDL vector to " << journalfn << std::endl;

				bool ok = writeJournal();

				// if we managed to write the journal
				if ( ok )
				{
					try
					{
						// apply updates
						for ( uint64_t i = 0; i < V.size(); ++i )
						{
							V[i].dispatch(*cdlstream);
							if ( ! cdlstream )
							{
								libmaus2::exception::LibMausException lme;
								lme.getStream() << "[E] failed to write update" << std::endl;
								lme.finish();
								throw lme;
							}
						}
						cdlstream->flush();
						if ( ! cdlstream )
						{
							libmaus2::exception::LibMausException lme;
							lme.getStream() << "[E] failed to write update (flush)" << std::endl;
							lme.finish();
							throw lme;
						}

						libmaus2::aio::FileRemoval::removeFile(journalfn);
					}
					catch(std::exception const & ex)
					{
						std::cerr << "[E] failed to apply updates: " << ex.what() << std::endl;
						ok = false;
					}

				}

				runok = runok && ok;
			}

			V.resize(0);

			return runok;
		}

		void * run()
		{
			try
			{
				while ( true )
				{
					WriteContainerRequest WCR = WCRQ.deque();
					V.push_back(WCR);

					if ( V.size() >= getUpdateThreshold() )
						handleVectorFlush();
					// WCR.dispatch(*cdlstream);
				}
			}
			catch(std::exception const & ex)
			{

			}

			handleVectorFlush();

			return 0;
		}
	};

	WCRQWriterThread WCRQT;

	static void writeJobDescription(
		std::string const & fn,
		std::string const & jobname,
		std::string const & outfn,
		uint64_t const utime,
		uint64_t const umem,
		uint64_t const threads,
		std::string const partition,
		std::string const command
	)
	{
		libmaus2::aio::OutputStreamInstance OSI(fn);

		OSI << "#!/bin/bash\n";
		OSI << "#SBATCH --job-name=" << jobname << "\n";
		OSI << "#SBATCH --output=" << outfn << "\n";
		OSI << "#SBATCH --ntasks=1" << "\n";
		OSI << "#SBATCH --time=" << utime << "\n";
		OSI << "#SBATCH --mem=" << umem << "\n";
		OSI << "#SBATCH --cpus-per-task=" << threads << "\n";
		OSI << "#SBATCH --cpus-per-task=" << threads << "\n";
		OSI << "#SBATCH --partition=" << partition << "\n";
		OSI << "srun bash -c \"" << command << "\"\n";
	}

	void processWakeupSet()
	{
		for ( std::set < uint64_t >::const_iterator it = wakeupSet.begin();
			it != wakeupSet.end(); ++it )
		{
			uint64_t const i = *it;
			std::cerr << "[V] sending wakeup to slot " << i << std::endl;

			FDIO fdio(AW[i].Asocket->getFD());
			fdio.writeNumber(1);
		}
		wakeupSet.clear();
	}

	void processResubmitSet()
	{
		for ( std::set < uint64_t >::const_iterator it = Sresubmit.begin();
			it != Sresubmit.end(); ++it )
		{
			uint64_t const i = *it;
			std::cerr << "[V] resubmitting slot " << i << " after deep sleep" << std::endl;
			Vreq[i].dispatch();

		}
		Sresubmit.clear();
	}

	uint64_t computeMaxThreads()
	{
		uint64_t maxthreads = 1;
		for ( uint64_t i = 0; i < VCC.size(); ++i )
			maxthreads = std::max(maxthreads,VCC[i].threads);
		return maxthreads;
	}

	static libmaus2::util::ContainerDescriptionList loadCDL(std::string const & cdl)
	{
		libmaus2::util::ContainerDescriptionList CDL;
		libmaus2::aio::InputStreamInstance ISI(cdl);
		CDL.deserialise(ISI);
		return CDL;
	}

	static std::vector < libmaus2::util::CommandContainer > loadVCC(std::vector < libmaus2::util::ContainerDescription > & CDLV)
	{
		std::vector < libmaus2::util::CommandContainer > VCC(CDLV.size());
		for ( uint64_t i = 0; i < CDLV.size(); ++i )
		{
			std::istringstream ISI(CDLV[i].fn);
			VCC[i].deserialise(ISI);
			CDLV[i].missingdep = 0;
		}

		return VCC;
	}

	void countUnfinished()
	{
		// count number of unfinished jobs per command container
		for ( uint64_t i = 0; i < CDLV.size(); ++i )
		{
			libmaus2::util::CommandContainer & CC = VCC[i];
			uint64_t numunfinished = 0;

			for ( uint64_t j = 0; j < CC.V.size(); ++j )
				if ( CC.V[j].completed )
				{

				}
				else
				{
					Munfinished[i]++;
					numunfinished += 1;
				}

			std::cerr << "[V] container " << i << " has " << numunfinished << " unfinished jobs" << std::endl;

			if ( numunfinished )
			{
				// check reverse dependencies
				for ( uint64_t j = 0; j < CC.rdepid.size(); ++j )
				{
					uint64_t const k = CC.rdepid[j];

					std::cerr << "[V] container " << k << " has missing dependency " << i << std::endl;

					CDLV[k].missingdep += 1;
				}
			}
		}
	}

	void addUnfinished(JobDescription const J)
	{
		Sunfinished.insert(J);
	}

	JobDescription getUnfinished()
	{
		std::set< JobDescription >::const_iterator const it = Sunfinished.begin();
		JobDescription const P = *it;
		Sunfinished.erase(it);
		return P;
	}

	void enqueUnfinished()
	{
		for ( uint64_t i = 0; i < CDLV.size(); ++i )
		{
			if ( CDLV[i].missingdep == 0 )
			{
				std::cerr << "[V] container " << i << " has no missing dependencies, enqueuing jobs" << std::endl;

				for ( uint64_t j = 0; j < VCC[i].V.size(); ++j )
				{
					if ( !VCC[i].V[j].completed )
					{
						addUnfinished(JobDescription(i,j));
					}
				}
			}
		}
	}

	std::vector < StartWorkerRequest > computeStartRequests(libmaus2::util::ArgParser const & arg)
	{
		std::vector < StartWorkerRequest > Vreq(workers);
		for ( uint64_t i = 0; i < workers; ++i )
			Vreq[i] = StartWorkerRequest(
				nextworkerid,tmpfilebase,hostname,serverport,
				workertime,workermem,workerthreads,partition,arg,AW.begin(),i,
				idToSlot,workers,&tmpgen
			);
		return Vreq;
	}

	void checkRequeue(uint64_t const slotid)
	{
		Mfail [ AW[slotid].packageid ] += 1;

		libmaus2::util::CommandContainer & CC = VCC[AW[slotid].packageid.containerid];
		libmaus2::util::Command & CO = CC.V[AW[slotid].packageid.subid];

		// mark pipeline as failed
		if ( Mfail [ AW[slotid].packageid ] >= CC.maxattempt )
		{
			std::cerr << "[V] too many failures on " << AW[slotid].packageid.containerid << "," << AW[slotid].packageid.subid << ", marking pipeline as failed" << std::endl;

			if ( !CO.ignorefail )
				failed = true;
		}
		// requeue
		else
		{
			std::cerr << "[V] requeuing " << AW[slotid].packageid.containerid << "," << AW[slotid].packageid.subid << std::endl;

			addUnfinished(AW[slotid].packageid);
			processWakeupSet();
			processResubmitSet();
		}
	}


	void resetSlot(uint64_t const slotid)
	{
		uint64_t const id = AW[slotid].id;
		EP.remove(AW[slotid].Asocket->getFD());
		fdToSlot.erase(AW[slotid].Asocket->getFD());
		AW[slotid].reset();
		idToSlot.erase(id);
		wakeupSet.erase(id);
		restartSet.insert(slotid);
	}


	void writeContainer(uint64_t const i)
	{
		// serialise object to string
		std::ostringstream ostr;
		VCC.at(i).serialise(ostr);
		// store it in the CDL vector
		CDL.V.at(i).fn = ostr.str();

		// get updated object
		libmaus2::util::ContainerDescription const CD = CDL.V.at(i);

		// make sure it has the correct size
		assert ( CDL.checkSize(i,CD) );

		// get offset in file
		uint64_t const offset = CDL.getOffset(i).first;

		// enque write request
		WriteContainerRequest W(CD,offset);
		WCRQ.enque(W);
	}

	void handleSuccessfulCommand(
		uint64_t const slotid,
		bool const verbose = false
	)
	{
		if ( verbose )
			std::cerr << "[V] getting package id for slot " << slotid << std::endl;
		JobDescription const packageid = AW[slotid].packageid;
		if ( verbose )
			std::cerr << "[V] found package id " << packageid.containerid << "," << packageid.subid << std::endl;

		if ( verbose )
			std::cerr << "[V] getting command container" << std::endl;
		libmaus2::util::CommandContainer & CC = VCC[packageid.containerid];
		if ( verbose )
			std::cerr << "[V] got command container" << std::endl;

		libmaus2::util::Command & CO = CC.V[packageid.subid];

		if ( verbose )
			std::cerr << "[V] updating numattempts,completed" << std::endl;
		CO.numattempts += 1;
		CO.completed = true;
		if ( CO.deepsleep )
		{
			assert ( ndeepsleep > 0 );
			ndeepsleep -= 1;
		}
		Srunning.erase(packageid);
		if ( verbose )
			std::cerr << "[V] updated numattempts,completed to " << CO.numattempts << "," << CO.completed << std::endl;

		writeContainer(packageid.containerid);

		uint64_t const numunfin = --Munfinished [ packageid.containerid ];

		if ( !numunfin )
		{
			std::cerr << "[V] finished command container " << AW[slotid].packageid.containerid << std::endl;

			for ( uint64_t j = 0; j < CC.rdepid.size(); ++j )
			{
				uint64_t const k = CC.rdepid[j];

				assert ( CDLV[k].missingdep );

				CDLV[k].missingdep -= 1;

				if ( ! CDLV[k].missingdep )
				{
					std::cerr << "[V] activating container " << k << std::endl;
					for ( uint64_t j = 0; j < VCC[k].V.size(); ++j )
					{
						addUnfinished(JobDescription(k,j));
					}
					if ( Sunfinished.size() )
					{
						processWakeupSet();
						processResubmitSet();
					}
				}
			}
		}

		AW[slotid].resetPackageId();
	}

	void handleFailedCommand(uint64_t const slotid)
	{
		std::cerr << "[V] getting package id for slot " << slotid << std::endl;
		JobDescription const packageid = AW[slotid].packageid;
		std::cerr << "[V] found package id " << packageid.containerid << "," << packageid.subid << std::endl;

		std::cerr << "[V] getting reference to command container" << std::endl;
		libmaus2::util::CommandContainer & CC = VCC.at(packageid.containerid);
		std::cerr << "[V] got reference to command container" << std::endl;

		std::cerr << "[V] gettting reference to command" << std::endl;
		libmaus2::util::Command & CO = CC.V[packageid.subid];
		std::cerr << "[V] got reference to command" << std::endl;

		std::cerr << "[V] incrementing numattempts" << std::endl;
		CO.numattempts += 1;
		if ( CO.deepsleep )
		{
			assert ( ndeepsleep > 0 );
			ndeepsleep -= 1;
		}
		Srunning.erase(packageid);
		std::cerr << "[V] incremented numattempts to " << CO.numattempts << std::endl;

		if ( CO.numattempts >= CC.maxattempt && CO.ignorefail )
		{
			std::cerr << "[V] number of attempts reached max " << CC.maxattempt << " but container has ignorefail flag set" << std::endl;

			std::cerr << "[V] decreasing numattempts" << std::endl;
			CO.numattempts -= 1;
			if ( CO.deepsleep )
				ndeepsleep += 1;
			Srunning.insert(packageid);
			std::cerr << "[V] decreased numattempts to " << CO.numattempts << std::endl;

			std::cerr << "[V] calling handleSuccesfulCommand" << std::endl;
			handleSuccessfulCommand(slotid,true);
			std::cerr << "[V] returned from handleSuccesfulCommand" << std::endl;
		}
		else
		{
			writeContainer(packageid.containerid);
			checkRequeue(slotid);
			AW[slotid].resetPackageId();
		}
	}

	SlurmControl(
		std::string const & rtmpfilebase,
		uint64_t const rworkertime,
		uint64_t const rworkermem,
		std::string const rpartition,
		uint64_t const rworkers,
		std::string const & rcdl,
		int64_t const rworkerthreads,
		libmaus2::util::ArgParser const & rarg
	)
	: curdir(libmaus2::util::ArgInfo::getCurDir()),
	  serverport(50000), backlog(1024), tries(1000), nextworkerid(0), hostname(libmaus2::network::GetHostName::getHostName()),
	  tmpfilebase(rtmpfilebase),
	  tmpgen(tmpfilebase+"_tmpgen",3),
	  workertime(rworkertime),
	  workermem(rworkermem),
	  partition(rpartition),
	  workers(rworkers),
	  cdl(rcdl),
	  AW(workers),
	  idToSlot(),
	  fdToSlot(),
	  Mfail(),
	  CDL(loadCDL(cdl)),
	  CDLV(CDL.V),
	  VCC(loadVCC(CDLV)),
	  Sunfinished(),
	  Srunning(),
	  ndeepsleep(0),
	  Munfinished(),
	  maxthreads(computeMaxThreads()),
	  workerthreads(rworkerthreads > 0 ? rworkerthreads : maxthreads),
	  EP(workers+1),
	  Pservsock(
		libmaus2::network::ServerSocket::allocateServerSocket(
			serverport,
			backlog,
			hostname,
			tries
		)
	  ),
	  restartSet(),
	  wakeupSet(),
	  // pending(0),
	  pstate(),
	  failed(false),
	  Vreq(computeStartRequests(rarg)),
	  metastream(cdl + ".meta",std::ios::in | std::ios::out | std::ios::binary),
	  cdlstream(new libmaus2::aio::InputOutputStreamInstance(cdl,std::ios::in | std::ios::out | std::ios::binary)),
	  WCRQ(),
	  WCRQT(WCRQ,cdlstream,cdl)
	{

		metastream.seekp(0,std::ios::end);

		std::cerr << "[V] hostname=" << hostname << " serverport=" << serverport << " number of containers " << CDLV.size() << std::endl;

		std::cerr << "[V] got server fd " << Pservsock->getFD() << std::endl;
		EP.add(Pservsock->getFD());
		fdToSlot[Pservsock->getFD()] = std::numeric_limits<uint64_t>::max();

		countUnfinished();
		enqueUnfinished();

		WCRQT.start();
	}

	~SlurmControl()
	{
		WCRQ.terminate();
		WCRQT.join();
	}

	int process()
	{
		for ( uint64_t i = 0; i < workers; ++i )
			Vreq[i].dispatch();

		libmaus2::util::TempFileNameGenerator tmpgen(tmpfilebase+"_tmpgen",3);

		while ( Sunfinished.size() || Srunning.size() )
		{
			std::set<uint64_t> nrestartSet;
			for ( std::set<uint64_t>::const_iterator it = restartSet.begin(); it != restartSet.end(); ++it )
			{
				uint64_t const i = *it;

				try
				{
					Vreq[i].dispatch();
				}
				catch(std::exception const & ex)
				{
					std::cerr << "[E] job start failed:\n" << ex.what() << std::endl;
					nrestartSet.insert(i);
					AW[i].reset();
				}
			}
			restartSet.clear();
			restartSet = nrestartSet;

			ProgState npstate(
				Sunfinished.size(),
				Srunning.size()
			);

			if ( npstate != pstate )
			{
				pstate = npstate;
				std::cerr << "[V] Sunfinished.size()=" << pstate.numunfinished << " pending=" << pstate.numpending << std::endl;
			}

			int rfd = -1;
			if ( EP.wait(rfd) )
			{
				std::map<int,uint64_t>::const_iterator itslot = fdToSlot.find(rfd);

				if ( itslot == fdToSlot.end() )
				{
					libmaus2::exception::LibMausException lme;
					lme.getStream() << "[E] EPoll::wait returned unknown file descriptor" << std::endl;
					lme.finish();
					throw lme;
				}
				else if ( itslot->second == std::numeric_limits<uint64_t>::max() )
				{
					assert ( rfd == Pservsock->getFD() );
					int64_t slot = -1;

					try
					{
						libmaus2::network::SocketBase::unique_ptr_type nptr = Pservsock->accept();

						FDIO fdio(nptr->getFD());
						uint64_t const jobid = fdio.readNumber();

						std::cerr << "[V] accepted connection for jobid=" << jobid << " fd " << nptr->getFD() << std::endl;

						if ( idToSlot.find(jobid) != idToSlot.end() )
						{
							slot = idToSlot.find(jobid)->second;
							fdio.writeNumber(AW[slot].workerid);
							fdio.writeString(curdir);
							bool const curdirok = fdio.readNumber();

							if ( curdirok )
							{
								fdio.writeString(AW[slot].wtmpbase);
								std::string const outdatafn = fdio.readString();
								std::string const errdatafn = fdio.readString();
								std::string const metafn = fdio.readString();

								AW[slot].outdatafn = outdatafn;
								AW[slot].errdatafn = errdatafn;
								AW[slot].metafn = metafn;

								if ( ! AW[slot].Asocket )
								{
									AW[slot].Asocket = UNIQUE_PTR_MOVE(nptr);
									EP.add(AW[slot].Asocket->getFD());
									fdToSlot[AW[slot].Asocket->getFD()] = slot;
									AW[slot].active = true;

									std::cerr << "[V] marked slot " << slot << " active for jobid " << AW[slot].id << std::endl;
								}
								else
								{
									libmaus2::exception::LibMausException lme;
									lme.getStream() << "[E] erratic worker trying to open second connection" << std::endl;
									lme.finish();
									throw lme;
								}
							}
						}
						else
						{
							std::cerr << "[V] job id unknown" << std::endl;
						}
					}
					catch(std::exception const & ex)
					{
						std::cerr << "[E] error while accepting new connection:\n" << ex.what() << std::endl;
						if ( slot >= 0 )
							AW[slot].reset();
					}
				}
				else
				{
					uint64_t const i = itslot->second;

					std::cerr << "[V] epoll returned slot " << i << " ready for reading" << std::endl;

					if ( ! AW[i].active )
					{
						libmaus2::exception::LibMausException lme;
						lme.getStream() << "[E] epoll returned file descriptor for inactive slot" << std::endl;
						lme.finish();
						throw lme;
					}

					try
					{
						// read worker state
						FDIO fdio(AW[i].Asocket->getFD());
						uint64_t const rd = fdio.readNumber();

						// worker is idle
						if ( rd == 0 )
						{
							if ( Sunfinished.size() )
							{
								// get next package
								JobDescription const currentid = getUnfinished();
								// get command
								libmaus2::util::Command const com = VCC[currentid.containerid].V[currentid.subid];
								// serialise command to string
								std::ostringstream ostr;
								com.serialise(ostr);
								// process command
								AW[i].packageid = currentid;
								fdio.writeNumber(0);
								fdio.writeString(ostr.str());
								fdio.writeNumber(currentid.containerid);
								fdio.writeNumber(currentid.subid);
								std::string const sruninfo = fdio.readString();

								Srunning.insert(currentid);
								if ( com.deepsleep )
									ndeepsleep += 1;

								std::cerr << "[V] started " << com << " for " << currentid.containerid << "," << currentid.subid << " on slot " << i << " wtmpbase " << AW[i].wtmpbase << std::endl;
							}
							else
							{
								if ( ndeepsleep == Srunning.size() )
								{
									// put slot to deep sleep
									std::cerr << "[V] putting slot " << i << " to deep sleep" << std::endl;

									// request termination
									fdio.writeNumber(2);
									EP.remove(AW[i].Asocket->getFD());
									fdToSlot.erase(AW[i].Asocket->getFD());
									AW[i].reset();
									Sresubmit.insert(i);
								}
								else
								{
									std::cerr << "[V] putting slot " << i << " in wakeupSet" << std::endl;

									wakeupSet.insert(i);
								}
							}
						}
						// worker has finished a job (may or may not be succesful)
						else if ( rd == 1 )
						{
							uint64_t const status = fdio.readNumber();
							int const istatus = static_cast<int>(status);
							std::string const sruninfo = fdio.readString();
							RunInfo const RI(sruninfo);
							RI.serialise(metastream);
							metastream.flush();
							// acknowledge
							fdio.writeNumber(0);

							std::cerr << "[V] slot " << i << " reports job ended with istatus=" << istatus << std::endl;

							if ( WIFEXITED(istatus) && (WEXITSTATUS(istatus) == 0) )
							{
								handleSuccessfulCommand(i);
							}
							else
							{
								std::cerr << "[V] slot " << i << " failed, checking requeue " << AW[i].packageid.containerid << "," << AW[i].packageid.subid << std::endl;
								handleFailedCommand(i);
							}
						}
						// worker is still running a job
						else if ( rd == 2 )
						{
							// acknowledge
							fdio.writeNumber(0);
						}
						else
						{
							std::cerr << "[V] process for slot " << i << " jobid " << AW[i].id << " is erratic" << std::endl;

							if ( AW[i].packageid.containerid >= 0 )
							{
								handleFailedCommand(i);
							}

							resetSlot(i /* slotid */);
						}
					}
					catch(std::exception const & ex)
					{
						std::cerr << "[V] exception for slot " << i << " jobid " << AW[i].id << std::endl;
						std::cerr << ex.what() << std::endl;

						if ( AW[i].packageid.containerid >= 0 )
						{
							try
							{
								handleFailedCommand(i);
							}
							catch(std::exception const & ex)
							{
								std::cerr << "[E] exception in handleFailedCommand: " << std::endl;
								std::cerr << ex.what() << std::endl;
								throw;
							}
						}

						try
						{
							resetSlot(i /* slotid */);
						}
						catch(std::exception const & ex)
						{
							std::cerr << "[E] exception in resetSlot: " << std::endl;
							std::cerr << ex.what() << std::endl;
							throw;
						}
					}
				}
			}
		}

		processWakeupSet();
		processResubmitSet();

		std::set<uint64_t> Sactive;
		for ( uint64_t i = 0; i < AW.size(); ++i )
			if ( AW[i].active )
				Sactive.insert(i);

		while ( Sactive.size() )
		{
			std::vector < uint64_t > Vterm;

			for ( std::set<uint64_t>::const_iterator it = Sactive.begin(); it != Sactive.end(); ++it )
			{
				uint64_t const i = *it;

				try
				{
					// read worker state
					FDIO fdio(AW[i].Asocket->getFD());
					uint64_t const rd = fdio.readNumber();

					// worker is idle
					if ( rd == 0 )
					{
						// request terminate
						fdio.writeNumber(2);
						EP.remove(AW[i].Asocket->getFD());
						fdToSlot.erase(AW[i].Asocket->getFD());
						AW[i].reset();
						Vterm.push_back(i);
					}
					else if ( rd == 1 )
					{
						std::cerr << "[V] slot " << i << " reports finished job with no jobs active" << std::endl;
					}
					// worker is still running a job
					else if ( rd == 2 )
					{
						std::cerr << "[V] slot " << i << " reports job running, but we know of no such job" << std::endl;
						fdio.writeNumber(0);
					}
					else
					{
						std::cerr << "[V] process for slot " << i << " jobid " << AW[i].id << " is erratic" << std::endl;

						resetSlot(i /* slotid */);
					}
				}
				catch(...)
				{
					std::cerr << "[V] exception for slot " << i << " jobid " << AW[i].id << std::endl;

					resetSlot(i /* slotid */);
				}
			}

			for ( uint64_t i = 0; i < Vterm.size(); ++i )
				Sactive.erase(Vterm[i]);
		}

		if ( failed )
		{
			std::cerr << "[E] pipeline failed" << std::endl;
			return EXIT_FAILURE;
		}
		else
		{
			std::cerr << "[V] pipeline finished ok" << std::endl;
			return EXIT_SUCCESS;
		}
	}
};

int hpcschedcontrol(libmaus2::util::ArgParser const & arg)
{
	std::string const hpcschedworker = which("hpcschedworker");
	std::cerr << "[V] found hpcschedworker at " << hpcschedworker << std::endl;

	std::string const tmpfilebase = arg.uniqueArgPresent("T") ? arg["T"] : libmaus2::util::ArgInfo::getDefaultTmpFileName(arg.progname);
	uint64_t const workertime = arg.uniqueArgPresent("workertime") ? arg.getParsedArg<uint64_t>("workertime") : 1440;
	uint64_t const workermem = arg.uniqueArgPresent("workermem") ? arg.getParsedArg<uint64_t>("workermem") : 40000;
	std::string const partition = arg.uniqueArgPresent("p") ? arg["p"] : "haswell";
	uint64_t const workers = arg.uniqueArgPresent("workers") ? arg.getParsedArg<uint64_t>("workers") : 16;

	std::string const cdl = arg[0];
	std::string const cdlmeta = cdl + ".meta";
	std::string const cdljournal = cdl + ".journal";

	bool const journalok = SlurmControl::WCRQWriterThread::tryApplyJournal(cdl,cdljournal);

	if ( ! journalok )
	{
		std::cerr << "[E] failed to replay journal, please check error messages" << std::endl;
		return EXIT_FAILURE;
	}

	if ( ! libmaus2::util::GetFileSize::fileExists(cdlmeta) )
	{
		libmaus2::aio::OutputStreamInstance OSI(cdlmeta);
	}

	SlurmControl SC(
		tmpfilebase,workertime,workermem,partition,workers,cdl,
		arg.uniqueArgPresent("workerthreads") ? arg.getParsedArg<uint64_t>("workerthreads") : -1,
		arg
	);

	int const r = SC.process();

	return r;
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

		int const r = hpcschedcontrol(arg);

		return r;
	}
	catch(std::exception const & ex)
	{
		std::cerr << "[E] exception in main: " << ex.what() << std::endl;
		return EXIT_FAILURE;
	}
}
