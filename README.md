# hpcsched
hpcsched is a HPC job scheduler supporting job dependencies. Jobs and
dependencies can be stated in a file similar in syntax to a make file.

An abstract example control file is

```
result_a:
	prog_a
result_b: result_a
	prog_b
result_c: result_a
	prog_c
result_d: result_b result_c
	prog_d
```

The file consists of a set of rules. Each rule has the syntax

```
result_1 result_2 ... : dependency_1 dependency_2 ...
<tab>prog1 prog1_arg1 prog1_arg2
<tab>prog2 prog2_arg1 prog2_arg2
```

This denotes that the rule can only be started if dependency_1, dependency_2
etc have been finished. The rule produces result_1, result_2 etc. The
computations for producing the results are performed by executing the lines
below the dependencies and results line which start with a tab symbol up to
the start of the next rule. Comments can be inserted as lines starting with
the # symbol.

The dependency graph for this file is depicted below:

![Image of dependency graph](https://raw.githubusercontent.com/gt1/hpcsched/master/doc/depgraph.svg?sanitize=true)

- result_a can be computed without prerequesites. It is produced by running prog_a
- result_b and result_c can only be computed after result_a has been produced. result_b is produced by running prog_b when result_a is computed. result_c is produced by running prog_c when result_a is computed.
- result_d depends on both result_b and result_c. When both have been produced, then result_d is computed by calling prog_d

A concrete toy example is

```
text_a:
	echo "Hello A" > text_a.txt
text_b:
	echo "Hello B" > text_b.txt
text_c: text_a text_b
	cat text_a.txt text_b.txt > text_c.txt
```

This first produces a file text_a.txt containing `Hello A` and file text_b.txt
containing `Hello B`. When both have been produced then they are
concatenated to a file text_c.txt.

For processing the text based control file first needs to be transformed
into an intermediate format by running

```
hpschedmake Makefile
```

This may fail if the syntax in Makefile is not recognized. If the processing
is succesful than the name of a binary control file is printed on the
standard output channel. An example run is

```
$ hpcschedmake Makefile
hpcschedmake_node_26769_1517412398/00/00/00/00/file04.cdl
```

The prefix used for temporary files by hpcschedmake can be set using the -T
switch, e.g.

```
hpcschedmake -Ttmpdir Makefile
```

The intermediate form stores various pieces of information about the
progress reached so far. Processing on an HPC system can be started using

```
hpcschedcontrol hpcschedmake_node_26769_1517412398/00/00/00/00/file04.cdl
```

where `hpcschedmake_node_26769_1517412398/00/00/00/00/file04.cdl` is the
file name reported by hpcschedmake. Note that hpcschedcontrol only supports
the SLURM batch system so far. When hpcschedcontrol is run, then
hpcschedworker needs to be available in the users path via setting the PATH
variable accordingly. hpcschedcontrol has several options for controlling
its behaviour:

* -T: prefix used for temporary files (example: -Ttmpdir)
* --workertime: run-time limit used for starting jobs via the batch system (example: --workertime720, by default this is --workertime1440)
* --workermem: memory limit used when starting jobs (example: --workermem1000, by default this is --workermem40000). This value overides memory values provided via the config file (see below)
* --workers: number of worker processes started. hpcschedcontrol manages a pool of worker jobs of this size.
* -p: partition name in batch system used for starting jobs (-phaswell by default)

Note that white space is not supported between the argument name and its
value (i.e. `--workertime100` is valid, `--workertime 100` is not).

hpcschedcontrol prints progress information on the standard error channel
while it runs. After is has finished the set of log files produced by the
jobs can be stored inside a tar file using e.g.

```
hpcschedprocesslogs hpcschedmake_node_26769_1517412398/00/00/00/00/file04.cdl
```

This will produce a tar file named hpcschedmake_node_26769_1517412398/00/00/00/00/file04.cdl.log.tar.
This tar file contains a file containing the output and error channel for
each job run as well as a file containing the return status.

hpcschedcontrol checks the return status of each job run to detect whether a
rule was executed successfully. Success is assumed if that return status is
0, any other return code will be considered as a failed run. A failed run
will be retried a given number of times (see below) before hpcschedcontrol
considers the whole pipeline as failed.

Additional options for running commands in rules can be given using comment
lines starting with `#{{hpcschedflags}}` in the input file passed to
hpcschedmake. An example is

```
#{{hpcschedflags}} {{maxtry5}} {{mem2000}}
```

The values set in such a line are used starting from that line up to the
point the next line starting by `#{{hpcschedflags}}` is encountered.
Possible arguments are

* maxtry<int>: maximum number of times a job is retried before it is marked as failed permanently
* mem<int>: memory parameter passed on to the batch system
* threads<int>: number of threads requested from the batch system for running jobs
* ignorefail: consider job as finished successfully even if it has failed the maximal number of tries
* deepsleep: terminate unused worker processes if only jobs marked as deepsleep are running or ready to run. The terminated worker processes will be restarted once new jobs become available. This setting is useful to avoid processes which are idle for a long time.

Example for daligner
--------------------

A Makefile for daligner [https://github.com/thegenemyers/DALIGNER] can be
created using the program hpcscheddaligner similarly to calling HPC.daligner
in the daligner package.

A sample call is

	hpcscheddaligner -T16 -M32 reads.db

Other valid daligner options will be passed through to daligner calls. A
Makefile produced for a two block reads.db database is

```
#{{hpcschedflags}} {{deepsleep}} {{threads16}} {{mem32768}}
reads.1.reads.1.las:
	daligner -T16 -M32 reads.1 reads.1
reads.2.reads.1.las reads.1.reads.2.las reads.2.reads.2.las:
	daligner -T16 -M32 reads.2 reads.1 reads.2
#{{hpcschedflags}} {{deepsleep}}
reads.1.reads.1.las.check:reads.1.reads.1.las
	LAcheck -v ./reads.db ./reads.db reads.1.reads.1.las
reads.1.reads.2.las.check:reads.1.reads.2.las
	LAcheck -v ./reads.db ./reads.db reads.1.reads.2.las
reads.1.las: reads.1.reads.1.las.check reads.1.reads.2.las.check
	LAmerge reads.1.las reads.1.reads.1.las reads.1.reads.2.las
LAmerge_reads.1.las_cleanup: reads.1.las
	rm reads.1.reads.1.las
	rm reads.1.reads.2.las
reads.1.las.check:reads.1.las
	LAcheck -v ./reads.db ./reads.db reads.1.las
reads.2.reads.1.las.check:reads.2.reads.1.las
	LAcheck -v ./reads.db ./reads.db reads.2.reads.1.las
reads.2.reads.2.las.check:reads.2.reads.2.las
	LAcheck -v ./reads.db ./reads.db reads.2.reads.2.las
reads.2.las: reads.2.reads.1.las.check reads.2.reads.2.las.check
	LAmerge reads.2.las reads.2.reads.1.las reads.2.reads.2.las
LAmerge_reads.2.las_cleanup: reads.2.las
	rm reads.2.reads.1.las
	rm reads.2.reads.2.las
reads.2.las.check:reads.2.las
	LAcheck -v ./reads.db ./reads.db reads.2.las
```

The file produced can be used via hpcschedmake as described above. In the
example case this will produce the alignment file reads.1.las and
reads.2.las. These two files and also the temporary files produced in
between are checked using daligner's LAcheck program.

Source
------

The hpcsched source code is hosted on github:

	git@github.com:gt1/hpcsched.git

Release packages can be found at

	https://github.com/gt1/hpcsched/releases

Please make sure to choose a package containing the word "release" in it's name if you
intend to compile hpcsched for production (i.e. non development) use.

Compilation of hpcsched
-------------------------

hpcsched needs libmaus2 [https://github.com/gt1/libmaus2] . When libmaus2
is installed in ${LIBMAUSPREFIX} then hpcsched can be compiled and
installed in ${HOME}/hpcsched using

	- autoreconf -i -f
	- ./configure --with-libmaus2=${LIBMAUSPREFIX} \
		--prefix=${HOME}/hpcsched
	- make install

The release packages come with a configure script included (making the autoreconf call unnecessary for source obtained via one of those).
