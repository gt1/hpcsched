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
