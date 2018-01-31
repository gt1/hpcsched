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
