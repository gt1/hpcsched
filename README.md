# hpcsched
hpcsched is a HPC job scheduler supporting job dependencies. Jobs and
dependencies can be stated in a file similar in syntax to a make file.

An example control file is

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
![Image of dependency graph](https://raw.githubusercontent.com/gt1/hpcsched/master/doc/depgraph.svg?sanitize=true)
