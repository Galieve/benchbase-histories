[Back to init](../../README.md)

## Main code

This directory contains the main part of the code of both BenchBase project and CSOB artifact. For distinguishing our contribution from the source code of BenchBase, our code is contained on directories whose name contain the suffix `History`.

### Structure

The code is structured in three layers:

- [Assembly code](assembly/): The code in this folder is the same provided by BenchBase; we refer to the original BenchBase project for more information.

- [Java code](java/com/oltpbenchmark/): It contains the Java source code. The main class corresponds to [DBWorkloadHistory](java/com/oltpbenchmark/DBWorkloadHistory.java).

  - [History directory](java/com/oltpbenchmark/historyModelHistory): Definition of events, histories and isolation levels as in Sections 2 and 3.
  - [Algorithm directory](java/com/oltpbenchmark/algorithmsHistory): Includes the implementation of [CheckConsistency algorithm](java/com/oltpbenchmark/algorithmsHistory/algorithms/CSOB.java) (Algorithm 3) as well the [naive algorithm](java/com/oltpbenchmark/algorithmsHistory/algorithms/NaiveCheckClientConsistency.java) (see Section 6).

  - [Benchmark directory](java/com/oltpbenchmark/benchmarksHistory): Include Twitter, TPC-C and TPC-C PC benchmarks (see Section 6).
  - [Util directory](java/com/oltpbenchmark/utilHistory): Miscellaneous package for utilities


- [Resources code](resources/): Include the definition of the PostgreSQL database of each benchmark, including the data type of each column, table and additional database structures such as indexes. BenchBase resources showcase how to extend to other database providers and how to initialize the database from a custom state.