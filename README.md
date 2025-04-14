# Checking SO-Bounded Database executions Artifact
## Getting started

---

This repository contains the software artifact that supports the CAV'25 submission on "_On the Complexity of Checking Mixed Isolation Levels for SQL Transactions_". Checking SO-Bounded database executions (CheckSOBound) is an extension of BenchBase capable to test whether a database isolation level implementation conforms with respect to their specifications. We encourage the reviewers to read the [BenchBase README](BenchBase-README.md).

Our artifact is split into different parts following the architecture of BenchBase. The main ones include:

- The directory `src/main` contains the main source code, including the BenchBase project and the CheckSOBound extension. See more information about the source code in this [README file](src/main/README.md).

- The directory `config` contains a skeleton of the parameters employed for each benchmark.
- The directory `docker` contains information about generating a benchmark for docker using BenchBase.
- The directory `target/benchbase-postgres` will contain the compiled version of the source code. The directory `target/benchbase-postgres/results` will contain the results of the experiment.
- The directory `data` contains additional data used for BenchBase and CheckSOBound benchmarks.

## Build

To build CheckSOBound, we recommend use the script `docker-build.sh`.

```bash
bash artifact-scripts/docker-build.sh
```

This command builds the CheckSOBound on a Docker container and prepares it. The volume of the Docker is /benchbase/results directory. For building it in local, use script `local-build.sh` instead. For more information, we refer to [BenchBase instructions](BenchBase-README.md).


## Run

---

We include four scripts for running the project: ``smoke-test.sh``, ``session-scala.sh``, ``transaction-scala.sh``, and ``ser-rc-naive-csob.sh``; corresponding to the smoke test, and the first, second and third experiment presented in section 6. It suffices to run them using ``bash`` for obtaining the results.

For example, for executing the smoke test, use:
```
bash smoke-test.sh
```

Every experiment execute multiple benchmarks and configurations. Each execution is stored in a different folder ``results/testFiles/$benchmark_name/$isolation_configuration/$case``. The executed benchmarks (``$benchmark_name``) either ``tpccHistories``, ``tpccPChistories``or ``twitterHistories``.


produce several short `.out` files. It is recommended to read their content either with cat or copying it to the host machine via [`docker cp`](https://docs.docker.com/engine/reference/commandline/cp/).

**Notes**

The time limit set is to 30' per case. It is recommended to be careful when running each script as it may take up to 1 day per script. Some sub-benchmarks are already predefined for having a satisfactory user experience.


### First experiment: application scalability

The following command shall be run. Its outcome can be found in "bin/benchmarks/application-scalability" folder.

```
bash bench-application-scalability.sh
```

It will produce 5 folders ("courseware/", "shoppingCart/", "tpcc/", "twitter/" and "wikipedia/"), each with 5 subfolders (one per number of sessions in the benchmark). Each subfolder will contain 7 .out files, one per isolation level treated (Appendix F, Table F1).

#### Demo version

One can run the command below to execute a smaller benchmark where only the first two rows of Table F1 are executed. The results of this test case can be found in `bin/benchmarks/demo-application-scalability`.

```
bash bench-demo-app.sh
```

### Second and third experiment: session and transaction scalability

The following commands shall be run:

- Second experiment. Its outcome can be found in "bin/benchmarks/session-scalability" folder:

```
bash bench-session-scalability.sh
```

- Third experiment. Its outcome can be found in "bin/benchmarks/transaction-scalability" folder:

```
bash bench-transaction-scalability.sh
```

Both of them will produce 2 folders ("tpcc/" and "wikipedia/"), each with 5 subfolders (one per study case). Inside them, 5 folders can be found; obtaining in total a system of 50 folders.
For example, one of those final directories will be `bin/benchmarks/transaction-scalability/tpcc/case1/2-transactions-per-session`.

Each final folder will contain 1 .out file, corresponding with a cell in Appendix F, Table F2 or Appendix F, Table F3.


#### Demo version

One can run the command below to execute a smaller benchmark where only the first two rows and first three columns of Table F2 (respectively F3) are executed. The results of this test case can be found in `bin/benchmarks/demo-session-scalability` (respectively `bin/benchmarks/demo-transaction-scalability`).

**Second experiment:**
```
bash bench-demo-session.sh
```

**Third experiment:**
```
bash bench-demo-transaction.sh
```

## Do it yourself!

---

If the reader wants to test their own programs, we recommend to read both the [`JPF-README`](JPF-README.md), that explains the usage of JPF, and the [`DIY-README`](DIY-README.md) that summarizes the new features that TrJPF brings.

## Requirements

---

This artifact was tested on a Mac OS. We recommend using a Mac/Linux OS version with updated software.

Docker is required. Please install it for your OS. The necessary documentation is available [here](https://docs.docker.com/get-docker).

<!---
This artifact was tested on a Linux OS. We recommend using a new Unix/Linux OS version with updated software.

Docker is required. Please install it for your OS. The necessary documentation is available [here](https://docs.docker.com/get-docker) and then follow the [post installation steps](https://docs.docker.com/engine/install/linux-postinstall) so that you can run `docker` commands without admin privileges or sudo.

-->

