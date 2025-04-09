# Checking SO-Bounded Database executions Artifact
## Getting started

---

This repository contains the software artifact that supports the CAV'25 submission on "_On the Complexity of Checking Mixed Isolation Levels for SQL Transactions_". Checking SO-Bounded database executions (CSOB) is an extension of [`BenchBase`](BenchBase-README.md) capable to test whether a database isolation level implementation conforms with respect to their specifications.

Our artifact is split into different parts following the architecture of BenchBase. The main ones include:

- The directory [`src/main`](src/main/README.md) contains the main source code, including JPF and TrJPF.
- The directory [`src/examples`](src/examples/README.md) contains both the API database along with the source code that describe the benchmarks in section 7.2 (Courseware, ShoppingCart, TPC-C, Twitter and Wikipedia).
- The directory [`src/benchmarks`](src/benchmarks/README.md) contains the parameters that define clients tested in section 7.3 and the initial state of the database in each execution. Clients are instantiation of code from examples directory according to adequate parameters.
- The directory [`bin/benchmarks`](bin/benchmarks/README.md) will contain the output files after each execution.
- Configuration file [`jpf.properties`](jpf.properties), including the isolation levels, search mode, output configuration, client APIs to take into account, etc... Configuration parameters can be modified


## Build

---

For building, simply run the following:

```
docker build -t tr-jpf:latest .
```

## Run

---

The three experiments in section 7.3 have an associated script. It suffices to run it for obtaining the results. For a detailed description about the design, input or output of the experiments check the links above.

The following command shall be run before executing any experiment. It produces the container where our experiments will be run.

```
docker run -it tr-jpf:latest bash
```

Every experiment produce several short `.out` files. It is recommended to read their content either with cat or copying it to the host machine via [`docker cp`](https://docs.docker.com/engine/reference/commandline/cp/).

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

