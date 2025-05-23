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

We provide a built image in Docker for ARM and AMD.     For loading the image, use one of the commands below. The volume of the image is the `/benchbase/results` directory.

```bash
bash docker load < benchbase-artifact-postgres_arm64.tar
```

or

```bash
bash docker load < benchbase-artifact-postgres_amd64.tar
```


 For building it in local, use script `local-build.sh` instead. For more information, we refer to [BenchBase instructions](BenchBase-README.md).


## Run

---

We include four scripts in the ``artifact-scripts`` directory  for running the project: ``smoke-test.sh``, ``session-scala.sh``, ``transaction-scala.sh``, and ``ser-rc-naive-csob.sh``; corresponding to the smoke-test, and the first, second and third experiment presented in section 6. 

First, we run the docker container using the following command:

````
bash artifact-scripts/init.sh
````

After that, it suffices to run them using ``bash`` for obtaining the results.

For example, for executing the smoke test, use:
```
bash smoke-test.sh
```

The outcome of each experiment appears in the folder ``results``. For example, the configuration files for the first part of the smoke test, ``Smoke-Test-Sessions``, appear in ``results/config/Smoke-Test-Sessions``whereas all data of the experiment appears in ``results/testFiles/Smoke-Test-Sessions``.

**Notes**

The time limit set is to 60s per case. It is recommended to be careful when running each script as it may take more than 1 day to execute all experiments. The smoke-test is already predefined for having a better user experience. As the scripts rely on modern bash syntax, please use `bash` over `sh`.

### Smoke test

The smoke test executes a reduced version of the other three experiments. After running the smoke test, three folders (`Smoke-Test-Sessions`, `Smoke-Test-Transactions` and `Smoke-Test-Comparisons`) are created, each with one sub-folder per benchmark (`twitter`, `tpcc` and `tpccPC` when applicable). In the case of `Smoke-Test-Sessions` and `Smoke-Test-Transactions`, each benchmark folder contains one sub-folder per isolation configuration (`SER`, `SI+RC`); while in the other case, only one sub-folder, (`Naive-vs-CheckSOBound`). At the end of the execution, each of these sub-folders contains two csvs. One, describing all information about all concerning executions and a second one, summarizing the executions showing the average result per case. In addition, in the case of the experiments `Smoke-Test-Sessions`, `Smoke-Test-Transactions` each benchmark folder  (`twitter`, `tpcc` and `tpccPC`) contain a `.png` describing the evolution of Algorithm 3 runtime (corresponding to a smoke-version of Figures 5 & 6).

More information can be found in the file [smoke-test.sh](artifact-scripts/smoke-test.sh).



### First and Second Experiment: Session and Transaction Scalability

The following commands shall be run inside the docker container for obtaining their correspondent outcome.

```
bash session-scalability.sh
```

```
bash transaction-scalability.sh
```

Please observe that now we consider five isolation configurations (`SER`, `SI`, `RC`, `SER+RC`, `SI+RC`), as described in the paper, and larger size of programs. We refer to our submission, the smoke-test description above and [the session script](artifact-scripts/session-scalability.sh) and [the transaction script](artifact-scripts/session-scalability.sh) for more information.



### Third Experiment: Baseline Comparison


The following command shall be run.

```
bash comparison-naive-checksobound.sh
```
We refer to our submission, the smoke-test description above and [the comparison script](artifact-scripts/comparison-naive-checksobound.sh) for more information.

## Do it yourself!

---

If the reader wants to test their own programs, we recommend to read both the [BenchBase-README](BenchBase-README.md), that explains the usage of BenchBase, and the [DIY-README](DIY-README.md) that summarizes the new features that CheckSOBound brings.

## Requirements

---

This artifact was tested on a Mac OS. We recommend using a Mac/Linux OS version with updated software.

Docker is required. Please install it for your OS. The necessary documentation is available [here](https://docs.docker.com/get-docker). Docker composed is also required. For Mac OS and Windows distributions, docker composed is included with docker. For Linux distributions it is required to install it separately (see more information [here](https://github.com/docker/compose/tree/main?tab=readme-ov-file#linux))

<!---
Currently, our scripts are not fully integrated in Docker. The user may require installing python3 for generating the csvs and graphics.


This artifact was tested on a Linux OS. We recommend using a new Unix/Linux OS version with updated software.

Docker is required. Please install it for your OS. The necessary documentation is available [here](https://docs.docker.com/get-docker) and then follow the [post installation steps](https://docs.docker.com/engine/install/linux-postinstall) so that you can run `docker` commands without admin privileges or sudo.

-->

