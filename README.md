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

To build CheckSOBound, we recommend using the script `docker-build.sh`.

```bash
bash docker-build.sh
```

This command builds the CheckSOBound on a Docker container and prepares it. The volume of the Docker is /benchbase/results directory. For building it in local, use script `local-build.sh` instead. For more information, we refer to [BenchBase instructions](BenchBase-README.md).


## Run

---

We include four scripts in the ``artifact-scripts`` directory  for running the project: ``smoke-test.sh``, ``session-scala.sh``, ``transaction-scala.sh``, and ``ser-rc-naive-csob.sh``; corresponding to the smoke-test, and the first, second and third experiment presented in section 6. It suffices to run them using ``bash`` for obtaining the results.

For example, for executing the smoke test, use:
```
bash artifact-scripts/smoke-test.sh
```

The results of each experiment appear in the folder ``results/testFiles/$experiment``.

**Notes**

The time limit set is to 60s per case. It is recommended to be careful when running each script as it may take more than 1 day to execute all experiments. The smoke-test is already predefined for having a better user experience. As the scripts are not fully run in Docker, please use `bash` over `sh`.

### Smoke test

The smoke test executes a reduced version of the other three experiments. After running the smoke test, three folders (`Smoke-Test-Sessions`, `Smoke-Test-Transactions` and `Smoke-Test-Comparisons`) are created, each with one sub-folder per benchmark (`twitter`, `tpcc` and `tpccPC` when applicable). In the case of `Smoke-Test-Sessions` and `Smoke-Test-Transactions`, each benchmark folder contains one sub-folder per isolation configuration (`SER`, `SI`, `RC`, `SER+RC`, `SI+RC`); while in the other case, only one sub-folder, (`Naive-vs-CheckSOBound`). At the end of the execution, each of these sub-folders contains two csvs. One, describing all information about all concerning executions and a second one, summarizing the executions showing the average result per case. In addition, in the case of the experiments `Smoke-Test-Sessions`, `Smoke-Test-Transactions` each benchmark folder  (`twitter`, `tpcc` and `tpccPC`) contain a `.png` describing the evolution of Algorithm 3 runtime (corresponding to Figures 5 & 6).

More information can be found in the file [smoke-test.sh](artifact-scripts/smoke-test.sh).




### First and Second Experiment: Session and Transaction Scalability

The following commands shall be run for obtaining their correspondent outcome. As in the smoke test, the result of the execution can be found in the folder `results/testFiles`.

```
bash artifact-scripts/session-scalability.sh
```

```
bash artifact-scripts/transaction-scalability.sh
```


### Third Experiment: Baseline Comparison


The following command shall be run; the result of the execution can be found in the folder `results/testFiles`.

```
bash artifact-scripts/comparison-naive-checksobound.sh
```

## Do it yourself!

---

If the reader wants to test their own programs, we recommend to read both the [BenchBase-README](BenchBase-README.md), that explains the usage of BenchBase, and the [DIY-README](DIY-README.md) that summarizes the new features that CheckSOBound brings.

## Requirements

---

This artifact was tested on a Mac OS. We recommend using a Mac/Linux OS version with updated software.

Docker is required. Please install it for your OS. The necessary documentation is available [here](https://docs.docker.com/get-docker).


Currently, our scripts are not fully integrated in Docker. The user may require installing xmlstarlet and python3.

<!---
This artifact was tested on a Linux OS. We recommend using a new Unix/Linux OS version with updated software.

Docker is required. Please install it for your OS. The necessary documentation is available [here](https://docs.docker.com/get-docker) and then follow the [post installation steps](https://docs.docker.com/engine/install/linux-postinstall) so that you can run `docker` commands without admin privileges or sudo.

-->

