[Back to init](README.md)

# Do it yourself!

## Prerrequisites:

It is strongly recommended to read both the [initial README](README.md) and the BenchBase README [BenchBase README](BenchBase-README.md)


## Creating your own benchmarks

On one hand, BenchBase take as input a skeleton of a Java program and executes it. For example, for executing TPC-C benchmark under BenchBase, BenchBase requires the [TPC-C Java skeleton](src/main/java/com/oltpbenchmark/benchmarks/tpcc). On the other hand, [CheckSOBound's algorithm](src/main/java/com/oltpbenchmark/algorithmsHistory/algorithms/CheckSOBound.java) takes as input a [history](src/main/java/com/oltpbenchmark/historyModelHistory/History.java) obtained as an execution of a Java program and checks its consistency. For creating the history, it is required to modify the original code, capturing all SQL statements as suitable events. For example, we adapt TPC-C as ["TPC-C History version"](src/main/java/com/oltpbenchmark/benchmarksHistory/tpccHistories). Observe that the modifications are pretty straightforward and, in absence of JOIN predicates, do not modify the semantics of the original program. For generating your own benchmark, BenchBase provides a collection of other examples for you train on.

## Modifying Configuration Parameters

The proportion of each operation affects both performance and convergence to a buggy state. Those parameters can be modified manually through the configuration files in `config/`. For selecting the isolation level of each operation, and generating mixed isolation configurations, we introduce additional parameters on such configuration files (see more information about how to do it in the [experiment scripts](artifact-scripts)).


## CheckSOBound Beyond BenchBase

Our CheckSOBound implementation can be used beyond BenchBase. Observe that any Java program under the scope of our submission can be easily modified, introducing extra lines on each SQL query for extracting their histories. Our implementations do not depend on the BenchBase structure, so it can also be applied to your favorite benchmark!
