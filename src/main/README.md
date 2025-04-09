[`Back to init`](../../README.md)

## Main code

### Structure

The code is structured in two layers:

- JPF code: [`gov directory`](gov/). The code in this folder is essentially the same provided by JPF. It includes the VM class family (Virtual Machine) that execute every bytecode instruction and update the local memory consequently. For more information read [`JPF README`](../../JPF-README.md).

- TrJPF code: [`country directory`](country/). Extends several JPF classes in the gov directory for using them in a transactional context.

    - Bytecode: Several Java instructions modified to be interpreted as database extern calls instead of JVM standard interpretation.
    - Database: Auxiliary classes for the algorithm. Include the or definition (Oracle.java) and the database class (Database.java). Database is used by both TrEventRegister.java to add new events into the database and by TrDFSearch.java to obtain the next alternative to explore.
    - Events: Package in charge of defining what is an event and creating them when a database call has been carried out.
    - Histories: Definition of several isolation levels in terms of history. They implement a method "isConsistent".
    - Report: Classes to output the outcome of our procedure.
    - Search: DFS iterative procedures. TrDFSearch.java implements EXPLORE(I_0, I) while NaiveTrDFSearch.java implements a Naive DFS without DPOR techniques.

