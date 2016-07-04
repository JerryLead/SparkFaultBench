
## Topic
FaultFactory: A Fault-tolerance Benchmarking Suite for Big Data Frameworks

## Description
Current big data benchmarks (e.g., HiBench) aim to test the performance of big data frameworks (e.g., Hadoop MapReduce).
However, there is not a suite to test whether the deployed frameworks can tolerate static and runtime errors, including
data errors, configuration errors, program errors, hardware errors, etc.

This project aims to build a benchmarking suite to test the fault-tolerance capacity of big data frameworks (Hadoop and
Spark). We expect to use it to find bugs in current frameworks and also use it to detect the configuration/data/program
faults in the running applications.