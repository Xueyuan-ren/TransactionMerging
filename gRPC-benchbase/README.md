## Description

We implement the gRPC benchmark based on the Benchbase framework to include the grpc version TPC-C and Spree benchmark.

We currently have the following benchmarks: TPC-C, Spree.

---

## Quickstart

To compile the Benchbase using the `mysql` profile:

```bash
cd gRPC-benchbase
./mvnw -DskipTests clean package -P mysql
```

This produces artifacts in the `target` folder, which can be extracted:

```bash
cd target
tar xvzf benchbase-mysql.tgz
cd benchbase-mysql
```

To execute the Benchbase with `tpcc` benchmark:

```bash
java -jar benchbase.jar -b tpcc -c config/mysql/sample_tpcc_config.xml --create=true --load=true --execute=true
```

A full list of options can be displayed,

```bash
java -jar benchbase.jar -h
```

---

## Usage Guide

The following options are provided:

```text
usage: benchbase
 -b,--bench <arg>               [required] Benchmark class. Currently
                                supported: [tpcc, Spree]
 -c,--config <arg>              [required] Workload configuration file
    --clear <arg>               Clear all records in the database for this
                                benchmark
    --create <arg>              Initialize the database for this benchmark
 -d,--directory <arg>           Base directory for the result files,
                                default is current directory
    --dialects-export <arg>     Export benchmark SQL to a dialects file
    --execute <arg>             Execute the benchmark workload
 -h,--help                      Print this help
 -im,--interval-monitor <arg>   Throughput Monitoring Interval in
                                milliseconds
    --load <arg>                Load data using the benchmark's data
                                loader
 -s,--sample <arg>              Sampling window
```