## Quickstart

To clone and build BenchBase using the `mysql` profile,

```bash
git clone --depth 1 https://github.com/cmu-db/benchbase.git
cd benchbase
./mvnw clean package -P mysql
```

This produces artifacts in the `target` folder, which can be extracted,

```bash
cd target
tar xvzf benchbase-mysql.tgz
cd benchbase-mysql
```

Inside this folder, you can run BenchBase. For example, to execute the `tpcc` benchmark,

```bash
java -jar benchbase.jar -b tpcc -c config/mysql/sample_tpcc_config.xml --create=true --load=true --execute=true
```

A full list of options can be displayed,

```bash
java -jar benchbase.jar -h
```

---

## Description

Benchmarking is incredibly useful, yet endlessly painful. This benchmark suite is the result of a group of
PhDs/post-docs/professors getting together and combining their workloads/frameworks/experiences/efforts. We hope this
will save other people's time, and will provide an extensible platform, that can be grown in an open-source fashion.

BenchBase is a multi-threaded load generator. The framework is designed to be able to produce variable rate,
variable mixture load against any JDBC-enabled relational database. The framework also provides data collection
features, e.g., per-transaction-type latency and throughput logs.

The BenchBase framework has the following benchmarks:

This framework is design to allow for easy extension. We provide stub code that a contributor can use to include a new
benchmark, leveraging all the system features (logging, controlled speed, controlled mixture, etc.)

---

## Usage Guide

### How to Build
Run the following command to build the distribution for a given database specified as the profile name (`-P`).  
The following profiles are currently supported: `mysql`.

```bash
./mvnw clean package -P <profile name>
```

The following files will be placed in the `./target` folder:

* `benchbase-<profile name>.tgz`
* `benchbase-<profile name>.zip`

### How to Run
Once you build and unpack the distribution, you can run `benchbase` just like any other executable jar.  The following examples assume you are running from the root of the expanded `.zip` or `.tgz` distribution.  If you attempt to run `benchbase` outside of the distribution structure you may encounter a variety of errors including `java.lang.NoClassDefFoundError`.

To bring up help contents:
```bash
java -jar benchbase.jar -h
```

To execute the `tpcc` benchmark:
```bash
java -jar benchbase.jar -b tpcc -c config/mysql/sample_tpcc_config.xml --create=true --load=true --execute=true
```

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