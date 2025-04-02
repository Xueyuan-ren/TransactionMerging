package com.oltpbenchmark.benchmarks.tpccgrpc;

import com.oltpbenchmark.api.AbstractTestWorker;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.benchmarks.tpccgrpc.TPCCgrpcBenchmark;

import java.util.List;

public class TestTPCCgrpcWorker extends AbstractTestWorker<TPCCgrpcBenchmark> {

    @Override
    public List<Class<? extends Procedure>> procedures() {
        return TestTPCCgrpcBenchmark.PROCEDURE_CLASSES;
    }

    @Override
    public Class<TPCCgrpcBenchmark> benchmarkClass() {
        return TPCCgrpcBenchmark.class;
    }

}
