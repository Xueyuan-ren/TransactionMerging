/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.benchmarks.tpccgrpc;

import com.oltpbenchmark.api.AbstractTestLoader;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.benchmarks.tpccgrpc.TPCCgrpcBenchmark;

import java.util.List;

public class TestTPCCLoader extends AbstractTestLoader<TPCCgrpcBenchmark> {

    @Override
    public List<Class<? extends Procedure>> procedures() {
        return TestTPCCgrpcBenchmark.PROCEDURE_CLASSES;
    }

    @Override
    public Class<TPCCgrpcBenchmark> benchmarkClass() {
        return TPCCgrpcBenchmark.class;
    }

}
