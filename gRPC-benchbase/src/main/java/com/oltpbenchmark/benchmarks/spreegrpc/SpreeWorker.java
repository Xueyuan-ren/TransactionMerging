/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */


package com.oltpbenchmark.benchmarks.spreegrpc;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.spreegrpc.procedures.SpreeProcedure;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.grpcservice.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

public class SpreeWorker extends Worker<SpreegrpcBenchmark> {

    private static final Logger LOG = LoggerFactory.getLogger(SpreeWorker.class);

    private final int terminalWarehouseID;
    /**
     * Forms a range [lower, upper] (inclusive).
     */
    private final int terminalDistrictLowerID;
    private final int terminalDistrictUpperID;
    private final Random gen = new Random();

    private final int numWarehouses;

    private ManagedChannel channel;                                  // gRPC channel for each worker
    private TxnServiceGrpc.TxnServiceBlockingStub blockingStub;      // gRPC channel blocking Stub for each worker
    private String target = null;

    public SpreeWorker(SpreegrpcBenchmark benchmarkModule, int id,
                      int terminalWarehouseID, int terminalDistrictLowerID,
                      int terminalDistrictUpperID, int numWarehouses) {
        super(benchmarkModule, id);

        this.terminalWarehouseID = terminalWarehouseID;
        this.terminalDistrictLowerID = terminalDistrictLowerID;
        this.terminalDistrictUpperID = terminalDistrictUpperID;

        this.numWarehouses = numWarehouses;
        
        // rxy: Generate the gRPC channel and stub for this SpreeWorker to connect gRPC server
        this.target = "10.10.1.2:8080";

        this.channel = Grpc.newChannelBuilder(this.target, InsecureChannelCredentials.create()).build();
        this.blockingStub = TxnServiceGrpc.newBlockingStub(this.channel);

        // rxy: simple ping to establish physical TCP connections with the gRPC server
        try {
            blockingStub.ping(PingRequest.newBuilder().build());
        } catch (StatusRuntimeException e) {
            LOG.error("Failed to ping the gRPC server.", e);
            throw new RuntimeException("gRPC server is unreachable", e);
        }
    }

    /**
     * Executes a single Spree transaction of type transactionType.
     */
    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType nextTransaction) 
    throws UserAbortException, SQLException, StatusRuntimeException {
        try {
            SpreeProcedure proc = (SpreeProcedure) this.getProcedure(nextTransaction.getProcedureClass());
            proc.run(conn, channel, blockingStub, gen, terminalWarehouseID, numWarehouses,
                    terminalDistrictLowerID, terminalDistrictUpperID, this);
        } catch (ClassCastException ex) {
            //fail gracefully
            LOG.error("We have been invoked with an INVALID transactionType?!", ex);
            throw new RuntimeException("Bad transaction type = " + nextTransaction);
        }
        return (TransactionStatus.SUCCESS);
    }

    @Override
    protected long getPreExecutionWaitInMillis(TransactionType type) {
        // TPC-C 5.2.5.2: For keying times for each type of transaction.
        return type.getPreExecutionWait();
    }

    @Override
    protected long getPostExecutionWaitInMillis(TransactionType type) {
        // TPC-C 5.2.5.4: For think times for each type of transaction.
        long mean = type.getPostExecutionWait();

        float c = this.getBenchmark().rng().nextFloat();
        long thinkTime = (long) (-1 * Math.log(c) * mean);
        if (thinkTime > 10 * mean) {
            thinkTime = 10 * mean;
        }

        return thinkTime;
    }

    /**
     * Called at the end of the test to shut down gRPC channel for this worker if any.
     */
    @Override
    protected void shutDownChannel() {
        if (this.channel != null) {
            try {
                //LOG.info("Shut Down the gRPC Channel for this worker...");
                channel.shutdownNow().awaitTermination(5L, TimeUnit.SECONDS);
            } catch (Exception e) {
                //java Logger
                //logger.log(Level.WARNING, "exception thrown: {0}", e);
                //LOG.warn("exception thrown: {0}", e);
                LOG.error("gRPC channel couldn't be closed.", e);
            }
        }
    }

}
