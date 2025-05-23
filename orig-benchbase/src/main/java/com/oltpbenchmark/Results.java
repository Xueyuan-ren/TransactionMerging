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


package com.oltpbenchmark;

import com.oltpbenchmark.LatencyRecord.Sample;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.util.Histogram;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class Results {

    private final long nanoseconds;
    private final int measuredRequests;
    private final int measuredSuccessRequests;
    private final DistributionStatistics distributionStatistics;
    private final DistributionStatistics successDistributionStatistics;
    private final List<LatencyRecord.Sample> latencySamples;
    private final List<LatencyRecord.Sample> successLatencySamples;
    private final Histogram<TransactionType> unknown = new Histogram<>(false);
    private final Histogram<TransactionType> success = new Histogram<>(true);
    private final Histogram<TransactionType> abort = new Histogram<>(false);
    private final Histogram<TransactionType> retry = new Histogram<>(false);
    private final Histogram<TransactionType> error = new Histogram<>(false);
    private final Histogram<TransactionType> retryDifferent = new Histogram<>(false);
    private final Map<TransactionType, Histogram<String>> abortMessages = new HashMap<>();

    public Results(long nanoseconds, int measuredRequests, int measuredSuccessRequests, 
                    DistributionStatistics distributionStatistics, DistributionStatistics successDistributionStatistics,
                    final List<LatencyRecord.Sample> latencySamples, final List<LatencyRecord.Sample> successLatencySamples) {
        this.nanoseconds = nanoseconds;
        this.measuredRequests = measuredRequests;
        this.measuredSuccessRequests = measuredSuccessRequests;
        this.distributionStatistics = distributionStatistics;
        this.successDistributionStatistics = successDistributionStatistics;

        if (distributionStatistics == null) {
            this.latencySamples = null;
        } else {
            // defensive copy
            this.latencySamples = List.copyOf(latencySamples);

        }

        if (successDistributionStatistics == null) {
            this.successLatencySamples = null;
        } else {
            // defensive copy
            this.successLatencySamples = List.copyOf(successLatencySamples);

        }
    }

    public DistributionStatistics getDistributionStatistics() {
        return distributionStatistics;
    }

    public DistributionStatistics getSuccessDistributionStatistics() {
        return successDistributionStatistics;
    }

    public Histogram<TransactionType> getSuccess() {
        return success;
    }

    public Histogram<TransactionType> getUnknown() {
        return unknown;
    }

    public Histogram<TransactionType> getAbort() {
        return abort;
    }

    public Histogram<TransactionType> getRetry() {
        return retry;
    }

    public Histogram<TransactionType> getError() {
        return error;
    }

    public Histogram<TransactionType> getRetryDifferent() {
        return retryDifferent;
    }

    public Map<TransactionType, Histogram<String>> getAbortMessages() {
        return abortMessages;
    }

    public double requestsPerSecondThroughput() {
        return (double) measuredRequests / (double) nanoseconds * 1e9;
    }

    public double successRequestsPerSecondThroughput() {
        return (double) measuredSuccessRequests / (double) nanoseconds * 1e9;
    }

    public double requestsPerSecondGoodput() {
        return (double) success.getSampleCount() / (double) nanoseconds * 1e9;
    }

    public List<Sample> getLatencySamples() {
        return latencySamples;
    }

    public List<Sample> getSuccessLatencySamples() {
        return successLatencySamples;
    }

    public long getNanoseconds() {
        return nanoseconds;
    }

    public int getMeasuredRequests() {
        return measuredRequests;
    }

    public int getMeasuredSuccessRequests() {
        return measuredSuccessRequests;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Results(nanoSeconds=");
        sb.append(nanoseconds);
        sb.append(", measuredRequests=");
        sb.append(measuredRequests);
        sb.append(", measuredSuccessRequests=");
        sb.append(measuredSuccessRequests);
        sb.append(") = ");
        sb.append(successRequestsPerSecondThroughput());
        sb.append(" successRequests/sec (success throughput)");
        sb.append(", ");
        sb.append(requestsPerSecondThroughput());
        sb.append(" requests/sec (throughput)");
        sb.append(", ");
        sb.append(requestsPerSecondGoodput());
        sb.append(" requests/sec (goodput)");
        return sb.toString();
    }


}