package com.grpc.client;

import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class ModelServiceClient {

    private final ManagedChannel channel;
    private final ModelServiceGrpc.ModelServiceBlockingStub blockingStub;

    public ModelServiceClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .build();
        this.blockingStub = ModelServiceGrpc.newBlockingStub(channel);
    }

    public UpdateResponse reportAndUpdate(int mergeSize, int threadNum, double throughput) {
        // Report the metrics to the model service
        DataPoint dataPoint = DataPoint.newBuilder()
            .setMergeSize(mergeSize)
            .setThreadNum(threadNum)
            .setThroughput(throughput)
            .build();

        PerformanceData request = PerformanceData.newBuilder()
            .addHistoricalData(dataPoint)
            .build();
        // Update the model with the new data and get prediction
        UpdateResponse response = blockingStub.updatePerformanceData(request);
        // Boolean updated = response.getUpdated();
        // Prediction prediction = response.getPrediction();
        return response;
    }

    // check if the channel is working
    public boolean isChannelActive() {
        return !channel.isShutdown();
    }
    
    public void shutdown() {
        if (!channel.isShutdown()) {
            channel.shutdownNow();
        }
    }
}
