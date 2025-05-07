package com.grpc.examples;

import com.grpc.client.*;
import com.grpc.examples.Procedure.UserAbortException;
import com.grpc.examples.spree.SpreeNewOrder;
import com.grpc.examples.tpcc.NewOrder;
import com.grpc.examples.tpcc.TPCCConfig;
import io.grpc.stub.StreamObserver;
import io.grpc.Status;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TxnServiceImpl extends TxnServiceGrpc.TxnServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(TxnServiceImpl.class);

    public final static String url = "jdbc:mysql://10.10.1.1:3306/benchbase?rewriteBatchedStatements=true";
    public final static String user = "java";
    public final static String password = "xueyuanren91";

    private volatile int mergeSize = 1;
    private volatile int numThreads;
    private static double currentThroughput;
    private int maxThreads = 100;
    private ArrayList<LinkedBlockingQueue<GrpcMessage>> msgQueues;
    private List<MergeThread> mergeThreads;
    private int WAREHOUSE;
    private int AllDistricts;
    //private int threadPerDist;
    private int distPerThread;
    private int threadPerWhse;

    private static final int timeout = 5000;
    private final AtomicInteger processedCount = new AtomicInteger(0);
    private final String modelServiceHost = "localhost";
    private final int modelServicePort = 50051; 
    private final ModelServiceClient modelServiceClient;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> scheduledReport;
    // Parameter exploration
    private final AtomicInteger reportCounter = new AtomicInteger(0);
    private int iterationLimit = 10; // Example limit for exploration iterations
    private static final int MIN_MERGE_SIZE = 1;
    private static final int MAX_MERGE_SIZE = 10;
    private static final int MIN_THREAD_NUM = 1;
    private static final int MAX_THREAD_NUM = 10;
    private static final double IMPROVEMENT_THRESHOLD = 0.05; // Example threshold

    public TxnServiceImpl(int warehouse, int merge, int thread, int maxThreads, int iterationLimit) {
        this.modelServiceClient = new ModelServiceClient(modelServiceHost, modelServicePort);
        this.mergeSize = merge;
        this.WAREHOUSE = warehouse;
        this.threadPerWhse = thread;
        this.maxThreads = maxThreads;
        this.iterationLimit = iterationLimit;
        this.AllDistricts = WAREHOUSE * TPCCConfig.configDistPerWhse;
        msgQueues = new ArrayList<LinkedBlockingQueue<GrpcMessage>>();
        this.numThreads = Math.min(this.threadPerWhse * this.WAREHOUSE, this.maxThreads);

        //this.threadPerDist = this.numThreads / this.AllDistricts;
        double distPerThreadDouble = Math.max(1, (double) AllDistricts/numThreads); // each thread handles at least 1 district
        this.distPerThread = (int) Math.ceil(distPerThreadDouble);

        mergeThreads = new ArrayList<MergeThread>();
        for (int i = 0; i < this.maxThreads; i++) {
            LinkedBlockingQueue<GrpcMessage> msgQueue = new LinkedBlockingQueue<GrpcMessage>();
            msgQueues.add(msgQueue);
        }
        for (int i = 0; i < this.maxThreads; i++) {
            MergeThread mergeThread = new MergeThread(i, mergeSize, msgQueues.get(i), AllDistricts);
            mergeThreads.add(mergeThread);
            mergeThreads.get(i).start();
        }
    }

    public synchronized int getThreadNum() {
        return numThreads;
    }

    public synchronized int getMergeSize() {
        return mergeSize;
    }

    public synchronized double getCurrentThroughput() {
        return currentThroughput;
    }

    private int generateRandomMergeSize() {
        return ThreadLocalRandom.current().nextInt(MIN_MERGE_SIZE, MAX_MERGE_SIZE + 1);
    }
    
    private int generateRandomThreadNum() {
        int threadNum = ThreadLocalRandom.current().nextInt(MIN_THREAD_NUM, MAX_THREAD_NUM + 1);
        return Math.min(threadNum, maxThreads); // Ensure it doesn't exceed server's max
    }

    private void reportParameters(int intervalReport) {
        currentThroughput = calculateThroughput(intervalReport);
        int currentMergeSize = getMergeSize();
        int currentThreadNum = getThreadNum();
        if (currentThroughput == 0) {
            System.out.println("Throughput is 0, no need to report metrics.");
            return;            
        }

        int currentCount = reportCounter.incrementAndGet(); // Track iteration count

        // Report the metrics to the model service
        UpdateResponse response = modelServiceClient.reportAndUpdate(
            currentMergeSize, currentThreadNum, currentThroughput);
        boolean success = response.getSuccess();

        if (success) {
            System.out.println("Metrics reported.");

            if (currentCount < iterationLimit) {
                // Phase 1: Initial exploration (first 10 iterations)
                // Set new random parameters for the next interval
                int newMergeSize = generateRandomMergeSize();
                int newThreadNum = generateRandomThreadNum();
                boolean updated = updateParameters(newMergeSize, newThreadNum);
                if (updated) {
                    LOG.info("Exploring parameters: mergeSize={}, threadNum={}", newMergeSize, newThreadNum);
                    scheduleNextReport();
                } else {
                    LOG.error("Failed to update parameters during exploration.");
                    scheduleNextReport(); // Retry even if update fails
                }
            } else {
                // Phase 2: Optimization based on model service prediction
                Prediction prediction = response.getPrediction();
                int newMergeSize = prediction.getOptimalMergeSize();
                int newThreadNum = prediction.getOptimalThreadNum();
                double expectedThroughput = prediction.getExpectedThroughput();

                // Check if the parameters have changed
                boolean parametersChanged = (newMergeSize != currentMergeSize || newThreadNum != currentThreadNum);
                // Check if the expected throughput is significant
                boolean improvementSufficient = ((expectedThroughput - currentThroughput) / currentThroughput) > IMPROVEMENT_THRESHOLD;
                // boolean improvementSufficient = (expectedThroughput - currentThroughput) > IMPROVEMENT_THRESHOLD;

                if (parametersChanged || improvementSufficient) {
                    // Apply model's suggestion
                    boolean updated = updateParameters(newMergeSize, newThreadNum);
                    if (updated) {
                        LOG.info("Optimized to mergeSize={}, threadNum={}, expectedThroughput={}", 
                            newMergeSize, newThreadNum, expectedThroughput);
                        // Reschedule the report after updating parameters
                        scheduleNextReport();
                    } else {
                        LOG.warn("Updating failed. Keeping current parameters.");
                        scheduleNextReport();
                    }
                } else {
                    // Stop if no improvement and parameters are optimal
                    LOG.info("Stopping auto-tuning. Optimal parameters and insufficient improvement.");
                    LOG.info("Current parameters: mergeSize={}, threadNum={} after {} iterations", 
                        currentMergeSize, currentThreadNum, currentCount);
                }
            }
        } else {
            System.out.println("Failed to report metrics. Retrying...");
            scheduleNextReport(); // Retry on failure
        }
    }

    private double calculateThroughput(int interval) {
        // Update to log the throughput every minute
        double throughput = processedCount.getAndSet(0) / (interval * 1.0); // Requests per second
        LOG.info("Current throughput: {} requests/sec", throughput);
        return throughput;
    }

    private Boolean updateParameters(int mergeSize, int threadNum) {
        this.mergeSize = mergeSize;
        int newNumThreads = Math.min(threadNum, this.maxThreads);
        this.numThreads = newNumThreads;

        for (MergeThread mergeThread : mergeThreads) {
            mergeThread.setMergeSize(mergeSize);
        }

        LOG.info("Updated parameters: mergeSize={}, threadNum={}", this.mergeSize, this.numThreads);
        return true;
    }

    // Helper method to schedule the next report
    private void scheduleNextReport() {
        processedCount.set(0); // Reset throughput counter for new measurements
        // Cancel any pending report
        if (scheduledReport != null && !scheduledReport.isDone()) {
            scheduledReport.cancel(false);
        }
        // Schedule report after 60 seconds
        scheduledReport = scheduler.schedule(() -> {
            reportParameters(10); // Calculate throughput over 30 seconds
        }, 10, TimeUnit.SECONDS);
    }

    public void shutdown() {
        // Shutdown all merge thread connections
        for (int i = 0; i < mergeThreads.size(); i++) {
            mergeThreads.get(i).shutdown();
        }
        // Shutdown the scheduler
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }
        // Shutdown the model service client
        modelServiceClient.shutdown();
        try {
            if (!modelServiceClient.isChannelActive()) {
                modelServiceClient.shutdown();
            }
        } catch (Exception e) {
            LOG.warn("Error shutting down model service client: {}", e.getMessage());
        }
    }

    @Override
    public void ping(PingRequest request, StreamObserver<PingResponse> responseObserver) {
        PingResponse response = PingResponse.newBuilder().build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public synchronized void tuneServer(TuneServerRequest request, StreamObserver<TuneServerReply> responseObserver)  {
        int mergeSize = request.getMergeSize();
        int threadCount = request.getThreadCount();
        boolean tuned = updateParameters(mergeSize, threadCount);

        // Reset the exploration counter on manual tuning
        reportCounter.set(0);
        // Schedule the report to run after 60 seconds
        scheduleNextReport();

        TuneServerReply response = TuneServerReply.newBuilder().setTuned(tuned).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void makeConnections(OpenRequest request, StreamObserver<OpenReply> responseObserver) {
        
        OpenReply response;
        response = OpenReply.newBuilder().setOpened(true).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void closeConnections(CloseRequest request, StreamObserver<CloseReply> responseObserver) {

        CloseReply response;
        response = CloseReply.newBuilder().setClosed(true).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
        System.exit(0);
    }

    @Override
    public void newOrderTxn(NewOrderRequest request, StreamObserver<NewOrderReply> responseObserver)  {
        
        NewOrderReply response = null;
        Status status = null;
        NewOrderMessage reqAndReply = new NewOrderMessage();
        reqAndReply.setRequest(request);
        reqAndReply.setReply(response);
        reqAndReply.setStatus(status);
        int w_id = request.getTerminalWarehouseID();
        int district_id = request.getDistrictID();
        
        //int workid = request.getWorkid();
        //int idx = workid % numThreads;
        int idx = ((w_id - 1) * TPCCConfig.configDistPerWhse + (district_id - 1)) % this.numThreads;

        try {
            msgQueues.get(idx).put(reqAndReply);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }

        synchronized(reqAndReply){
            long startTime = System.currentTimeMillis();
            while (reqAndReply.getReply() == null && reqAndReply.getStatus() == null) {
                try {
                    long elapsedTime = System.currentTimeMillis() - startTime;
                    if (elapsedTime >= timeout) {
                        break;
                    }
                    reqAndReply.wait(timeout - elapsedTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                }
            }
        }

        if (reqAndReply.getReply() == null && reqAndReply.getStatus() == null) {
            // wait timeout happens, merge fails for this request, may retry
            status = Status.INTERNAL.withDescription("Retryable SQLException:");
            reqAndReply.setStatus(status);
        }
        
        if (reqAndReply.getReply() != null) {
            // transaction successful in merge execution
            responseObserver.onNext(reqAndReply.getReply());
            responseObserver.onCompleted();
            processedCount.incrementAndGet();
        } else if (reqAndReply.getStatus() != null) {
            // transaction fails in merge execution, return status
            responseObserver.onError(reqAndReply.getStatus().asRuntimeException());
        } else {
            // unexpected error occurs
            throw new RuntimeException("Unexpected error occurs when executing newOrderTxn.");
        }

    }

    @Override
    public void paymentTxn(PaymentRequest request, StreamObserver<PaymentReply> responseObserver) {
        
        PaymentReply response = null;
        Status status = null;
        PaymentMessage reqAndReply = new PaymentMessage();
        reqAndReply.setRequest(request);
        reqAndReply.setReply(response);
        reqAndReply.setStatus(status);
        int w_id = request.getTerminalWarehouseID();
        int district_id = request.getDistrictID();
        int idx = ((w_id - 1) * TPCCConfig.configDistPerWhse + (district_id - 1)) % this.numThreads;

        try {
            msgQueues.get(idx).put(reqAndReply);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }

        synchronized(reqAndReply){
            long startTime = System.currentTimeMillis();
            while (reqAndReply.getReply() == null && reqAndReply.getStatus() == null) {
                try {
                    long elapsedTime = System.currentTimeMillis() - startTime;
                    if (elapsedTime >= timeout) {
                        break;
                    }
                    reqAndReply.wait(timeout - elapsedTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                }
            }
        }

        if (reqAndReply.getReply() == null && reqAndReply.getStatus() == null) {
            // wait timeout happens, merge fails for this request, may retry
            status = Status.INTERNAL.withDescription("Retryable SQLException:");
            reqAndReply.setStatus(status);
        }

        if (reqAndReply.getReply() != null) {
            // transaction successful in merge execution
            responseObserver.onNext(reqAndReply.getReply());
            responseObserver.onCompleted();
            processedCount.incrementAndGet();
        } else if (reqAndReply.getStatus() != null) {
            // transaction fails in merge execution, return status
            responseObserver.onError(reqAndReply.getStatus().asRuntimeException());
        } else {
            // unexpected error occurs
            throw new RuntimeException("Unexpected error occurs when executing paymentTxn.");
        }

    }

    @Override
    public void deliveryTxn(DeliveryRequest request, StreamObserver<DeliveryReply> responseObserver) {
        
        DeliveryReply response = null;
        Status status = null;
        DeliveryMessage reqAndReply = new DeliveryMessage();
        reqAndReply.setRequest(request);
        reqAndReply.setReply(response);
        reqAndReply.setStatus(status);
        int w_id = request.getTerminalWarehouseID();
        // take terminalDistrictUpperID as the district_id for delivery request
        int district_id = request.getTerminalDistrictUpperID();
        int idx = ((w_id - 1) * TPCCConfig.configDistPerWhse + (district_id - 1)) % this.numThreads;

        try {
            msgQueues.get(idx).put(reqAndReply);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }

        synchronized(reqAndReply){
            long startTime = System.currentTimeMillis();
            while (reqAndReply.getReply() == null && reqAndReply.getStatus() == null) {
                try {
                    long elapsedTime = System.currentTimeMillis() - startTime;
                    if (elapsedTime >= timeout) {
                        break;
                    }
                    reqAndReply.wait(timeout - elapsedTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                }
            }
        }

        if (reqAndReply.getReply() == null && reqAndReply.getStatus() == null) {
            // wait timeout happens, merge fails for this request, may retry
            status = Status.INTERNAL.withDescription("Retryable SQLException:");
            reqAndReply.setStatus(status);
        }

        if (reqAndReply.getReply() != null) {
            // transaction successful in merge execution
            responseObserver.onNext(reqAndReply.getReply());
            responseObserver.onCompleted();
            processedCount.incrementAndGet();
        } else if (reqAndReply.getStatus() != null) {
            // transaction fails in merge execution, return status
            responseObserver.onError(reqAndReply.getStatus().asRuntimeException());
        } else {
            // unexpected error occurs
            throw new RuntimeException("Unexpected error occurs when executing deliveryTxn.");
        }

    }

    @Override
    public void orderStatusTxn(OrderStatusRequest request, StreamObserver<OrderStatusReply> responseObserver) {
        
        OrderStatusReply response = null;
        Status status = null;
        OrderStatusMessage reqAndReply = new OrderStatusMessage();
        reqAndReply.setRequest(request);
        reqAndReply.setReply(response);
        reqAndReply.setStatus(status);
        int w_id = request.getTerminalWarehouseID();
        int district_id = request.getDistrictID();
        int idx = ((w_id - 1) * TPCCConfig.configDistPerWhse + (district_id - 1)) % this.numThreads;

        try {
            msgQueues.get(idx).put(reqAndReply);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }

        synchronized(reqAndReply){
            long startTime = System.currentTimeMillis();
            while (reqAndReply.getReply() == null && reqAndReply.getStatus() == null) {
                try {
                    long elapsedTime = System.currentTimeMillis() - startTime;
                    if (elapsedTime >= timeout) {
                        break;
                    }
                    reqAndReply.wait(timeout - elapsedTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                }
            }
        }

        if (reqAndReply.getReply() == null && reqAndReply.getStatus() == null) {
            // wait timeout happens, merge fails for this request, may retry
            status = Status.INTERNAL.withDescription("Retryable SQLException:");
            reqAndReply.setStatus(status);
        }

        if (reqAndReply.getReply() != null) {
            // transaction successful in merge execution
            responseObserver.onNext(reqAndReply.getReply());
            responseObserver.onCompleted();
            processedCount.incrementAndGet();
        } else if (reqAndReply.getStatus() != null) {
            // transaction fails in merge execution, return status
            responseObserver.onError(reqAndReply.getStatus().asRuntimeException());
        } else {
            // unexpected error occurs
            throw new RuntimeException("Unexpected error occurs when executing OrderStatusTxn.");
        }

    }

    @Override
    public void stockLevelTxn(StockLevelRequest request, StreamObserver<StockLevelReply> responseObserver) {
        
        StockLevelReply response = null;
        Status status = null;
        StockLevelMessage reqAndReply = new StockLevelMessage();
        reqAndReply.setRequest(request);
        reqAndReply.setReply(response);
        reqAndReply.setStatus(status);
        int w_id = request.getTerminalWarehouseID();
        int district_id = request.getDistrictID();
        int idx = ((w_id - 1) * TPCCConfig.configDistPerWhse + (district_id - 1)) % this.numThreads;

        try {
            msgQueues.get(idx).put(reqAndReply);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }

        synchronized(reqAndReply){
            long startTime = System.currentTimeMillis();
            while (reqAndReply.getReply() == null && reqAndReply.getStatus() == null) {
                try {
                    long elapsedTime = System.currentTimeMillis() - startTime;
                    if (elapsedTime >= timeout) {
                        break;
                    }
                    reqAndReply.wait(timeout - elapsedTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                }
            }
        }

        if (reqAndReply.getReply() == null && reqAndReply.getStatus() == null) {
            // wait timeout happens, merge fails for this request, may retry
            status = Status.INTERNAL.withDescription("Retryable SQLException:");
            reqAndReply.setStatus(status);
        }

        if (reqAndReply.getReply() != null) {
            // transaction successful in merge execution
            responseObserver.onNext(reqAndReply.getReply());
            responseObserver.onCompleted();
            processedCount.incrementAndGet();
        } else if (reqAndReply.getStatus() != null) {
            // transaction fails in merge execution, return status
            responseObserver.onError(reqAndReply.getStatus().asRuntimeException());
        } else {
            // unexpected error occurs
            throw new RuntimeException("Unexpected error occurs when executing StockLevelTxn.");
        }

    }

    @Override
    public void spreeNewOrderTxn(SpreeNewOrderRequest request, StreamObserver<SpreeNewOrderReply> responseObserver)  {
        
        SpreeNewOrderReply response = null;
        Status status = null;
        SpreeNewOrderMessage reqAndReply = new SpreeNewOrderMessage();
        reqAndReply.setRequest(request);
        reqAndReply.setReply(response);
        reqAndReply.setStatus(status);

        int workid = request.getWorkid();
        int idx = workid % this.numThreads;

        try {
            msgQueues.get(idx).put(reqAndReply);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }

        synchronized(reqAndReply){
            long startTime = System.currentTimeMillis();
            while(reqAndReply.getReply() == null && reqAndReply.getStatus() == null) {
                try {
                    long elapsedTime = System.currentTimeMillis() - startTime;
                    if (elapsedTime >= timeout) {
                        break;
                    }
                    reqAndReply.wait(timeout - elapsedTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                }
            }
        }

        if (reqAndReply.getReply() == null && reqAndReply.getStatus() == null) {
            //wait timeout happens, merge fails for this request, may retry
            LOG.info("wait timeout happens, merge fails for this spreeNeworder request, may retry");
            status = Status.INTERNAL.withDescription("Retryable SQLException:");
            reqAndReply.setStatus(status);
        }

        if (reqAndReply.getReply() != null) {
            // transaction succeeds in merging
            responseObserver.onNext(reqAndReply.getReply());
            responseObserver.onCompleted();
        } else if (reqAndReply.getStatus() != null) {
            // error happens in merging, return status
            responseObserver.onError(reqAndReply.getStatus().asRuntimeException());
        } else {
            // unexpected error occurs
            throw new RuntimeException("Unexpected error occurs when executing spreeNewOrderTxn.");
        }

    }

    @Override
    public void spreeAddItemTxn(SpreeAddItemRequest request, StreamObserver<SpreeAddItemReply> responseObserver)  {
        
        SpreeAddItemReply response = null;
        Status status = null;
        SpreeAddItemMessage reqAndReply = new SpreeAddItemMessage();
        reqAndReply.setRequest(request);
        reqAndReply.setReply(response);
        reqAndReply.setStatus(status);

        int workid = request.getWorkid();
        int idx = workid % this.numThreads;

        try {
            msgQueues.get(idx).put(reqAndReply);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }

        synchronized(reqAndReply){
            long startTime = System.currentTimeMillis();
            while (reqAndReply.getReply() == null && reqAndReply.getStatus() == null) {
                try {
                    long elapsedTime = System.currentTimeMillis() - startTime;
                    if (elapsedTime >= timeout) {
                        break;
                    }
                    reqAndReply.wait(timeout - elapsedTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                }
            }
        }

        if (reqAndReply.getReply() == null && reqAndReply.getStatus() == null) {
            //wait timeout happens, merge fails for this request, may retry
            LOG.info("wait timeout happens, merge fails for this spreeAddItem request, may retry");
            status = Status.INTERNAL.withDescription("Retryable SQLException:");
            reqAndReply.setStatus(status);
        }

        if (reqAndReply.getReply() != null) {
            // transaction succeeds in merging
            responseObserver.onNext(reqAndReply.getReply());
            responseObserver.onCompleted();
        } else if (reqAndReply.getStatus() != null) {
            // error happens in merging, return status
            responseObserver.onError(reqAndReply.getStatus().asRuntimeException());
        } else {
            // unexpected error occurs
            throw new RuntimeException("Unexpected error occurs when executing spreeAddItemTxn.");
        }

    }

}
