package com.grpc.examples;

import com.grpc.examples.tpcc.DeliveryMT;
import com.grpc.examples.tpcc.NewOrderMT;
import com.grpc.examples.tpcc.OrderStatus;
import com.grpc.examples.tpcc.PaymentMT;
import com.grpc.examples.tpcc.StockLevel;
import com.grpc.examples.tpcc.TPCCConfig;
import com.grpc.examples.spree.SpreeNewOrderMT;
import com.grpc.examples.spree.SpreeAddItemMT;

import com.grpc.examples.Procedure.UserAbortException;
import io.grpc.Status;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(MergeThread.class);

    private Connection conn = null;
    private final int threadId;
    private List<List<NewOrderMessage>> newOrderList;
    private List<List<PaymentMessage>> paymentList;
    private List<SpreeNewOrderMessage> spreeNewOrderList;
    private List<SpreeAddItemMessage> spreeAddItemList;
    private LinkedBlockingQueue<GrpcMessage> msgQueue;
    private volatile int MERGE_SIZE;
    private int distPerThread;
    private final AtomicInteger intervalRequests = new AtomicInteger(0);
    private volatile boolean running = true;

    public MergeThread(int id, int merge, LinkedBlockingQueue<GrpcMessage> globalmsgQueue, int allDistricts) {
        super("Merge-Thread-" + id);
        this.threadId = id; 
        this.MERGE_SIZE = merge;
        this.msgQueue = globalmsgQueue;  
        this.distPerThread = distPerThread;
        newOrderList = new ArrayList<>(allDistricts);
        paymentList = new ArrayList<>(allDistricts);
        // maintain a message queue for each district, in case of requests from different districts than the current one
        for (int i = 0; i < allDistricts; i++) {
            newOrderList.add(new ArrayList<>());
            paymentList.add(new ArrayList<>());
        }
        // spree message queue
        spreeNewOrderList = new ArrayList<>();
        spreeAddItemList = new ArrayList<>();
        conn = createConnection(TxnServiceImpl.url, TxnServiceImpl.user, TxnServiceImpl.password);
    }

    public final int getAndResetIntervalRequests() {
        return intervalRequests.getAndSet(0);
    }

    public void setMergeSize(int merge) {
        this.MERGE_SIZE = merge;
    }

    public int getMergeSize() {
        return this.MERGE_SIZE;
    }

    public void shutdown() {
        running = false;
        try {
            conn.close();
        } catch (SQLException ex) {
            ex.printStackTrace(System.out);
            throw new RuntimeException("MergeThread failed to close connection", ex);
        }
    }

    // public synchronized void flushOrphanedDistricts(int newNumThreads) {
    //     if (this.threadId >= newNumThreads) {
    //         // this thread is no longer assigned to any district, flush all message lists
    //         for (int listIndex = 0; listIndex < newOrderList.size(); listIndex++) {
    //             if (newOrderList.get(listIndex).size() > 0) {
    //                 processMessageList(newOrderList.get(listIndex), this::mergeNewOrderTxn);
    //                 System.out.println("flush neworder inactive thread district " +  listIndex + " from mergeThread " + Thread.currentThread().getName()) ;
    //             }
    //         }
    //         for (List<PaymentMessage> thisDistrict : paymentList) {
    //             processMessageList(thisDistrict, this::mergePaymentTxn);
    //         }
    //         // // flush spree message queues
    //         // processMessageList(spreeNewOrderList, this::mergeSpreeNewOrderTxn);
    //         // processMessageList(spreeAddItemList, this::mergeSpreeAddItemTxn);
    //     } 
    //     else {
    //         // this thread is still assigned to some districts, flush only the orphaned districts
    //         for (int listIndex = 0; listIndex < newOrderList.size(); listIndex++) {
    //             int assignedThreadId = (listIndex % newNumThreads);
    //             if (assignedThreadId == this.threadId) {
    //                 // this district is still assigned to this thread, skip it
    //                 continue;
    //             }
    //             // flush neworder message lists
    //             if (newOrderList.get(listIndex).size() > 0) {
    //                 processMessageList(newOrderList.get(listIndex), this::mergeNewOrderTxn);
    //                 System.out.println("flush neworder orphaned district " +  listIndex + " from mergeThread " + Thread.currentThread().getName()) ;
    //             }
    //             if (paymentList.get(listIndex).size() > 0) {
    //                 // flush payment message lists
    //                 processMessageList(paymentList.get(listIndex), this::mergePaymentTxn);
    //                 System.out.println("flush payment orphaned district " +  listIndex + " from mergeThread " + Thread.currentThread().getName()) ;
    //             }             
    //         }
    //         // // flush spree message queues
    //         // processMessageList(spreeNewOrderList, this::mergeSpreeNewOrderTxn);
    //         // processMessageList(spreeAddItemList, this::mergeSpreeAddItemTxn);
    //     }
    // }

    private <T extends GrpcMessage> void processMessageList(List<T> messageList, Consumer<List<T>> mergeFunction) {
        if (messageList.isEmpty()) return;
        mergeFunction.accept(messageList);
        // Notify all messages in the message list
        for (T msg : messageList) {
            synchronized (msg) {
                msg.notify();
            }
        }
        // System.out.println("Flushing message list" +  " of size " + messageList.size() + " from thread " + this.threadId);
        messageList.clear();
    }

    public void run() {
        //System.out.println("Merge Thread running:  " + Thread.currentThread().getName());
        while (running) {
            GrpcMessage reqAndReply = null;

            try {
                reqAndReply = msgQueue.poll(1000, TimeUnit.MILLISECONDS); //timeout
                // queue poll timeout happens
                if (reqAndReply == null) {
                    // execute all remaining requests in each district's neworder message queue
                    for (List<NewOrderMessage> thisDistrict : newOrderList) {
                        processMessageList(thisDistrict, this::mergeNewOrderTxn);
                        
                        // if (thisDistrict != null && thisDistrict.size() > 0) {
                        //     // merge
                        //     System.out.println("poll timeout for this district's neworder message queue: " + thisDistrict.size() + " from ThreadForMerge: " + Thread.currentThread().getName()) ;
                        //     mergeNewOrderTxn(thisDistrict);
                        //     //for each reqAndReply in the list
                        //     for (int j = 0; j < thisDistrict.size(); j++) {
                        //         //notify
                        //         reqAndReply = thisDistrict.get(j);
                        //         synchronized(reqAndReply){
                        //             reqAndReply.notify();
                        //         }
                        //     }
                        //     // remove
                        //     thisDistrict.clear();
                        // }
                    }
                    // execute all remaining requests in each district's payment message queue
                    for (List<PaymentMessage> thisDistrict : paymentList) {
                        processMessageList(thisDistrict, this::mergePaymentTxn);
                        
                        // if (thisDistrict != null && thisDistrict.size() > 0) {
                        //     // merge
                        //     System.out.println("poll timeout for this district's payment message queue: " + thisDistrict.size() + " from ThreadForMerge: " + Thread.currentThread().getName()) ;
                        //     mergePaymentTxn(thisDistrict);
                        //     //for each reqAndReply in the list
                        //     for (int j = 0; j < thisDistrict.size(); j++) {
                        //         //notify
                        //         reqAndReply = thisDistrict.get(j);
                        //         synchronized(reqAndReply){
                        //             reqAndReply.notify();
                        //         }
                        //     }
                        //     // remove
                        //     thisDistrict.clear();
                        // }
                    }
                    // execute all remaining requests in spree neworder message queue
                    processMessageList(spreeNewOrderList, this::mergeSpreeNewOrderTxn);

                    // if (spreeNewOrderList != null && spreeNewOrderList.size() > 0) {
                    //     // merge
                    //     System.out.println("poll timeout for spreeNewOrderList: size " + spreeNewOrderList.size() + " from ThreadForMerge: " + Thread.currentThread().getName()) ;
                    //     mergeSpreeNewOrderTxn(spreeNewOrderList);
                    //     //for each reqAndReply in distList
                    //     for (int j = 0; j < spreeNewOrderList.size(); j++) {
                    //         //notify
                    //         reqAndReply = spreeNewOrderList.get(j);
                    //         synchronized(reqAndReply){
                    //             reqAndReply.notify();
                    //         }
                    //     }
                    //     // remove
                    //     spreeNewOrderList.clear();
                    // }

                    // execute all remaining requests in spree additem message queue
                    processMessageList(spreeAddItemList, this::mergeSpreeAddItemTxn);

                    // if (spreeAddItemList != null && spreeAddItemList.size() > 0) {
                    //     // merge
                    //     System.out.println("poll timeout for spreeAddItemList: size " + spreeAddItemList.size() + " from ThreadForMerge: " + Thread.currentThread().getName()) ;
                    //     mergeSpreeAddItemTxn(spreeAddItemList);
                    //     //for each reqAndReply in distList
                    //     for (int j = 0; j < spreeAddItemList.size(); j++) {
                    //         //notify
                    //         reqAndReply = spreeAddItemList.get(j);
                    //         synchronized(reqAndReply){
                    //             reqAndReply.notify();
                    //         }
                    //     }
                    //     // remove
                    //     spreeAddItemList.clear();
                    // }
                    continue;
                }

                //int w_id = reqAndReply.getRequest().getTerminalWarehouseID();
                // message poll succeeds, confirm the type of grpc message
                if (reqAndReply instanceof SpreeNewOrderMessage) {
                    SpreeNewOrderMessage spreenom = (SpreeNewOrderMessage) reqAndReply;
                    spreeNewOrderList.add(spreenom);
                    if (spreeNewOrderList.size() >= MERGE_SIZE) {
                        processMessageList(spreeNewOrderList, this::mergeSpreeNewOrderTxn);
                        // // merge
                        // mergeSpreeNewOrderTxn(spreeNewOrderList);
                        // //for each reqAndReply in thisDistrict
                        // for (int i = 0; i < spreeNewOrderList.size(); i++) {
                        //     //notify
                        //     reqAndReply = spreeNewOrderList.get(i);
                        //     synchronized(reqAndReply){
                        //         reqAndReply.notify();
                        //     }
                        // }
                        // // remove
                        // spreeNewOrderList.clear();
                    }
                } else if (reqAndReply instanceof SpreeAddItemMessage) {
                    SpreeAddItemMessage spreeaim = (SpreeAddItemMessage) reqAndReply;
                    spreeAddItemList.add(spreeaim);
                    if (spreeAddItemList.size() >= MERGE_SIZE) {
                        processMessageList(spreeAddItemList, this::mergeSpreeAddItemTxn);
                        // // merge
                        // mergeSpreeAddItemTxn(spreeAddItemList);
                        // //for each reqAndReply in thisDistrict
                        // for (int i = 0; i < spreeAddItemList.size(); i++) {
                        //     //notify
                        //     reqAndReply = spreeAddItemList.get(i);
                        //     synchronized(reqAndReply){
                        //         reqAndReply.notify();
                        //     }
                        // }
                        // // remove
                        // spreeAddItemList.clear();
                    }
                } else if (reqAndReply instanceof NewOrderMessage) {
                    NewOrderMessage nom = (NewOrderMessage) reqAndReply;
                    int w_id = nom.getRequest().getTerminalWarehouseID();
                    int district_id = nom.getRequest().getDistrictID();
                    int listIndex = (w_id - 1) * TPCCConfig.configDistPerWhse + (district_id - 1);
                    //int listIndex = (district_id - 1) % this.distPerThread;
                    newOrderList.get(listIndex).add(nom);
                    if (newOrderList.get(listIndex).size() >= MERGE_SIZE) {
                        processMessageList(newOrderList.get(listIndex), this::mergeNewOrderTxn);
                        // // merge
                        // mergeNewOrderTxn(newOrderList.get(listIndex));
                        // //for each reqAndReply in thisDistrict
                        // for (int i = 0; i < newOrderList.get(listIndex).size(); i++) { 
                        //     //notify
                        //     reqAndReply = newOrderList.get(listIndex).get(i);
                        //     synchronized(reqAndReply){
                        //         reqAndReply.notify();
                        //     }
                        // }
                        // // update intervalRequests
                        // intervalRequests.addAndGet(newOrderList.get(listIndex).size());
                        // // remove
                        // newOrderList.get(listIndex).clear();
                    }
                } else if (reqAndReply instanceof PaymentMessage) {
                    PaymentMessage paym = (PaymentMessage) reqAndReply;
                    int w_id = paym.getRequest().getTerminalWarehouseID();
                    int district_id = paym.getRequest().getDistrictID();
                    int listIndex = (w_id - 1) * TPCCConfig.configDistPerWhse + (district_id - 1);
                    //int listIndex = (district_id - 1) % this.distPerThread;
                    paymentList.get(listIndex).add(paym);
                    if (paymentList.get(listIndex).size() >= MERGE_SIZE) {
                        processMessageList(paymentList.get(listIndex), this::mergePaymentTxn);
                        // // merge
                        // mergePaymentTxn(paymentList.get(listIndex));
                        // //for each reqAndReply in thisDistrict
                        // for (int i = 0; i < paymentList.get(listIndex).size(); i++) {
                        //     //notify
                        //     reqAndReply = paymentList.get(listIndex).get(i);
                        //     synchronized(reqAndReply){
                        //         reqAndReply.notify();
                        //     }
                        // }
                        // // update intervalRequests
                        // intervalRequests.addAndGet(paymentList.get(listIndex).size());
                        // // remove
                        // paymentList.get(listIndex).clear();
                    }
                } else if (reqAndReply instanceof DeliveryMessage) {
                    DeliveryMessage delm = (DeliveryMessage) reqAndReply;
                    // directly execute each delivery request, merge internally
                    executeDeliveryTxn(delm);
                    // notify for this delivery request
                    synchronized(delm){
                        delm.notify();
                    }
                    // increment intervalRequests
                    intervalRequests.incrementAndGet();
                } else if (reqAndReply instanceof OrderStatusMessage) {
                    OrderStatusMessage osm = (OrderStatusMessage) reqAndReply;
                    // directly execute each order-status request, merge internally
                    executeOrderStatusTxn(osm);
                    // notify for this order-status request
                    synchronized(osm){
                        osm.notify();
                    }
                    // increment intervalRequests
                    intervalRequests.incrementAndGet();
                } else if (reqAndReply instanceof StockLevelMessage) {
                    StockLevelMessage slm = (StockLevelMessage) reqAndReply;
                    // directly execute each stock-level request, merge internally
                    executeStockLevelTxn(slm);
                    // notify for this stock-level request
                    synchronized(slm){
                        slm.notify();
                    }
                    // increment intervalRequests
                    intervalRequests.incrementAndGet();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("Thread interrupted: {}", Thread.currentThread().getName());
            }

        }
    }
    
    // execute merged spree NewOrder transactions
    private void mergeSpreeNewOrderTxn(List<SpreeNewOrderMessage> messages) {
        SpreeNewOrderReply response = null;
        Status status = null;
        SpreeNewOrderRequest[] reqArray = new SpreeNewOrderRequest[messages.size()];
        for (int i = 0; i < messages.size(); i++) {
            SpreeNewOrderRequest request = messages.get(i).getRequest();
            reqArray[i] = request;
        }
        SpreeNewOrderMT spreeNewOrder = new SpreeNewOrderMT(messages.size());
        
        try {
            try {
                response = spreeNewOrder.spreeNewOrderMergeTransaction(reqArray, conn);
                conn.commit();

            } catch (SQLException ex) {
                conn.rollback();
                if (isRetryable(ex)) {
                    LOG.debug(String.format("merge: Retryable SQLException occurred during <mergeNewOrderTxn>... sql state [%s], error code [%d].", ex.getSQLState(), ex.getErrorCode()), ex);
                    status = Status.INTERNAL.withDescription("Retryable SQLException:").augmentDescription(ex.getMessage());
                } else {
                    LOG.warn(String.format("merge: SQLException occurred during <mergeNewOrderTxn> and will not be retried... sql state [%s], error code [%d].", ex.getSQLState(), ex.getErrorCode()), ex);
                    status = Status.INTERNAL.withDescription("Unretryable SQLException:").augmentDescription(ex.getMessage());
                }

            } catch (UserAbortException ex) {
                //System.out.println("UserAbortException: catch " +  ex);
                conn.rollback();
                //System.out.println("UserAbortException: rollback ");
                status = Status.INTERNAL.withDescription("UserAbortException");

            } catch (RuntimeException ex) {
                // transaction execution calls RuntimeException
                conn.rollback();
                status = Status.INTERNAL.withDescription("Transaction RuntimeException:").augmentDescription(ex.getMessage());
            }
        } catch (SQLException ex) {
            LOG.warn(String.format("Unexpected SQLException in [%s] when executing [%s].", this, new Throwable().getStackTrace()[0].getMethodName()), ex);
            //ex.printStackTrace(System.out);
            status = Status.INTERNAL.withDescription("Unexpected SQLException:").augmentDescription(ex.getMessage());
        }
        
        if (response != null) {
            // execution succeeds
            for (int i = 0; i < messages.size(); i++) {
                messages.get(i).setReply(response);
            }
        }

        if (status != null) {
            // exception thrown, execution fails
            for (int i = 0; i < messages.size(); i++) {
                messages.get(i).setStatus(status);
            }
        }
    }

    // execute merged spree AddItem transactions
    private void mergeSpreeAddItemTxn(List<SpreeAddItemMessage> messages) {
        SpreeAddItemReply response = null;
        Status status = null;
        SpreeAddItemRequest[] reqArray = new SpreeAddItemRequest[messages.size()];
        for (int i = 0; i < messages.size(); i++) {
            SpreeAddItemRequest request = messages.get(i).getRequest();
            reqArray[i] = request;
        }
        SpreeAddItemMT spreeAddItem = new SpreeAddItemMT(messages.size());
        
        try {
            try {
                response = spreeAddItem.spreeAddItemMergeTransaction(reqArray, conn);
                conn.commit();

            } catch (SQLException ex) {
                conn.rollback();
                if (isRetryable(ex)) {
                    LOG.debug(String.format("merge: Retryable SQLException occurred during <mergeNewOrderTxn>... sql state [%s], error code [%d].", ex.getSQLState(), ex.getErrorCode()), ex);
                    status = Status.INTERNAL.withDescription("Retryable SQLException:").augmentDescription(ex.getMessage());
                } else {
                    LOG.warn(String.format("merge: SQLException occurred during <mergeNewOrderTxn> and will not be retried... sql state [%s], error code [%d].", ex.getSQLState(), ex.getErrorCode()), ex);
                    status = Status.INTERNAL.withDescription("Unretryable SQLException:").augmentDescription(ex.getMessage());
                }

            } catch (UserAbortException ex) {
                //System.out.println("UserAbortException: catch " +  ex);
                conn.rollback();
                //System.out.println("UserAbortException: rollback ");
                status = Status.INTERNAL.withDescription("UserAbortException");

            } catch (RuntimeException ex) {
                // transaction execution calls RuntimeException
                conn.rollback();
                status = Status.INTERNAL.withDescription("Transaction RuntimeException:").augmentDescription(ex.getMessage());
            }
        } catch (SQLException ex) {
            LOG.warn(String.format("Unexpected SQLException in [%s] when executing [%s].", this, new Throwable().getStackTrace()[0].getMethodName()), ex);
            //ex.printStackTrace(System.out);
            status = Status.INTERNAL.withDescription("Unexpected SQLException:").augmentDescription(ex.getMessage());
        }
        
        if (response != null) {
            // execution succeeds
            // TODO: distribute different responses to different requests
            for (int i = 0; i < messages.size(); i++) {
                messages.get(i).setReply(response);
            }
        }

        if (status != null) {
            // exception thrown, execution fails
            for (int i = 0; i < messages.size(); i++) {
                messages.get(i).setStatus(status);
            }
        }
    }

    // execute merged NewOrder transactions
    private void mergeNewOrderTxn(List<NewOrderMessage> messages) {
        NewOrderReply response = null;
        Status status = null;
        NewOrderRequest[] reqArray = new NewOrderRequest[messages.size()];
        for (int i = 0; i < messages.size(); i++) {
            NewOrderRequest request = messages.get(i).getRequest();
            reqArray[i] = request;
        }
        NewOrderMT new_order = new NewOrderMT(messages.size());
        
        try {
            try {
                response = new_order.newOrderMergeTransaction(reqArray, conn);
                conn.commit();

            } catch (SQLException ex) {
                conn.rollback();
                if (isRetryable(ex)) {
                    LOG.debug(String.format("merge: Retryable SQLException occurred during <mergeNewOrderTxn>... sql state [%s], error code [%d].", ex.getSQLState(), ex.getErrorCode()), ex);
                    status = Status.INTERNAL.withDescription("Retryable SQLException:").augmentDescription(ex.getMessage());
                } else {
                    LOG.warn(String.format("merge: SQLException occurred during <mergeNewOrderTxn> and will not be retried... sql state [%s], error code [%d].", ex.getSQLState(), ex.getErrorCode()), ex);
                    status = Status.INTERNAL.withDescription("Unretryable SQLException:").augmentDescription(ex.getMessage());
                }

            } catch (UserAbortException ex) {
                //System.out.println("UserAbortException: catch " +  ex);
                conn.rollback();
                //System.out.println("UserAbortException: rollback ");
                status = Status.INTERNAL.withDescription("UserAbortException");

            } catch (RuntimeException ex) {
                // transaction execution calls RuntimeException
                conn.rollback();
                status = Status.INTERNAL.withDescription("Transaction RuntimeException:").augmentDescription(ex.getMessage());
            }
        } catch (SQLException ex) {
            LOG.warn(String.format("Unexpected SQLException in [%s] when executing [%s].", this, new Throwable().getStackTrace()[0].getMethodName()), ex);
            //ex.printStackTrace(System.out);
            status = Status.INTERNAL.withDescription("Unexpected SQLException:").augmentDescription(ex.getMessage());
        }
        
        if (response != null) {
            // execution succeeds
            // TODO: distribute different responses to transactions
            for (int i = 0; i < messages.size(); i++) {
                messages.get(i).setReply(response);
            }
        }

        if (status != null) {
            // execution fails
            for (int i = 0; i < messages.size(); i++) {
                messages.get(i).setStatus(status);
                // if (status.getDescription() == "UserAbortException") {
                //     messages.get(i).setRun(1);
                // }
            }
        }

    }

    // execute merged Payment transactions
    public void mergePaymentTxn(List<PaymentMessage> messages) {
        PaymentReply response = null;
        Status status = null;
        PaymentRequest[] reqArray = new PaymentRequest[messages.size()];
        for (int i = 0; i < messages.size(); i++) {
            PaymentRequest request = messages.get(i).getRequest();
            reqArray[i] = request;
        }
        PaymentMT payment = new PaymentMT(messages.size());
        
        try {
            try {
                response = payment.paymentMergeTransaction(reqArray, conn);
                conn.commit();

            } catch (SQLException ex) {
                conn.rollback();
                if (isRetryable(ex)) {
                    LOG.debug(String.format("merge: Retryable SQLException occurred during <mergePaymentTxn>... sql state [%s], error code [%d].", ex.getSQLState(), ex.getErrorCode()), ex);
                    status = Status.INTERNAL.withDescription("Retryable SQLException:").augmentDescription(ex.getMessage());
                } else {
                    LOG.warn(String.format("merge: SQLException occurred during <mergePaymentTxn> and will not be retried... sql state [%s], error code [%d].", ex.getSQLState(), ex.getErrorCode()), ex);
                    status = Status.INTERNAL.withDescription("Unretryable SQLException:").augmentDescription(ex.getMessage());
                }

            } catch (UserAbortException ex) {
                conn.rollback();
                //System.out.println("UserAbortException: rollback ");
                status = Status.INTERNAL.withDescription("UserAbortException");

            } catch (RuntimeException ex) {
                // transaction execution calls RuntimeException
                conn.rollback();
                status = Status.INTERNAL.withDescription("Transaction RuntimeException:").augmentDescription(ex.getMessage());
            }
        } catch (SQLException ex) {
            LOG.warn(String.format("Unexpected SQLException in [%s] when executing [%s].", this, new Throwable().getStackTrace()[0].getMethodName()), ex);
            //ex.printStackTrace(System.out);
            status = Status.INTERNAL.withDescription("Unexpected SQLException:").augmentDescription(ex.getMessage());
        }

        if (response != null) {
            // execution succeeds
            // TODO: distribute different responses to transactions
            for (int i = 0; i < messages.size(); i++) {
                messages.get(i).setReply(response);
            }
        }

        if (status != null) {
            // execution fails
            for (int i = 0; i < messages.size(); i++) {
                messages.get(i).setStatus(status);
                // if (status.getDescription() == "UserAbortException") {
                //     messages.get(i).setRun(1);
                // }
            }
        }

    }

    // execute merged Delivery transactions
    public void executeDeliveryTxn(DeliveryMessage delmsg) {
        DeliveryReply response = null;
        Status status = null;
        DeliveryRequest request = delmsg.getRequest();
        
        DeliveryMT delivery = new DeliveryMT(1);
        
        try {
            try {
                response = delivery.deliveryInternalMergeTransaction(request, conn);
                conn.commit();

            } catch (SQLException ex) {
                conn.rollback();
                if (isRetryable(ex)) {
                    LOG.debug(String.format("merge: Retryable SQLException occurred during <executeDeliveryTxn>... sql state [%s], error code [%d].", ex.getSQLState(), ex.getErrorCode()), ex);
                    status = Status.INTERNAL.withDescription("Retryable SQLException:").augmentDescription(ex.getMessage());
                } else {
                    LOG.warn(String.format("merge: SQLException occurred during <executeDeliveryTxn> and will not be retried... sql state [%s], error code [%d].", ex.getSQLState(), ex.getErrorCode()), ex);
                    status = Status.INTERNAL.withDescription("Unretryable SQLException:").augmentDescription(ex.getMessage());
                }

            } catch (UserAbortException ex) {
                conn.rollback();
                //System.out.println("UserAbortException: rollback ");
                status = Status.INTERNAL.withDescription("UserAbortException");

            } catch (RuntimeException ex) {
                // transaction execution calls RuntimeException
                conn.rollback();
                status = Status.INTERNAL.withDescription("Transaction RuntimeException:").augmentDescription(ex.getMessage());
            }
        } catch (SQLException ex) {
            LOG.warn(String.format("Unexpected SQLException in [%s] when executing [%s].", this, new Throwable().getStackTrace()[0].getMethodName()), ex);
            //ex.printStackTrace(System.out);
            status = Status.INTERNAL.withDescription("Unexpected SQLException:").augmentDescription(ex.getMessage());
        }

        if (response != null) {
            // execution succeeds
            delmsg.setReply(response);
        }

        if (status != null) {
            // execution fails
            delmsg.setStatus(status);
        }

    }

    // execute the OrderStatus transaction
    public void executeOrderStatusTxn(OrderStatusMessage osmsg) {
        OrderStatusReply response = null;
        Status status = null;
        OrderStatusRequest request = osmsg.getRequest();
        
        OrderStatus orderStatus = new OrderStatus();
        
        try {
            try {
                response = orderStatus.orderStatusTransaction(request, conn);
                conn.commit();

            } catch (SQLException ex) {
                conn.rollback();
                if (isRetryable(ex)) {
                    LOG.debug(String.format("merge: Retryable SQLException occurred during <executeOrderStatusTxn>... sql state [%s], error code [%d].", ex.getSQLState(), ex.getErrorCode()), ex);
                    status = Status.INTERNAL.withDescription("Retryable SQLException:").augmentDescription(ex.getMessage());
                } else {
                    LOG.warn(String.format("merge: SQLException occurred during <executeOrderStatusTxn> and will not be retried... sql state [%s], error code [%d].", ex.getSQLState(), ex.getErrorCode()), ex);
                    status = Status.INTERNAL.withDescription("Unretryable SQLException:").augmentDescription(ex.getMessage());
                }

            } catch (UserAbortException ex) {
                conn.rollback();
                //System.out.println("UserAbortException: rollback ");
                status = Status.INTERNAL.withDescription("UserAbortException");

            } catch (RuntimeException ex) {
                // transaction execution calls RuntimeException
                conn.rollback();
                status = Status.INTERNAL.withDescription("Transaction RuntimeException:").augmentDescription(ex.getMessage());
            }
        } catch (SQLException ex) {
            LOG.warn(String.format("Unexpected SQLException in [%s] when executing [%s].", this, new Throwable().getStackTrace()[0].getMethodName()), ex);
            //ex.printStackTrace(System.out);
            status = Status.INTERNAL.withDescription("Unexpected SQLException:").augmentDescription(ex.getMessage());
        }

        if (response != null) {
            // execution succeeds
            osmsg.setReply(response);
        }

        if (status != null) {
            // execution fails
            osmsg.setStatus(status);
        }

    }

    // execute the StockLevel transaction
    public void executeStockLevelTxn(StockLevelMessage slmsg) {
        StockLevelReply response = null;
        Status status = null;
        StockLevelRequest request = slmsg.getRequest();
        
        StockLevel stockLevel = new StockLevel();
        
        try {
            try {
                response = stockLevel.stockLevelTransaction(request, conn);
                conn.commit();

            } catch (SQLException ex) {
                conn.rollback();
                if (isRetryable(ex)) {
                    LOG.debug(String.format("merge: Retryable SQLException occurred during <executeStockLevelTxn>... sql state [%s], error code [%d].", ex.getSQLState(), ex.getErrorCode()), ex);
                    status = Status.INTERNAL.withDescription("Retryable SQLException:").augmentDescription(ex.getMessage());
                } else {
                    LOG.warn(String.format("merge: SQLException occurred during <executeStockLevelTxn> and will not be retried... sql state [%s], error code [%d].", ex.getSQLState(), ex.getErrorCode()), ex);
                    status = Status.INTERNAL.withDescription("Unretryable SQLException:").augmentDescription(ex.getMessage());
                }

            } catch (UserAbortException ex) {
                conn.rollback();
                //System.out.println("UserAbortException: rollback ");
                status = Status.INTERNAL.withDescription("UserAbortException");

            } catch (RuntimeException ex) {
                // transaction execution calls RuntimeException
                conn.rollback();
                status = Status.INTERNAL.withDescription("Transaction RuntimeException:").augmentDescription(ex.getMessage());
            }
        } catch (SQLException ex) {
            LOG.warn(String.format("Unexpected SQLException in [%s] when executing [%s].", this, new Throwable().getStackTrace()[0].getMethodName()), ex);
            //ex.printStackTrace(System.out);
            status = Status.INTERNAL.withDescription("Unexpected SQLException:").augmentDescription(ex.getMessage());
        }

        if (response != null) {
            // execution succeeds
            slmsg.setReply(response);
        }

        if (status != null) {
            // execution fails
            slmsg.setStatus(status);
        }

    }

    private Connection createConnection(String url, String user, String password) {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, password);
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);   
        } catch (SQLException ex) {
            ex.printStackTrace(System.out);
            throw new RuntimeException("MergeThread failed to connect to database", ex);
        }
        
        return conn;
    }

    private boolean isRetryable(SQLException ex) {
        
        String sqlState = ex.getSQLState();
        int errorCode = ex.getErrorCode();
        
        LOG.debug("sql state [{}] and error code [{}]", sqlState, errorCode);
        
        if (sqlState == null) {
            return false;
        }
        // ------------------
        // MYSQL: https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-error-sqlstates.html
        // ------------------
        if (errorCode == 1213 && sqlState.equals("40001")) {
            // MySQL ER_LOCK_DEADLOCK
            return true;
        } else if (errorCode == 1205 && sqlState.equals("40001")) {
            // MySQL ER_LOCK_WAIT_TIMEOUT
            return true;
        }
        
        // ------------------
        // POSTGRES: https://www.postgresql.org/docs/current/errcodes-appendix.html
        // ------------------
        // Postgres serialization_failure
        return errorCode == 0 && sqlState.equals("40001");
    }

}