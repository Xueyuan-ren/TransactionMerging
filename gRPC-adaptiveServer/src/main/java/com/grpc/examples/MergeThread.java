package com.grpc.examples;

import com.grpc.examples.tpcc.*;
import com.grpc.examples.spree.*;
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

    private <T extends GrpcMessage> void processMessageList(List<T> messageList, Consumer<List<T>> mergeFunction) {
        if (messageList.isEmpty()) return;
        mergeFunction.accept(messageList);
        // Notify all messages in the message list
        for (T msg : messageList) {
            synchronized (msg) {
                msg.notify();
            }
        }
        
        messageList.clear();
    }

    public void run() {

        while (running) {
            GrpcMessage reqAndReply = null;

            try {
                reqAndReply = msgQueue.poll(1000, TimeUnit.MILLISECONDS); //timeout
                // queue poll timeout happens
                if (reqAndReply == null) {

                    // execute all remaining requests in each district's neworder message queue
                    for (List<NewOrderMessage> thisDistrict : newOrderList) {
                        processMessageList(thisDistrict, this::mergeNewOrderTxn);
                    }

                    // execute all remaining requests in each district's payment message queue
                    for (List<PaymentMessage> thisDistrict : paymentList) {
                        processMessageList(thisDistrict, this::mergePaymentTxn);
                    }

                    // execute all remaining requests in spree neworder message queue
                    processMessageList(spreeNewOrderList, this::mergeSpreeNewOrderTxn);

                    // execute all remaining requests in spree additem message queue
                    processMessageList(spreeAddItemList, this::mergeSpreeAddItemTxn);

                    continue;
                }

                // message poll succeeds, confirm the type of grpc message
                if (reqAndReply instanceof SpreeNewOrderMessage) {
                    SpreeNewOrderMessage spreenom = (SpreeNewOrderMessage) reqAndReply;
                    spreeNewOrderList.add(spreenom);
                    if (spreeNewOrderList.size() >= MERGE_SIZE) {
                        processMessageList(spreeNewOrderList, this::mergeSpreeNewOrderTxn);                       
                    }

                } else if (reqAndReply instanceof SpreeAddItemMessage) {
                    SpreeAddItemMessage spreeaim = (SpreeAddItemMessage) reqAndReply;
                    spreeAddItemList.add(spreeaim);
                    if (spreeAddItemList.size() >= MERGE_SIZE) {
                        processMessageList(spreeAddItemList, this::mergeSpreeAddItemTxn);                     
                    }

                } else if (reqAndReply instanceof NewOrderMessage) {
                    NewOrderMessage nom = (NewOrderMessage) reqAndReply;
                    // merging for NewOrderMessage
                    int w_id = nom.getRequest().getTerminalWarehouseID();
                    int district_id = nom.getRequest().getDistrictID();
                    int listIndex = (w_id - 1) * TPCCConfig.configDistPerWhse + (district_id - 1);
                    newOrderList.get(listIndex).add(nom);
                    if (newOrderList.get(listIndex).size() >= MERGE_SIZE) {
                        processMessageList(newOrderList.get(listIndex), this::mergeNewOrderTxn);
                    }

                    // // directly execute each new order request
                    // executeNewOrderTxn(nom);
                    // // notify for this new order request
                    // synchronized(nom){
                    //     nom.notify();
                    // }

                } else if (reqAndReply instanceof PaymentMessage) {
                    PaymentMessage paym = (PaymentMessage) reqAndReply;
                    // merging for PaymentMessage
                    int w_id = paym.getRequest().getTerminalWarehouseID();
                    int district_id = paym.getRequest().getDistrictID();
                    int listIndex = (w_id - 1) * TPCCConfig.configDistPerWhse + (district_id - 1);
                    paymentList.get(listIndex).add(paym);
                    if (paymentList.get(listIndex).size() >= MERGE_SIZE) {
                        processMessageList(paymentList.get(listIndex), this::mergePaymentTxn);                      
                    }

                    // // directly execute each payment request
                    // executePaymentTxn(paym);
                    // // notify for this payment request
                    // synchronized(paym){
                    //     paym.notify();
                    // }

                } else if (reqAndReply instanceof DeliveryMessage) {
                    DeliveryMessage delm = (DeliveryMessage) reqAndReply;
                    // merging internally for DeliveryMessage
                    executeDeliveryTxn(delm);
                    // notify for this delivery request
                    synchronized(delm){
                        delm.notify();
                    }
                    
                    // // directly execute each original delivery request
                    // executeOriginalDeliveryTxn(delm);
                    // // notify for this delivery request
                    // synchronized(delm){
                    //     delm.notify();
                    // }

                } else if (reqAndReply instanceof OrderStatusMessage) {
                    OrderStatusMessage osm = (OrderStatusMessage) reqAndReply;
                    // directly execute each order-status request
                    executeOrderStatusTxn(osm);
                    // notify for this order-status request
                    synchronized(osm){
                        osm.notify();
                    }

                } else if (reqAndReply instanceof StockLevelMessage) {
                    StockLevelMessage slm = (StockLevelMessage) reqAndReply;
                    // directly execute each stock-level request
                    executeStockLevelTxn(slm);
                    // notify for this stock-level request
                    synchronized(slm){
                        slm.notify();
                    }
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
                    LOG.debug(String.format("merge: Retryable SQLException occurred during <mergeSpreeNewOrderTxn>... sql state [%s], error code [%d].", ex.getSQLState(), ex.getErrorCode()), ex);
                    status = Status.INTERNAL.withDescription("Retryable SQLException:").augmentDescription(ex.getMessage());
                } else {
                    LOG.warn(String.format("merge: SQLException occurred during <mergeSpreeNewOrderTxn> and will not be retried... sql state [%s], error code [%d].", ex.getSQLState(), ex.getErrorCode()), ex);
                    status = Status.INTERNAL.withDescription("Unretryable SQLException:").augmentDescription(ex.getMessage());
                }

            } catch (UserAbortException ex) {
                conn.rollback();
                status = Status.INTERNAL.withDescription("UserAbortException");

            } catch (RuntimeException ex) {
                // transaction execution calls RuntimeException
                conn.rollback();
                status = Status.INTERNAL.withDescription("Transaction RuntimeException:").augmentDescription(ex.getMessage());
            }
        } catch (SQLException ex) {
            LOG.warn(String.format("Unexpected SQLException in [%s] when executing [%s].", this, new Throwable().getStackTrace()[0].getMethodName()), ex);
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
                    LOG.debug(String.format("merge: Retryable SQLException occurred during <mergeSpreeAddItemTxn>... sql state [%s], error code [%d].", ex.getSQLState(), ex.getErrorCode()), ex);
                    status = Status.INTERNAL.withDescription("Retryable SQLException:").augmentDescription(ex.getMessage());
                } else {
                    LOG.warn(String.format("merge: SQLException occurred during <mergeSpreeAddItemTxn> and will not be retried... sql state [%s], error code [%d].", ex.getSQLState(), ex.getErrorCode()), ex);
                    status = Status.INTERNAL.withDescription("Unretryable SQLException:").augmentDescription(ex.getMessage());
                }

            } catch (UserAbortException ex) {
                conn.rollback();
                status = Status.INTERNAL.withDescription("UserAbortException");

            } catch (RuntimeException ex) {
                // transaction execution calls RuntimeException
                conn.rollback();
                status = Status.INTERNAL.withDescription("Transaction RuntimeException:").augmentDescription(ex.getMessage());
            }
        } catch (SQLException ex) {
            LOG.warn(String.format("Unexpected SQLException in [%s] when executing [%s].", this, new Throwable().getStackTrace()[0].getMethodName()), ex);
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
    private void mergeNewOrderTxn(List<NewOrderMessage> neworderMsgs) {
        NewOrderReply response = null;
        Status status = null;
        NewOrderRequest[] reqArray = new NewOrderRequest[neworderMsgs.size()];
        for (int i = 0; i < neworderMsgs.size(); i++) {
            NewOrderRequest request = neworderMsgs.get(i).getRequest();
            reqArray[i] = request;
        }
        NewOrderMT newOrderMT = new NewOrderMT(neworderMsgs.size());
        
        try {
            try {
                response = newOrderMT.newOrderMergeTransaction(reqArray, conn);
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
                conn.rollback();
                status = Status.INTERNAL.withDescription("UserAbortException");

            } catch (RuntimeException ex) {
                // transaction execution calls RuntimeException
                conn.rollback();
                status = Status.INTERNAL.withDescription("Transaction RuntimeException:").augmentDescription(ex.getMessage());
            }
        } catch (SQLException ex) {
            LOG.warn(String.format("Unexpected SQLException in [%s] when executing [%s].", this, new Throwable().getStackTrace()[0].getMethodName()), ex);
            status = Status.INTERNAL.withDescription("Unexpected SQLException:").augmentDescription(ex.getMessage());
        }
        
        if (response != null) {
            // execution succeeds
            // TODO: distribute different responses to transactions
            for (int i = 0; i < neworderMsgs.size(); i++) {
                neworderMsgs.get(i).setReply(response);
            }
        }

        if (status != null) {
            // execution fails
            for (int i = 0; i < neworderMsgs.size(); i++) {
                neworderMsgs.get(i).setStatus(status);
            }
        }

    }

    // execute the single NewOrder transaction
    public void executeNewOrderTxn(NewOrderMessage neworderMsg) {
        NewOrderReply response = null;
        Status status = null;
        NewOrderRequest request = neworderMsg.getRequest();
        
        NewOrder newOrder = new NewOrder();
        
        try {
            try {
                response = newOrder.newOrderTransaction(request, conn);
                conn.commit();

            } catch (SQLException ex) {
                conn.rollback();
                if (isRetryable(ex)) {
                    LOG.debug(String.format("merge: Retryable SQLException occurred during <executeNewOrderTxn>... sql state [%s], error code [%d].", ex.getSQLState(), ex.getErrorCode()), ex);
                    status = Status.INTERNAL.withDescription("Retryable SQLException:").augmentDescription(ex.getMessage());
                } else {
                    LOG.warn(String.format("merge: SQLException occurred during <executeNewOrderTxn> and will not be retried... sql state [%s], error code [%d].", ex.getSQLState(), ex.getErrorCode()), ex);
                    status = Status.INTERNAL.withDescription("Unretryable SQLException:").augmentDescription(ex.getMessage());
                }

            } catch (UserAbortException ex) {
                conn.rollback();
                status = Status.INTERNAL.withDescription("UserAbortException");

            } catch (RuntimeException ex) {
                // transaction execution calls RuntimeException
                conn.rollback();
                status = Status.INTERNAL.withDescription("Transaction RuntimeException:").augmentDescription(ex.getMessage());
            }
        } catch (SQLException ex) {
            LOG.warn(String.format("Unexpected SQLException in [%s] when executing [%s].", this, new Throwable().getStackTrace()[0].getMethodName()), ex);
            status = Status.INTERNAL.withDescription("Unexpected SQLException:").augmentDescription(ex.getMessage());
        }

        if (response != null) {
            // execution succeeds
            neworderMsg.setReply(response);
        }

        if (status != null) {
            // execution fails
            neworderMsg.setStatus(status);
        }

    }

    // execute merged Payment transactions
    public void mergePaymentTxn(List<PaymentMessage> paymentMsgs) {
        PaymentReply response = null;
        Status status = null;
        PaymentRequest[] reqArray = new PaymentRequest[paymentMsgs.size()];
        for (int i = 0; i < paymentMsgs.size(); i++) {
            PaymentRequest request = paymentMsgs.get(i).getRequest();
            reqArray[i] = request;
        }
        PaymentMT paymentMT = new PaymentMT(paymentMsgs.size());
        
        try {
            try {
                response = paymentMT.paymentMergeTransaction(reqArray, conn);
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
                status = Status.INTERNAL.withDescription("UserAbortException");

            } catch (RuntimeException ex) {
                // transaction execution calls RuntimeException
                conn.rollback();
                status = Status.INTERNAL.withDescription("Transaction RuntimeException:").augmentDescription(ex.getMessage());
            }
        } catch (SQLException ex) {
            LOG.warn(String.format("Unexpected SQLException in [%s] when executing [%s].", this, new Throwable().getStackTrace()[0].getMethodName()), ex);
            status = Status.INTERNAL.withDescription("Unexpected SQLException:").augmentDescription(ex.getMessage());
        }

        if (response != null) {
            // execution succeeds
            // TODO: distribute different responses to transactions
            for (int i = 0; i < paymentMsgs.size(); i++) {
                paymentMsgs.get(i).setReply(response);
            }
        }

        if (status != null) {
            // execution fails
            for (int i = 0; i < paymentMsgs.size(); i++) {
                paymentMsgs.get(i).setStatus(status);
            }
        }

    }

    // execute the single Payment transaction
    public void executePaymentTxn(PaymentMessage paymentMsg) {
        PaymentReply response = null;
        Status status = null;
        PaymentRequest request = paymentMsg.getRequest();
        
        Payment payment = new Payment();
        
        try {
            try {
                response = payment.paymentTransaction(request, conn);
                conn.commit();

            } catch (SQLException ex) {
                conn.rollback();
                if (isRetryable(ex)) {
                    LOG.debug(String.format("merge: Retryable SQLException occurred during <executePaymentTxn>... sql state [%s], error code [%d].", ex.getSQLState(), ex.getErrorCode()), ex);
                    status = Status.INTERNAL.withDescription("Retryable SQLException:").augmentDescription(ex.getMessage());
                } else {
                    LOG.warn(String.format("merge: SQLException occurred during <executePaymentTxn> and will not be retried... sql state [%s], error code [%d].", ex.getSQLState(), ex.getErrorCode()), ex);
                    status = Status.INTERNAL.withDescription("Unretryable SQLException:").augmentDescription(ex.getMessage());
                }

            } catch (UserAbortException ex) {
                conn.rollback();
                status = Status.INTERNAL.withDescription("UserAbortException");

            } catch (RuntimeException ex) {
                // transaction execution calls RuntimeException
                conn.rollback();
                status = Status.INTERNAL.withDescription("Transaction RuntimeException:").augmentDescription(ex.getMessage());
            }
        } catch (SQLException ex) {
            LOG.warn(String.format("Unexpected SQLException in [%s] when executing [%s].", this, new Throwable().getStackTrace()[0].getMethodName()), ex);
            status = Status.INTERNAL.withDescription("Unexpected SQLException:").augmentDescription(ex.getMessage());
        }

        if (response != null) {
            // execution succeeds
            paymentMsg.setReply(response);
        }

        if (status != null) {
            // execution fails
            paymentMsg.setStatus(status);
        }

    }

    // execute merged Delivery transactions
    public void executeDeliveryTxn(DeliveryMessage delMsg) {
        DeliveryReply response = null;
        Status status = null;
        DeliveryRequest request = delMsg.getRequest();
        
        DeliveryMT deliveryMT = new DeliveryMT(1);
        
        try {
            try {
                // change the isolation level to SERIALIZABLE for the connection of delivery transactions
                conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

                response = deliveryMT.deliveryInternalMergeTransaction(request, conn);
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
                status = Status.INTERNAL.withDescription("UserAbortException");

            } catch (RuntimeException ex) {
                // transaction execution calls RuntimeException
                conn.rollback();
                status = Status.INTERNAL.withDescription("Transaction RuntimeException:").augmentDescription(ex.getMessage());
            
            } finally {
                // change the isolation level back to default REPEATABLE_READ
                conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            }
        } catch (SQLException ex) {
            LOG.warn(String.format("Unexpected SQLException in [%s] when executing [%s].", this, new Throwable().getStackTrace()[0].getMethodName()), ex);
            status = Status.INTERNAL.withDescription("Unexpected SQLException:").augmentDescription(ex.getMessage());
        }

        if (response != null) {
            // execution succeeds
            delMsg.setReply(response);
        }

        if (status != null) {
            // execution fails
            delMsg.setStatus(status);
        }

    }

    // execute single original Delivery transactions
    public void executeOriginalDeliveryTxn(DeliveryMessage delMsg) {
        DeliveryReply response = null;
        Status status = null;
        DeliveryRequest request = delMsg.getRequest();
        
        Delivery delivery = new Delivery();
        
        try {
            try {
                // change the isolation level to SERIALIZABLE for the connection of delivery transactions
                conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

                response = delivery.deliveryTransaction(request, conn);
                conn.commit();

            } catch (SQLException ex) {
                conn.rollback();
                if (isRetryable(ex)) {
                    LOG.debug(String.format("merge: Retryable SQLException occurred during <executeOriginalDeliveryTxn>... sql state [%s], error code [%d].", ex.getSQLState(), ex.getErrorCode()), ex);
                    status = Status.INTERNAL.withDescription("Retryable SQLException:").augmentDescription(ex.getMessage());
                } else {
                    LOG.warn(String.format("merge: SQLException occurred during <executeOriginalDeliveryTxn> and will not be retried... sql state [%s], error code [%d].", ex.getSQLState(), ex.getErrorCode()), ex);
                    status = Status.INTERNAL.withDescription("Unretryable SQLException:").augmentDescription(ex.getMessage());
                }

            } catch (UserAbortException ex) {
                conn.rollback();
                status = Status.INTERNAL.withDescription("UserAbortException");

            } catch (RuntimeException ex) {
                // transaction execution calls RuntimeException
                conn.rollback();
                status = Status.INTERNAL.withDescription("Transaction RuntimeException:").augmentDescription(ex.getMessage());
            
            } finally {
                // change the isolation level back to default REPEATABLE_READ
                conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            }
        } catch (SQLException ex) {
            LOG.warn(String.format("Unexpected SQLException in [%s] when executing [%s].", this, new Throwable().getStackTrace()[0].getMethodName()), ex);
            status = Status.INTERNAL.withDescription("Unexpected SQLException:").augmentDescription(ex.getMessage());
        }

        if (response != null) {
            // execution succeeds
            delMsg.setReply(response);
        }

        if (status != null) {
            // execution fails
            delMsg.setStatus(status);
        }

    }

    // execute the OrderStatus transaction
    public void executeOrderStatusTxn(OrderStatusMessage osMsg) {
        OrderStatusReply response = null;
        Status status = null;
        OrderStatusRequest request = osMsg.getRequest();
        
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
                status = Status.INTERNAL.withDescription("UserAbortException");

            } catch (RuntimeException ex) {
                // transaction execution calls RuntimeException
                conn.rollback();
                status = Status.INTERNAL.withDescription("Transaction RuntimeException:").augmentDescription(ex.getMessage());
            }
        } catch (SQLException ex) {
            LOG.warn(String.format("Unexpected SQLException in [%s] when executing [%s].", this, new Throwable().getStackTrace()[0].getMethodName()), ex);
            status = Status.INTERNAL.withDescription("Unexpected SQLException:").augmentDescription(ex.getMessage());
        }

        if (response != null) {
            // execution succeeds
            osMsg.setReply(response);
        }

        if (status != null) {
            // execution fails
            osMsg.setStatus(status);
        }

    }

    // execute the StockLevel transaction
    public void executeStockLevelTxn(StockLevelMessage slMsg) {
        StockLevelReply response = null;
        Status status = null;
        StockLevelRequest request = slMsg.getRequest();
        
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
                status = Status.INTERNAL.withDescription("UserAbortException");

            } catch (RuntimeException ex) {
                // transaction execution calls RuntimeException
                conn.rollback();
                status = Status.INTERNAL.withDescription("Transaction RuntimeException:").augmentDescription(ex.getMessage());
            }
        } catch (SQLException ex) {
            LOG.warn(String.format("Unexpected SQLException in [%s] when executing [%s].", this, new Throwable().getStackTrace()[0].getMethodName()), ex);
            status = Status.INTERNAL.withDescription("Unexpected SQLException:").augmentDescription(ex.getMessage());
        }

        if (response != null) {
            // execution succeeds
            slMsg.setReply(response);
        }

        if (status != null) {
            // execution fails
            slMsg.setStatus(status);
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