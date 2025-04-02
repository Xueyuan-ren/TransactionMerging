package com.grpc.examples;


public class OrderStatusMessage extends GrpcMessage {
    private OrderStatusRequest request;
    private OrderStatusReply reply;

    public OrderStatusRequest getRequest() {
        return request;
    }

    public void setRequest(OrderStatusRequest request) {
        this.request = request;
    }

    public OrderStatusReply getReply() {
        return reply;
    }

    public void setReply(OrderStatusReply reply) {
        this.reply = reply;
    }

}
