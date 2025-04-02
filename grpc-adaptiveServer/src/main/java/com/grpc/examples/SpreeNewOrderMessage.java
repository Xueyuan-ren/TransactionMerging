package com.grpc.examples;


public class SpreeNewOrderMessage extends GrpcMessage {
    private SpreeNewOrderRequest request;
    private SpreeNewOrderReply reply;

    public SpreeNewOrderRequest getRequest() {
        return request;
    }

    public void setRequest(SpreeNewOrderRequest request) {
        this.request = request;
    }

    public SpreeNewOrderReply getReply() {
        return reply;
    }

    public void setReply(SpreeNewOrderReply reply) {
        this.reply = reply;
    }

}
