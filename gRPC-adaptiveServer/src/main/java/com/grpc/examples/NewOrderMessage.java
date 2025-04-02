package com.grpc.examples;


public class NewOrderMessage extends GrpcMessage {
    private NewOrderRequest request;
    private NewOrderReply reply;

    public NewOrderRequest getRequest() {
        return request;
    }

    public void setRequest(NewOrderRequest request) {
        this.request = request;
    }

    public NewOrderReply getReply() {
        return reply;
    }

    public void setReply(NewOrderReply reply) {
        this.reply = reply;
    }

}
