package com.grpc.examples;


public class PaymentMessage extends GrpcMessage {
    private PaymentRequest request;
    private PaymentReply reply;

    public PaymentRequest getRequest() {
        return request;
    }

    public void setRequest(PaymentRequest request) {
        this.request = request;
    }

    public PaymentReply getReply() {
        return reply;
    }

    public void setReply(PaymentReply reply) {
        this.reply = reply;
    }

}
