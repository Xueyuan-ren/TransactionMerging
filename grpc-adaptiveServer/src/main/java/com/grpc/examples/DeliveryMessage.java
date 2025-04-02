package com.grpc.examples;


public class DeliveryMessage extends GrpcMessage {
    private DeliveryRequest request;
    private DeliveryReply reply;

    public DeliveryRequest getRequest() {
        return request;
    }

    public void setRequest(DeliveryRequest request) {
        this.request = request;
    }

    public DeliveryReply getReply() {
        return reply;
    }

    public void setReply(DeliveryReply reply) {
        this.reply = reply;
    }

}
