package com.grpc.examples;


public class SpreeAddItemMessage extends GrpcMessage {
    private SpreeAddItemRequest request;
    private SpreeAddItemReply reply;

    public SpreeAddItemRequest getRequest() {
        return request;
    }

    public void setRequest(SpreeAddItemRequest request) {
        this.request = request;
    }

    public SpreeAddItemReply getReply() {
        return reply;
    }

    public void setReply(SpreeAddItemReply reply) {
        this.reply = reply;
    }

}
