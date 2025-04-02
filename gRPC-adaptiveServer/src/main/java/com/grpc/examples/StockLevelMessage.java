package com.grpc.examples;


public class StockLevelMessage extends GrpcMessage {
    private StockLevelRequest request;
    private StockLevelReply reply;

    public StockLevelRequest getRequest() {
        return request;
    }

    public void setRequest(StockLevelRequest request) {
        this.request = request;
    }

    public StockLevelReply getReply() {
        return reply;
    }

    public void setReply(StockLevelReply reply) {
        this.reply = reply;
    }

}
