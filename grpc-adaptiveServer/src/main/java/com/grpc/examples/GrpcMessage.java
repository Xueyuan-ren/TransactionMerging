package com.grpc.examples;

import io.grpc.Status;

public abstract class GrpcMessage {
    private Status status; // return descriptor information if execution fails due to exceptions
    private int run = 0;

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public int getRun() {
        return run;
    }

    public void setRun(int run) {
        this.run = run;
    }

}
