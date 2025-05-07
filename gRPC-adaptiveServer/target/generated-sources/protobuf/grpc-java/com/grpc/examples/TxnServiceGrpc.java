package com.grpc.examples;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.53.0)",
    comments = "Source: TxnService.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class TxnServiceGrpc {

  private TxnServiceGrpc() {}

  public static final String SERVICE_NAME = "com.grpc.examples.TxnService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.grpc.examples.SelectRequest,
      com.grpc.examples.SelectReply> getSelectTxnMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SelectTxn",
      requestType = com.grpc.examples.SelectRequest.class,
      responseType = com.grpc.examples.SelectReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.grpc.examples.SelectRequest,
      com.grpc.examples.SelectReply> getSelectTxnMethod() {
    io.grpc.MethodDescriptor<com.grpc.examples.SelectRequest, com.grpc.examples.SelectReply> getSelectTxnMethod;
    if ((getSelectTxnMethod = TxnServiceGrpc.getSelectTxnMethod) == null) {
      synchronized (TxnServiceGrpc.class) {
        if ((getSelectTxnMethod = TxnServiceGrpc.getSelectTxnMethod) == null) {
          TxnServiceGrpc.getSelectTxnMethod = getSelectTxnMethod =
              io.grpc.MethodDescriptor.<com.grpc.examples.SelectRequest, com.grpc.examples.SelectReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SelectTxn"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.grpc.examples.SelectRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.grpc.examples.SelectReply.getDefaultInstance()))
              .setSchemaDescriptor(new TxnServiceMethodDescriptorSupplier("SelectTxn"))
              .build();
        }
      }
    }
    return getSelectTxnMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.grpc.examples.OpenRequest,
      com.grpc.examples.OpenReply> getMakeConnectionsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "MakeConnections",
      requestType = com.grpc.examples.OpenRequest.class,
      responseType = com.grpc.examples.OpenReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.grpc.examples.OpenRequest,
      com.grpc.examples.OpenReply> getMakeConnectionsMethod() {
    io.grpc.MethodDescriptor<com.grpc.examples.OpenRequest, com.grpc.examples.OpenReply> getMakeConnectionsMethod;
    if ((getMakeConnectionsMethod = TxnServiceGrpc.getMakeConnectionsMethod) == null) {
      synchronized (TxnServiceGrpc.class) {
        if ((getMakeConnectionsMethod = TxnServiceGrpc.getMakeConnectionsMethod) == null) {
          TxnServiceGrpc.getMakeConnectionsMethod = getMakeConnectionsMethod =
              io.grpc.MethodDescriptor.<com.grpc.examples.OpenRequest, com.grpc.examples.OpenReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "MakeConnections"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.grpc.examples.OpenRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.grpc.examples.OpenReply.getDefaultInstance()))
              .setSchemaDescriptor(new TxnServiceMethodDescriptorSupplier("MakeConnections"))
              .build();
        }
      }
    }
    return getMakeConnectionsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.grpc.examples.CloseRequest,
      com.grpc.examples.CloseReply> getCloseConnectionsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CloseConnections",
      requestType = com.grpc.examples.CloseRequest.class,
      responseType = com.grpc.examples.CloseReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.grpc.examples.CloseRequest,
      com.grpc.examples.CloseReply> getCloseConnectionsMethod() {
    io.grpc.MethodDescriptor<com.grpc.examples.CloseRequest, com.grpc.examples.CloseReply> getCloseConnectionsMethod;
    if ((getCloseConnectionsMethod = TxnServiceGrpc.getCloseConnectionsMethod) == null) {
      synchronized (TxnServiceGrpc.class) {
        if ((getCloseConnectionsMethod = TxnServiceGrpc.getCloseConnectionsMethod) == null) {
          TxnServiceGrpc.getCloseConnectionsMethod = getCloseConnectionsMethod =
              io.grpc.MethodDescriptor.<com.grpc.examples.CloseRequest, com.grpc.examples.CloseReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CloseConnections"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.grpc.examples.CloseRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.grpc.examples.CloseReply.getDefaultInstance()))
              .setSchemaDescriptor(new TxnServiceMethodDescriptorSupplier("CloseConnections"))
              .build();
        }
      }
    }
    return getCloseConnectionsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.grpc.examples.NewOrderRequest,
      com.grpc.examples.NewOrderReply> getNewOrderTxnMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "NewOrderTxn",
      requestType = com.grpc.examples.NewOrderRequest.class,
      responseType = com.grpc.examples.NewOrderReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.grpc.examples.NewOrderRequest,
      com.grpc.examples.NewOrderReply> getNewOrderTxnMethod() {
    io.grpc.MethodDescriptor<com.grpc.examples.NewOrderRequest, com.grpc.examples.NewOrderReply> getNewOrderTxnMethod;
    if ((getNewOrderTxnMethod = TxnServiceGrpc.getNewOrderTxnMethod) == null) {
      synchronized (TxnServiceGrpc.class) {
        if ((getNewOrderTxnMethod = TxnServiceGrpc.getNewOrderTxnMethod) == null) {
          TxnServiceGrpc.getNewOrderTxnMethod = getNewOrderTxnMethod =
              io.grpc.MethodDescriptor.<com.grpc.examples.NewOrderRequest, com.grpc.examples.NewOrderReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "NewOrderTxn"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.grpc.examples.NewOrderRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.grpc.examples.NewOrderReply.getDefaultInstance()))
              .setSchemaDescriptor(new TxnServiceMethodDescriptorSupplier("NewOrderTxn"))
              .build();
        }
      }
    }
    return getNewOrderTxnMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.grpc.examples.PaymentRequest,
      com.grpc.examples.PaymentReply> getPaymentTxnMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PaymentTxn",
      requestType = com.grpc.examples.PaymentRequest.class,
      responseType = com.grpc.examples.PaymentReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.grpc.examples.PaymentRequest,
      com.grpc.examples.PaymentReply> getPaymentTxnMethod() {
    io.grpc.MethodDescriptor<com.grpc.examples.PaymentRequest, com.grpc.examples.PaymentReply> getPaymentTxnMethod;
    if ((getPaymentTxnMethod = TxnServiceGrpc.getPaymentTxnMethod) == null) {
      synchronized (TxnServiceGrpc.class) {
        if ((getPaymentTxnMethod = TxnServiceGrpc.getPaymentTxnMethod) == null) {
          TxnServiceGrpc.getPaymentTxnMethod = getPaymentTxnMethod =
              io.grpc.MethodDescriptor.<com.grpc.examples.PaymentRequest, com.grpc.examples.PaymentReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PaymentTxn"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.grpc.examples.PaymentRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.grpc.examples.PaymentReply.getDefaultInstance()))
              .setSchemaDescriptor(new TxnServiceMethodDescriptorSupplier("PaymentTxn"))
              .build();
        }
      }
    }
    return getPaymentTxnMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.grpc.examples.DeliveryRequest,
      com.grpc.examples.DeliveryReply> getDeliveryTxnMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeliveryTxn",
      requestType = com.grpc.examples.DeliveryRequest.class,
      responseType = com.grpc.examples.DeliveryReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.grpc.examples.DeliveryRequest,
      com.grpc.examples.DeliveryReply> getDeliveryTxnMethod() {
    io.grpc.MethodDescriptor<com.grpc.examples.DeliveryRequest, com.grpc.examples.DeliveryReply> getDeliveryTxnMethod;
    if ((getDeliveryTxnMethod = TxnServiceGrpc.getDeliveryTxnMethod) == null) {
      synchronized (TxnServiceGrpc.class) {
        if ((getDeliveryTxnMethod = TxnServiceGrpc.getDeliveryTxnMethod) == null) {
          TxnServiceGrpc.getDeliveryTxnMethod = getDeliveryTxnMethod =
              io.grpc.MethodDescriptor.<com.grpc.examples.DeliveryRequest, com.grpc.examples.DeliveryReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DeliveryTxn"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.grpc.examples.DeliveryRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.grpc.examples.DeliveryReply.getDefaultInstance()))
              .setSchemaDescriptor(new TxnServiceMethodDescriptorSupplier("DeliveryTxn"))
              .build();
        }
      }
    }
    return getDeliveryTxnMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.grpc.examples.OrderStatusRequest,
      com.grpc.examples.OrderStatusReply> getOrderStatusTxnMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "OrderStatusTxn",
      requestType = com.grpc.examples.OrderStatusRequest.class,
      responseType = com.grpc.examples.OrderStatusReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.grpc.examples.OrderStatusRequest,
      com.grpc.examples.OrderStatusReply> getOrderStatusTxnMethod() {
    io.grpc.MethodDescriptor<com.grpc.examples.OrderStatusRequest, com.grpc.examples.OrderStatusReply> getOrderStatusTxnMethod;
    if ((getOrderStatusTxnMethod = TxnServiceGrpc.getOrderStatusTxnMethod) == null) {
      synchronized (TxnServiceGrpc.class) {
        if ((getOrderStatusTxnMethod = TxnServiceGrpc.getOrderStatusTxnMethod) == null) {
          TxnServiceGrpc.getOrderStatusTxnMethod = getOrderStatusTxnMethod =
              io.grpc.MethodDescriptor.<com.grpc.examples.OrderStatusRequest, com.grpc.examples.OrderStatusReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "OrderStatusTxn"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.grpc.examples.OrderStatusRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.grpc.examples.OrderStatusReply.getDefaultInstance()))
              .setSchemaDescriptor(new TxnServiceMethodDescriptorSupplier("OrderStatusTxn"))
              .build();
        }
      }
    }
    return getOrderStatusTxnMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.grpc.examples.StockLevelRequest,
      com.grpc.examples.StockLevelReply> getStockLevelTxnMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "StockLevelTxn",
      requestType = com.grpc.examples.StockLevelRequest.class,
      responseType = com.grpc.examples.StockLevelReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.grpc.examples.StockLevelRequest,
      com.grpc.examples.StockLevelReply> getStockLevelTxnMethod() {
    io.grpc.MethodDescriptor<com.grpc.examples.StockLevelRequest, com.grpc.examples.StockLevelReply> getStockLevelTxnMethod;
    if ((getStockLevelTxnMethod = TxnServiceGrpc.getStockLevelTxnMethod) == null) {
      synchronized (TxnServiceGrpc.class) {
        if ((getStockLevelTxnMethod = TxnServiceGrpc.getStockLevelTxnMethod) == null) {
          TxnServiceGrpc.getStockLevelTxnMethod = getStockLevelTxnMethod =
              io.grpc.MethodDescriptor.<com.grpc.examples.StockLevelRequest, com.grpc.examples.StockLevelReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StockLevelTxn"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.grpc.examples.StockLevelRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.grpc.examples.StockLevelReply.getDefaultInstance()))
              .setSchemaDescriptor(new TxnServiceMethodDescriptorSupplier("StockLevelTxn"))
              .build();
        }
      }
    }
    return getStockLevelTxnMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.grpc.examples.SpreeNewOrderRequest,
      com.grpc.examples.SpreeNewOrderReply> getSpreeNewOrderTxnMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SpreeNewOrderTxn",
      requestType = com.grpc.examples.SpreeNewOrderRequest.class,
      responseType = com.grpc.examples.SpreeNewOrderReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.grpc.examples.SpreeNewOrderRequest,
      com.grpc.examples.SpreeNewOrderReply> getSpreeNewOrderTxnMethod() {
    io.grpc.MethodDescriptor<com.grpc.examples.SpreeNewOrderRequest, com.grpc.examples.SpreeNewOrderReply> getSpreeNewOrderTxnMethod;
    if ((getSpreeNewOrderTxnMethod = TxnServiceGrpc.getSpreeNewOrderTxnMethod) == null) {
      synchronized (TxnServiceGrpc.class) {
        if ((getSpreeNewOrderTxnMethod = TxnServiceGrpc.getSpreeNewOrderTxnMethod) == null) {
          TxnServiceGrpc.getSpreeNewOrderTxnMethod = getSpreeNewOrderTxnMethod =
              io.grpc.MethodDescriptor.<com.grpc.examples.SpreeNewOrderRequest, com.grpc.examples.SpreeNewOrderReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SpreeNewOrderTxn"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.grpc.examples.SpreeNewOrderRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.grpc.examples.SpreeNewOrderReply.getDefaultInstance()))
              .setSchemaDescriptor(new TxnServiceMethodDescriptorSupplier("SpreeNewOrderTxn"))
              .build();
        }
      }
    }
    return getSpreeNewOrderTxnMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.grpc.examples.SpreeAddItemRequest,
      com.grpc.examples.SpreeAddItemReply> getSpreeAddItemTxnMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SpreeAddItemTxn",
      requestType = com.grpc.examples.SpreeAddItemRequest.class,
      responseType = com.grpc.examples.SpreeAddItemReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.grpc.examples.SpreeAddItemRequest,
      com.grpc.examples.SpreeAddItemReply> getSpreeAddItemTxnMethod() {
    io.grpc.MethodDescriptor<com.grpc.examples.SpreeAddItemRequest, com.grpc.examples.SpreeAddItemReply> getSpreeAddItemTxnMethod;
    if ((getSpreeAddItemTxnMethod = TxnServiceGrpc.getSpreeAddItemTxnMethod) == null) {
      synchronized (TxnServiceGrpc.class) {
        if ((getSpreeAddItemTxnMethod = TxnServiceGrpc.getSpreeAddItemTxnMethod) == null) {
          TxnServiceGrpc.getSpreeAddItemTxnMethod = getSpreeAddItemTxnMethod =
              io.grpc.MethodDescriptor.<com.grpc.examples.SpreeAddItemRequest, com.grpc.examples.SpreeAddItemReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SpreeAddItemTxn"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.grpc.examples.SpreeAddItemRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.grpc.examples.SpreeAddItemReply.getDefaultInstance()))
              .setSchemaDescriptor(new TxnServiceMethodDescriptorSupplier("SpreeAddItemTxn"))
              .build();
        }
      }
    }
    return getSpreeAddItemTxnMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.grpc.examples.TuneServerRequest,
      com.grpc.examples.TuneServerReply> getTuneServerMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "TuneServer",
      requestType = com.grpc.examples.TuneServerRequest.class,
      responseType = com.grpc.examples.TuneServerReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.grpc.examples.TuneServerRequest,
      com.grpc.examples.TuneServerReply> getTuneServerMethod() {
    io.grpc.MethodDescriptor<com.grpc.examples.TuneServerRequest, com.grpc.examples.TuneServerReply> getTuneServerMethod;
    if ((getTuneServerMethod = TxnServiceGrpc.getTuneServerMethod) == null) {
      synchronized (TxnServiceGrpc.class) {
        if ((getTuneServerMethod = TxnServiceGrpc.getTuneServerMethod) == null) {
          TxnServiceGrpc.getTuneServerMethod = getTuneServerMethod =
              io.grpc.MethodDescriptor.<com.grpc.examples.TuneServerRequest, com.grpc.examples.TuneServerReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "TuneServer"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.grpc.examples.TuneServerRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.grpc.examples.TuneServerReply.getDefaultInstance()))
              .setSchemaDescriptor(new TxnServiceMethodDescriptorSupplier("TuneServer"))
              .build();
        }
      }
    }
    return getTuneServerMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.grpc.examples.PingRequest,
      com.grpc.examples.PingResponse> getPingMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Ping",
      requestType = com.grpc.examples.PingRequest.class,
      responseType = com.grpc.examples.PingResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.grpc.examples.PingRequest,
      com.grpc.examples.PingResponse> getPingMethod() {
    io.grpc.MethodDescriptor<com.grpc.examples.PingRequest, com.grpc.examples.PingResponse> getPingMethod;
    if ((getPingMethod = TxnServiceGrpc.getPingMethod) == null) {
      synchronized (TxnServiceGrpc.class) {
        if ((getPingMethod = TxnServiceGrpc.getPingMethod) == null) {
          TxnServiceGrpc.getPingMethod = getPingMethod =
              io.grpc.MethodDescriptor.<com.grpc.examples.PingRequest, com.grpc.examples.PingResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Ping"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.grpc.examples.PingRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.grpc.examples.PingResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TxnServiceMethodDescriptorSupplier("Ping"))
              .build();
        }
      }
    }
    return getPingMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static TxnServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TxnServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TxnServiceStub>() {
        @java.lang.Override
        public TxnServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TxnServiceStub(channel, callOptions);
        }
      };
    return TxnServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static TxnServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TxnServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TxnServiceBlockingStub>() {
        @java.lang.Override
        public TxnServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TxnServiceBlockingStub(channel, callOptions);
        }
      };
    return TxnServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static TxnServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TxnServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TxnServiceFutureStub>() {
        @java.lang.Override
        public TxnServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TxnServiceFutureStub(channel, callOptions);
        }
      };
    return TxnServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class TxnServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Send a select transaction
     * </pre>
     */
    public void selectTxn(com.grpc.examples.SelectRequest request,
        io.grpc.stub.StreamObserver<com.grpc.examples.SelectReply> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSelectTxnMethod(), responseObserver);
    }

    /**
     * <pre>
     * Send a MakeConnections
     * </pre>
     */
    public void makeConnections(com.grpc.examples.OpenRequest request,
        io.grpc.stub.StreamObserver<com.grpc.examples.OpenReply> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getMakeConnectionsMethod(), responseObserver);
    }

    /**
     * <pre>
     * Send a CloseConnections
     * </pre>
     */
    public void closeConnections(com.grpc.examples.CloseRequest request,
        io.grpc.stub.StreamObserver<com.grpc.examples.CloseReply> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCloseConnectionsMethod(), responseObserver);
    }

    /**
     * <pre>
     * Send a NewOrder transaction
     * </pre>
     */
    public void newOrderTxn(com.grpc.examples.NewOrderRequest request,
        io.grpc.stub.StreamObserver<com.grpc.examples.NewOrderReply> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getNewOrderTxnMethod(), responseObserver);
    }

    /**
     * <pre>
     * Send a Payment transaction
     * </pre>
     */
    public void paymentTxn(com.grpc.examples.PaymentRequest request,
        io.grpc.stub.StreamObserver<com.grpc.examples.PaymentReply> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPaymentTxnMethod(), responseObserver);
    }

    /**
     * <pre>
     * Send a Delivery transaction
     * </pre>
     */
    public void deliveryTxn(com.grpc.examples.DeliveryRequest request,
        io.grpc.stub.StreamObserver<com.grpc.examples.DeliveryReply> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeliveryTxnMethod(), responseObserver);
    }

    /**
     * <pre>
     * Send a OrderStatus transaction
     * </pre>
     */
    public void orderStatusTxn(com.grpc.examples.OrderStatusRequest request,
        io.grpc.stub.StreamObserver<com.grpc.examples.OrderStatusReply> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getOrderStatusTxnMethod(), responseObserver);
    }

    /**
     * <pre>
     * Send a StockLevel transaction
     * </pre>
     */
    public void stockLevelTxn(com.grpc.examples.StockLevelRequest request,
        io.grpc.stub.StreamObserver<com.grpc.examples.StockLevelReply> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getStockLevelTxnMethod(), responseObserver);
    }

    /**
     * <pre>
     * Send a Spree NewOrder transaction
     * </pre>
     */
    public void spreeNewOrderTxn(com.grpc.examples.SpreeNewOrderRequest request,
        io.grpc.stub.StreamObserver<com.grpc.examples.SpreeNewOrderReply> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSpreeNewOrderTxnMethod(), responseObserver);
    }

    /**
     * <pre>
     * Send a Spree AddItem transaction
     * </pre>
     */
    public void spreeAddItemTxn(com.grpc.examples.SpreeAddItemRequest request,
        io.grpc.stub.StreamObserver<com.grpc.examples.SpreeAddItemReply> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSpreeAddItemTxnMethod(), responseObserver);
    }

    /**
     * <pre>
     * request to change grpc server mergesize and threads#
     * </pre>
     */
    public void tuneServer(com.grpc.examples.TuneServerRequest request,
        io.grpc.stub.StreamObserver<com.grpc.examples.TuneServerReply> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getTuneServerMethod(), responseObserver);
    }

    /**
     * <pre>
     * ping check
     * </pre>
     */
    public void ping(com.grpc.examples.PingRequest request,
        io.grpc.stub.StreamObserver<com.grpc.examples.PingResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPingMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getSelectTxnMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.grpc.examples.SelectRequest,
                com.grpc.examples.SelectReply>(
                  this, METHODID_SELECT_TXN)))
          .addMethod(
            getMakeConnectionsMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.grpc.examples.OpenRequest,
                com.grpc.examples.OpenReply>(
                  this, METHODID_MAKE_CONNECTIONS)))
          .addMethod(
            getCloseConnectionsMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.grpc.examples.CloseRequest,
                com.grpc.examples.CloseReply>(
                  this, METHODID_CLOSE_CONNECTIONS)))
          .addMethod(
            getNewOrderTxnMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.grpc.examples.NewOrderRequest,
                com.grpc.examples.NewOrderReply>(
                  this, METHODID_NEW_ORDER_TXN)))
          .addMethod(
            getPaymentTxnMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.grpc.examples.PaymentRequest,
                com.grpc.examples.PaymentReply>(
                  this, METHODID_PAYMENT_TXN)))
          .addMethod(
            getDeliveryTxnMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.grpc.examples.DeliveryRequest,
                com.grpc.examples.DeliveryReply>(
                  this, METHODID_DELIVERY_TXN)))
          .addMethod(
            getOrderStatusTxnMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.grpc.examples.OrderStatusRequest,
                com.grpc.examples.OrderStatusReply>(
                  this, METHODID_ORDER_STATUS_TXN)))
          .addMethod(
            getStockLevelTxnMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.grpc.examples.StockLevelRequest,
                com.grpc.examples.StockLevelReply>(
                  this, METHODID_STOCK_LEVEL_TXN)))
          .addMethod(
            getSpreeNewOrderTxnMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.grpc.examples.SpreeNewOrderRequest,
                com.grpc.examples.SpreeNewOrderReply>(
                  this, METHODID_SPREE_NEW_ORDER_TXN)))
          .addMethod(
            getSpreeAddItemTxnMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.grpc.examples.SpreeAddItemRequest,
                com.grpc.examples.SpreeAddItemReply>(
                  this, METHODID_SPREE_ADD_ITEM_TXN)))
          .addMethod(
            getTuneServerMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.grpc.examples.TuneServerRequest,
                com.grpc.examples.TuneServerReply>(
                  this, METHODID_TUNE_SERVER)))
          .addMethod(
            getPingMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.grpc.examples.PingRequest,
                com.grpc.examples.PingResponse>(
                  this, METHODID_PING)))
          .build();
    }
  }

  /**
   */
  public static final class TxnServiceStub extends io.grpc.stub.AbstractAsyncStub<TxnServiceStub> {
    private TxnServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TxnServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TxnServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Send a select transaction
     * </pre>
     */
    public void selectTxn(com.grpc.examples.SelectRequest request,
        io.grpc.stub.StreamObserver<com.grpc.examples.SelectReply> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSelectTxnMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Send a MakeConnections
     * </pre>
     */
    public void makeConnections(com.grpc.examples.OpenRequest request,
        io.grpc.stub.StreamObserver<com.grpc.examples.OpenReply> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getMakeConnectionsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Send a CloseConnections
     * </pre>
     */
    public void closeConnections(com.grpc.examples.CloseRequest request,
        io.grpc.stub.StreamObserver<com.grpc.examples.CloseReply> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCloseConnectionsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Send a NewOrder transaction
     * </pre>
     */
    public void newOrderTxn(com.grpc.examples.NewOrderRequest request,
        io.grpc.stub.StreamObserver<com.grpc.examples.NewOrderReply> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getNewOrderTxnMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Send a Payment transaction
     * </pre>
     */
    public void paymentTxn(com.grpc.examples.PaymentRequest request,
        io.grpc.stub.StreamObserver<com.grpc.examples.PaymentReply> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPaymentTxnMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Send a Delivery transaction
     * </pre>
     */
    public void deliveryTxn(com.grpc.examples.DeliveryRequest request,
        io.grpc.stub.StreamObserver<com.grpc.examples.DeliveryReply> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeliveryTxnMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Send a OrderStatus transaction
     * </pre>
     */
    public void orderStatusTxn(com.grpc.examples.OrderStatusRequest request,
        io.grpc.stub.StreamObserver<com.grpc.examples.OrderStatusReply> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getOrderStatusTxnMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Send a StockLevel transaction
     * </pre>
     */
    public void stockLevelTxn(com.grpc.examples.StockLevelRequest request,
        io.grpc.stub.StreamObserver<com.grpc.examples.StockLevelReply> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getStockLevelTxnMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Send a Spree NewOrder transaction
     * </pre>
     */
    public void spreeNewOrderTxn(com.grpc.examples.SpreeNewOrderRequest request,
        io.grpc.stub.StreamObserver<com.grpc.examples.SpreeNewOrderReply> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSpreeNewOrderTxnMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Send a Spree AddItem transaction
     * </pre>
     */
    public void spreeAddItemTxn(com.grpc.examples.SpreeAddItemRequest request,
        io.grpc.stub.StreamObserver<com.grpc.examples.SpreeAddItemReply> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSpreeAddItemTxnMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * request to change grpc server mergesize and threads#
     * </pre>
     */
    public void tuneServer(com.grpc.examples.TuneServerRequest request,
        io.grpc.stub.StreamObserver<com.grpc.examples.TuneServerReply> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getTuneServerMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * ping check
     * </pre>
     */
    public void ping(com.grpc.examples.PingRequest request,
        io.grpc.stub.StreamObserver<com.grpc.examples.PingResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPingMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class TxnServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<TxnServiceBlockingStub> {
    private TxnServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TxnServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TxnServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Send a select transaction
     * </pre>
     */
    public com.grpc.examples.SelectReply selectTxn(com.grpc.examples.SelectRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSelectTxnMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Send a MakeConnections
     * </pre>
     */
    public com.grpc.examples.OpenReply makeConnections(com.grpc.examples.OpenRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getMakeConnectionsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Send a CloseConnections
     * </pre>
     */
    public com.grpc.examples.CloseReply closeConnections(com.grpc.examples.CloseRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCloseConnectionsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Send a NewOrder transaction
     * </pre>
     */
    public com.grpc.examples.NewOrderReply newOrderTxn(com.grpc.examples.NewOrderRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getNewOrderTxnMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Send a Payment transaction
     * </pre>
     */
    public com.grpc.examples.PaymentReply paymentTxn(com.grpc.examples.PaymentRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPaymentTxnMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Send a Delivery transaction
     * </pre>
     */
    public com.grpc.examples.DeliveryReply deliveryTxn(com.grpc.examples.DeliveryRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeliveryTxnMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Send a OrderStatus transaction
     * </pre>
     */
    public com.grpc.examples.OrderStatusReply orderStatusTxn(com.grpc.examples.OrderStatusRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getOrderStatusTxnMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Send a StockLevel transaction
     * </pre>
     */
    public com.grpc.examples.StockLevelReply stockLevelTxn(com.grpc.examples.StockLevelRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getStockLevelTxnMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Send a Spree NewOrder transaction
     * </pre>
     */
    public com.grpc.examples.SpreeNewOrderReply spreeNewOrderTxn(com.grpc.examples.SpreeNewOrderRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSpreeNewOrderTxnMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Send a Spree AddItem transaction
     * </pre>
     */
    public com.grpc.examples.SpreeAddItemReply spreeAddItemTxn(com.grpc.examples.SpreeAddItemRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSpreeAddItemTxnMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * request to change grpc server mergesize and threads#
     * </pre>
     */
    public com.grpc.examples.TuneServerReply tuneServer(com.grpc.examples.TuneServerRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getTuneServerMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ping check
     * </pre>
     */
    public com.grpc.examples.PingResponse ping(com.grpc.examples.PingRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPingMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class TxnServiceFutureStub extends io.grpc.stub.AbstractFutureStub<TxnServiceFutureStub> {
    private TxnServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TxnServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TxnServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Send a select transaction
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.grpc.examples.SelectReply> selectTxn(
        com.grpc.examples.SelectRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSelectTxnMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Send a MakeConnections
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.grpc.examples.OpenReply> makeConnections(
        com.grpc.examples.OpenRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getMakeConnectionsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Send a CloseConnections
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.grpc.examples.CloseReply> closeConnections(
        com.grpc.examples.CloseRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCloseConnectionsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Send a NewOrder transaction
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.grpc.examples.NewOrderReply> newOrderTxn(
        com.grpc.examples.NewOrderRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getNewOrderTxnMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Send a Payment transaction
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.grpc.examples.PaymentReply> paymentTxn(
        com.grpc.examples.PaymentRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPaymentTxnMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Send a Delivery transaction
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.grpc.examples.DeliveryReply> deliveryTxn(
        com.grpc.examples.DeliveryRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeliveryTxnMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Send a OrderStatus transaction
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.grpc.examples.OrderStatusReply> orderStatusTxn(
        com.grpc.examples.OrderStatusRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getOrderStatusTxnMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Send a StockLevel transaction
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.grpc.examples.StockLevelReply> stockLevelTxn(
        com.grpc.examples.StockLevelRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getStockLevelTxnMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Send a Spree NewOrder transaction
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.grpc.examples.SpreeNewOrderReply> spreeNewOrderTxn(
        com.grpc.examples.SpreeNewOrderRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSpreeNewOrderTxnMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Send a Spree AddItem transaction
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.grpc.examples.SpreeAddItemReply> spreeAddItemTxn(
        com.grpc.examples.SpreeAddItemRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSpreeAddItemTxnMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * request to change grpc server mergesize and threads#
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.grpc.examples.TuneServerReply> tuneServer(
        com.grpc.examples.TuneServerRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getTuneServerMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * ping check
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.grpc.examples.PingResponse> ping(
        com.grpc.examples.PingRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPingMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SELECT_TXN = 0;
  private static final int METHODID_MAKE_CONNECTIONS = 1;
  private static final int METHODID_CLOSE_CONNECTIONS = 2;
  private static final int METHODID_NEW_ORDER_TXN = 3;
  private static final int METHODID_PAYMENT_TXN = 4;
  private static final int METHODID_DELIVERY_TXN = 5;
  private static final int METHODID_ORDER_STATUS_TXN = 6;
  private static final int METHODID_STOCK_LEVEL_TXN = 7;
  private static final int METHODID_SPREE_NEW_ORDER_TXN = 8;
  private static final int METHODID_SPREE_ADD_ITEM_TXN = 9;
  private static final int METHODID_TUNE_SERVER = 10;
  private static final int METHODID_PING = 11;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final TxnServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(TxnServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SELECT_TXN:
          serviceImpl.selectTxn((com.grpc.examples.SelectRequest) request,
              (io.grpc.stub.StreamObserver<com.grpc.examples.SelectReply>) responseObserver);
          break;
        case METHODID_MAKE_CONNECTIONS:
          serviceImpl.makeConnections((com.grpc.examples.OpenRequest) request,
              (io.grpc.stub.StreamObserver<com.grpc.examples.OpenReply>) responseObserver);
          break;
        case METHODID_CLOSE_CONNECTIONS:
          serviceImpl.closeConnections((com.grpc.examples.CloseRequest) request,
              (io.grpc.stub.StreamObserver<com.grpc.examples.CloseReply>) responseObserver);
          break;
        case METHODID_NEW_ORDER_TXN:
          serviceImpl.newOrderTxn((com.grpc.examples.NewOrderRequest) request,
              (io.grpc.stub.StreamObserver<com.grpc.examples.NewOrderReply>) responseObserver);
          break;
        case METHODID_PAYMENT_TXN:
          serviceImpl.paymentTxn((com.grpc.examples.PaymentRequest) request,
              (io.grpc.stub.StreamObserver<com.grpc.examples.PaymentReply>) responseObserver);
          break;
        case METHODID_DELIVERY_TXN:
          serviceImpl.deliveryTxn((com.grpc.examples.DeliveryRequest) request,
              (io.grpc.stub.StreamObserver<com.grpc.examples.DeliveryReply>) responseObserver);
          break;
        case METHODID_ORDER_STATUS_TXN:
          serviceImpl.orderStatusTxn((com.grpc.examples.OrderStatusRequest) request,
              (io.grpc.stub.StreamObserver<com.grpc.examples.OrderStatusReply>) responseObserver);
          break;
        case METHODID_STOCK_LEVEL_TXN:
          serviceImpl.stockLevelTxn((com.grpc.examples.StockLevelRequest) request,
              (io.grpc.stub.StreamObserver<com.grpc.examples.StockLevelReply>) responseObserver);
          break;
        case METHODID_SPREE_NEW_ORDER_TXN:
          serviceImpl.spreeNewOrderTxn((com.grpc.examples.SpreeNewOrderRequest) request,
              (io.grpc.stub.StreamObserver<com.grpc.examples.SpreeNewOrderReply>) responseObserver);
          break;
        case METHODID_SPREE_ADD_ITEM_TXN:
          serviceImpl.spreeAddItemTxn((com.grpc.examples.SpreeAddItemRequest) request,
              (io.grpc.stub.StreamObserver<com.grpc.examples.SpreeAddItemReply>) responseObserver);
          break;
        case METHODID_TUNE_SERVER:
          serviceImpl.tuneServer((com.grpc.examples.TuneServerRequest) request,
              (io.grpc.stub.StreamObserver<com.grpc.examples.TuneServerReply>) responseObserver);
          break;
        case METHODID_PING:
          serviceImpl.ping((com.grpc.examples.PingRequest) request,
              (io.grpc.stub.StreamObserver<com.grpc.examples.PingResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class TxnServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    TxnServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.grpc.examples.TxnServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("TxnService");
    }
  }

  private static final class TxnServiceFileDescriptorSupplier
      extends TxnServiceBaseDescriptorSupplier {
    TxnServiceFileDescriptorSupplier() {}
  }

  private static final class TxnServiceMethodDescriptorSupplier
      extends TxnServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    TxnServiceMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (TxnServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new TxnServiceFileDescriptorSupplier())
              .addMethod(getSelectTxnMethod())
              .addMethod(getMakeConnectionsMethod())
              .addMethod(getCloseConnectionsMethod())
              .addMethod(getNewOrderTxnMethod())
              .addMethod(getPaymentTxnMethod())
              .addMethod(getDeliveryTxnMethod())
              .addMethod(getOrderStatusTxnMethod())
              .addMethod(getStockLevelTxnMethod())
              .addMethod(getSpreeNewOrderTxnMethod())
              .addMethod(getSpreeAddItemTxnMethod())
              .addMethod(getTuneServerMethod())
              .addMethod(getPingMethod())
              .build();
        }
      }
    }
    return result;
  }
}
