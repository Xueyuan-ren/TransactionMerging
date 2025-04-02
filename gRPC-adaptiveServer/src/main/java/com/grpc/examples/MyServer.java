package com.grpc.examples;

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyServer {

    private static final Logger LOG = LoggerFactory.getLogger(MyServer.class);
    private Server server;	

    private void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            //System.out.println( "Shutdown: " + server.isShutdown() + " terminated: " + server.isTerminated());
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
    
    public static void main( String[] args ) throws IOException, InterruptedException {

        if (args.length < 4) {
            System.out.println("Input parameters: <warehouse> <merge size> <threadPerWhse> <iteration limit>\n");
            System.exit(0);
        }

        final MyServer myserver = new MyServer();
        LOG.info("Starting the server...");
        //System.out.println( "Starting the server..." );
        int port = 8080;
        int warehouse = Integer.parseInt(args[0]);
        int merge = Integer.parseInt(args[1]);
        int thread = Integer.parseInt(args[2]);
        int maxThread = Integer.parseInt(args[3]);
        int iterationLimit = Integer.parseInt(args[4]);
        myserver.server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                        .addService(new TxnServiceImpl(warehouse, merge, thread, maxThread, iterationLimit))
                        .build()
                        .start();
        
        LOG.info("Server started, listening on {}", port);
        //System.out.println("Server started, listening on " + port);
          
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    myserver.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                System.err.println("*** server shut down");
            }    
        });

        //System.out.println( "Shutdown: " + myserver.server.isShutdown() + " terminated: " + myserver.server.isTerminated());
        //myserver.stop();
        myserver.blockUntilShutdown();
    }
}
