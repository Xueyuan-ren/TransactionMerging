## To compile, please first install maven and run following commands:

cd gRPC-adaptiveServer/

mvn -DskipTests clean package

## To run the gRPC server for merging:

cd gRPC-adaptiveServer/target

java -jar grpc-server-1.0-SNAPSHOT.jar $warehouse $merge_size $thread_per_warehouse $max_allowed_threads $iteration