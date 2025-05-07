## setup python gRPC service for bayesian optimization:

sudo apt update

sudo apt install python3-pip

pip install numpy scikit-learn scikit-optimize grpcio grpcio-tools joblib


## start the gRPC server for BO:

python -m grpc_tools.protoc -I /path/to/your/proto/dir --python_out=/path/to/output/python/proto/service --grpc_python_out=/path/to/output/grpc/proto/service /path/to/your/proto/file