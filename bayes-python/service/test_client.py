import grpc
import ModelService_pb2
import ModelService_pb2_grpc
import random
import argparse
from skopt.sampler import Lhs

def parse_input_file(file_path):
    historical_data = []
    performance_dict = {}
    with open(file_path, 'r') as file:
        for line in file:
            # Skip empty lines and comments
            if not line.strip() or line.startswith('#'):
                continue
            try:
                thread_num, merge_size = map(int, line.strip().split(',')[:2])
                throughput = float(line.strip().split(',')[2])
                performance_dict[(thread_num, merge_size)] = throughput
                # historical_data.append({
                #     'thread_num': thread_num,
                #     'merge_size': merge_size,
                #     'throughput': throughput
                # })
            except ValueError:
                print(f"Skipping invalid line: {line.strip()}")
    
    return performance_dict

def run():
    # Connect to the gRPC server
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = ModelService_pb2_grpc.ModelServiceStub(channel)

        # randomly sample 10 data points from the input file
        performance_dict = parse_input_file('input.txt')
        lhs = Lhs(lhs_type="classic", criterion="maximin")
        param_space = [(0, 100), (0, 100)]
        initial_points = lhs.generate(param_space, n_samples=20)
        # sampleRate = args.sampleRate if args.sampleRate else 10
        # historical_data = random.sample(list(performance_dict.items()), 
        #                                 min(sampleRate, len(performance_dict)))
        historical_data = list(performance_dict.items())
        
        print(f"Sampled historical data: {historical_data}")
        historical_data = [
            ModelService_pb2.DataPoint(thread_num=key[0], merge_size=key[1], throughput=value)
            for key, value in historical_data
        ]
        
        # Test UpdatePerformanceData
        # historical_data = parse_input_file('input.txt')
        # historical_data = [ModelService_pb2.DataPoint(**data) for data in historical_data]
        performance_data = ModelService_pb2.PerformanceData(historical_data=historical_data)
        update_response = stub.UpdatePerformanceData(performance_data)
        print(f"UpdatePerformanceData response: {update_response}")
        
        # # Test PredictOptimalParameters
        # predict_request = ModelService_pb2.ParameterRequest(
        #     current_merge_size=3,
        #     current_thread_num=400,
        #     current_throughput=9742
        # )
        # predict_response = stub.PredictOptimalParameters(predict_request)
        # print(f"PredictOptimalParameters response: {predict_response}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Test gRPC client for ModelService.")
    parser.add_argument('--sampleRate', type=int, help="Number of data points to sample from the input file.")
    args = parser.parse_args()
    run()
    # parse_input_file('input')
