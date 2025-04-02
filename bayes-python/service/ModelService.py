import numpy as np
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import RBF, WhiteKernel, Matern
from sklearn.preprocessing import StandardScaler
from skopt import gp_minimize
from skopt.plots import plot_evaluations
import matplotlib.pyplot as plt
from skopt.space import Integer
from functools import lru_cache
import threading
import multiprocessing
from typing import List
import os
#os.environ["GRPC_ENABLE_FORK_SUPPORT"] = "TRUE"
import grpc
from concurrent import futures
import ModelService_pb2
import ModelService_pb2_grpc
import signal
import sys
import joblib
import logging

class ModelServiceServicer(ModelService_pb2_grpc.ModelServiceServicer):
    def __init__(self):
        self.lock = threading.Lock()
        self.historical_data: List[ModelService_pb2.DataPoint] = []
        self.evaluated_params = set()  # Persistent cache

        # Initialize the Gaussian Process Regressor
        self.kernel = RBF(length_scale=1.0) + WhiteKernel(noise_level=1.0)
        # self.kernel = Matern(nu=2.5)
        # self.kernel = RBF(length_scale=1.0)
        self.gp = GaussianProcessRegressor(
            kernel=self.kernel,
            n_restarts_optimizer=10,  # Try 10 random restarts
            random_state=42
        )

        # Initialize the scalers
        self.scaler_x = StandardScaler()
        self.scaler_y = StandardScaler()

        # parameter space for bayesian optimization
        self.param_space = [
            Integer(1, 10, name='merge_size', dtype=int), 
            Integer(1, 10, name='thread_num', dtype=int)
        ]

        # path to save the model
        self.model_path = "saved_gpr_model.pkl"

        # Load the model if it exists
        if os.path.exists(self.model_path):
            try:
                self._load_model()
                logging.info("Loaded pre-trained model from disk")
            except Exception as e:
                logging.error(f"Model loading failed: {str(e)}")
                self.gp = GaussianProcessRegressor(kernel=self.kernel)

    def _load_model(self):
        with open(self.model_path, 'rb') as f:
            model_data = joblib.load(f)
            self.gp = model_data['gp']
            self.scaler_x = model_data['scaler_x']
            self.scaler_y = model_data['scaler_y']

    def _save_model(self):
        with open(self.model_path, 'wb') as f:
            model_data = {
                'gp': self.gp,
                'scaler_x': self.scaler_x,
                'scaler_y': self.scaler_y
            }
            joblib.dump(model_data, f)
            logging.info(f"Model saved to {self.model_path}")

    def _train_model(self, X: np.ndarray, y: np.ndarray) -> bool:
        try:
            # Scale the input data
            X_scaled = self.scaler_x.fit_transform(X)
            y_scaled = self.scaler_y.fit_transform(y.reshape(-1, 1)).flatten()

            # Train the model
            self.gp.fit(X_scaled, y_scaled)

            # check if the model converged
            if not np.isfinite(self.gp.log_marginal_likelihood_value_):
                raise ValueError("Non-finite log marginal likelihood")
            
            # Print the optimized log-marginal likelihood
            logging.info(f"Optimized log-marginal likelihood: {self.gp.log_marginal_likelihood_value_}")

            # Print the optimized kernel
            logging.info(f"Optimized kernel: {self.gp.kernel_}")

            # Save the model
            self._save_model()

            return True
        except Exception as e:
            logging.error(f"Model training failed: {str(e)}")
            return False
    

    def _bayesian_optimization(self) -> tuple:
        # Perform Bayesian optimization to find optimal parameters
        
        def objective(params):
            merge_size, thread_num = params
            # # Check if the parameters have already been evaluated
            # if (merge_size, thread_num) in self.evaluated_params:
            #     return 0.0
            # self.evaluated_params.add((merge_size, thread_num))

            X_test = np.array([params])
            X_test_scaled = self.scaler_x.transform(X_test)

            # Make predictions using the scaled test data
            y_pred = self.gp.predict(X_test_scaled, return_std=True)
            # return the negative because skopt minimizes the objective
            return -y_pred[0][0]
        
        # Run the Bayesian optimization
        res = gp_minimize(
                objective, 
                self.param_space,
                n_calls=30,  # Number of calls to the objective function
                n_initial_points=20, 
                #n_jobs=-1,  # Use all available cores
                random_state=42,
                verbose=True  # Enable verbose output
                )
        # Check convergence
        logging.info("Best LML: %s", res.fun)  # Should stabilize
        # print("Parameter convergence:", res.x_iters)  # Points cluster around optima
        # Generate and save the plot
        plot_evaluations(res)
        plt.savefig('optimization_evaluations.png')  # Saves to current directory
        plt.close()

        optimal_params = res.x
        optimal_params = [int(p) for p in optimal_params]

        # (optimal_merge_size, optimal_thread_num)
        return optimal_params

    def UpdatePerformanceData(self, request, context):
        with self.lock:
            datapoint = request.historical_data[0]
            original = ModelService_pb2.Prediction(
                optimal_merge_size=datapoint.merge_size,
                optimal_thread_num=datapoint.thread_num,
                expected_throughput=0.0
            )

            try:
                # Validate the incoming data
                valid_data = []
                for d in request.historical_data:
                    if 1 <= d.merge_size <= 20 and 1 <= d.thread_num <= 500 and d.throughput > 0:
                        valid_data.append({
                            "thread_num": d.thread_num,
                            "merge_size": d.merge_size,
                            "throughput": d.throughput
                        })
                
                # limit the historical data to 100 data points
                self.historical_data = (self.historical_data + valid_data)[-100:]

                # transform the historical data to numpy arrays
                X = np.array([[d['merge_size'], d['thread_num']] for d in self.historical_data])
                y = np.array([d['throughput'] for d in self.historical_data])

                # # train the model anyway
                # if not self._train_model(X, y):
                #     print(f"Model training failed")
                #     # return the original parameters
                #     return ModelService_pb2.UpdateResponse(
                #         success=True,
                #         prediction=original
                #     )
                # # Pair each x with y for logging or debugging purposes
                # paired_data = list(zip(X, y))
                # logging.info(f"train model with historical data (X, y): {paired_data}")
                # print(f"train model with historical data: X={X}, y={y}")
            
                # check if the model needs to be trained
                if len(X) < 20 and not hasattr(self.gp, 'kernel_'):
                    updateResponse = ModelService_pb2.UpdateResponse(
                        success=True,
                        prediction=original
                    )
                    logging.info(f"less than 20 data points, return original parameters")
                    logging.info(f"Current historical data: {self.historical_data}")
                    return updateResponse
                # train the model after 10 data points
                if not self._train_model(X, y):
                    print(f"Model training failed")
                    # return the original parameters
                    return ModelService_pb2.UpdateResponse(
                        success=True,
                        prediction=original
                    )
                # Pair each x with y for logging or debugging purposes
                paired_data = list(zip(X, y))
                logging.info(f"train model with historical data (X, y): {paired_data}")

                # # Train the model if no saved model or update every 10 data points
                # if not hasattr(self.gp, 'kernel_') or (len(X) >= 10 and len(X) % 10 == 0):
                #     if not self._train_model(X, y):
                #         logging.error(f"Model training failed")
                #         # return the original parameters
                #         return ModelService_pb2.UpdateResponse(
                #             success=True,
                #             prediction=original
                #         )
                # bayesian optimization to find optimal parameters
                try:
                    optimal_params = self._bayesian_optimization()
                    optimal_merge_size, optimal_thread_num = optimal_params

                    X_new = np.array([[optimal_merge_size, optimal_thread_num]])
                    X_new_scaled = self.scaler_x.transform(X_new)
                    y_pred = self.gp.predict(X_new_scaled)
                    y_pred_original = self.scaler_y.inverse_transform(y_pred.reshape(-1, 1))
                    expected_throughput = y_pred_original[0][0]
                    # improvement = y_pred_original[0][0] - datapoint.throughput
                    logging.info(f"UpdatePerformanceData: optimal_merge_size={optimal_merge_size}, "
                        f"optimal_thread_num={optimal_thread_num}, "
                        f"expected_throughput={expected_throughput}"
                    )
                    return ModelService_pb2.UpdateResponse(
                        success=True,
                        prediction=ModelService_pb2.Prediction(
                            optimal_merge_size=int(optimal_merge_size),
                            optimal_thread_num=int(optimal_thread_num),
                            expected_throughput=float(expected_throughput)  
                        )
                    )
                except Exception as e:
                    logging.error(f"Bayesian optimization failed: {str(e)}")
                    return ModelService_pb2.UpdateResponse(
                        success=False,
                        prediction=original
                    )
            except Exception as e:
                logging.error(f"Error updating performance data: {str(e)}")
                return ModelService_pb2.UpdateResponse(
                    success=False,
                    prediction=original
                )

    # def PredictOptimalParameters(self, request, context):
    #     with self.lock:
    #         print(f"PredictOptimalParameters: {request}")
    #         # transform the historical data to numpy arrays
    #         X = np.array([[d['merge_size'], d['thread_num']] for d in self.historical_data])
    #         y = np.array([d['throughput'] for d in self.historical_data])
    #         # print(f"PredictOptimalParameters: X={X}, y={y}")

    #         # # check if the model needs to be trained
    #         # if len(X) < 10:
    #         #     return ModelService_pb2.ParameterResponse(
    #         #         optimal_merge_size=request.current_merge_size,
    #         #         optimal_thread_num=request.current_thread_num,
    #         #         expected_improvement=0.0
    #         #     )
    #         print(f"PredictOptimalParameters: Training the model...")
    #         # Train the model if no saved model or update every 20 data points
    #         if not hasattr(self.gp, 'kernel_') or (len(X) > 0 and len(X) % 20 == 0):
    #             if not self._train_model(X, y):
    #                 return ModelService_pb2.ParameterResponse(
    #                     optimal_merge_size=request.current_merge_size,
    #                     optimal_thread_num=request.current_thread_num,
    #                     expected_improvement=0.0
    #                 )
    #         print(f"PredictOptimalParameters: Model trained successfully")
    #         # bayesian optimization to find optimal parameters
    #         try:
    #             optimal_params = self._bayesian_optimization()
    #             optimal_merge_size, optimal_thread_num = optimal_params

    #             X_new = np.array([[optimal_merge_size, optimal_thread_num]])
    #             X_new_scaled = self.scaler_x.transform(X_new)
    #             y_pred = self.gp.predict(X_new_scaled)
    #             y_pred_original = self.scaler_y.inverse_transform(y_pred.reshape(-1, 1))
    #             improvement = y_pred_original[0][0] - request.current_throughput
                
    #             return ModelService_pb2.ParameterResponse(
    #                 optimal_merge_size=int(optimal_merge_size),
    #                 optimal_thread_num=int(optimal_thread_num),
    #                 expected_improvement=float(improvement)
    #             )
    #         except Exception as e:
    #             print(f"Bayesian optimization failed: {str(e)}")
    #             return ModelService_pb2.ParameterResponse(
    #                 optimal_merge_size=request.current_merge_size,
    #                 optimal_thread_num=request.current_thread_num,
    #                 expected_improvement=0.0
    #             )

def shutdown_handler(signum, frame):
    logging.info("Shutting down server...")
    sys.exit(0)

logging.basicConfig(
    filename='bayes.log',
    level=logging.INFO,
    format='%(levelname)s - %(filename)s:%(lineno)d - %(message)s'
)

if __name__ == '__main__':
    # # Create queues for communication
    # task_queue = multiprocessing.Queue()
    # result_queue = multiprocessing.Queue()
    # # Start the worker process before starting gRPC server
    # worker = multiprocessing.Process(target=optimization_worker, args=(task_queue, result_queue))
    # worker.start()
    

    signal.signal(signal.SIGINT, shutdown_handler)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    ModelService_pb2_grpc.add_ModelServiceServicer_to_server(ModelServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    logging.info("ModelService server started on port 50051")
    # print("ModelService server started on port 50051")
    server.wait_for_termination()
