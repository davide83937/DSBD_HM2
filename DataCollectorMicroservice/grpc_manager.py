import os
from concurrent import futures

import grpc
import service_pb2
import service_pb2_grpc
import DatabaseManager as db

def get_stub():
    target = os.getenv("TARGET_GRPC_HOST", "localhost:50051")
    channel = grpc.insecure_channel(target)
    stub = service_pb2_grpc.UserServiceStub(channel)
    return stub

class Servicer(service_pb2_grpc.UserServiceServicer):
    def delete_interestes_by_email(self, request, context):
        email = request.email
        if not email:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "L'email è obbligatoria")
        response = db.deleteInterest(email, "", True)
        if response == 0:
            return service_pb2.UserResponse(
                status=0,
                message="Interessi cancellati"
            )
        else:
            return service_pb2.UserResponse(
                status=1,
                message="Interessi NON cancellati"
            )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    service_pb2_grpc.add_UserServiceServicer_to_server(
        Servicer(), server
    )
    port = "50052"
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"Server avviato sulla porta {port}")
    print("In attesa di client request...")
    print("")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()


