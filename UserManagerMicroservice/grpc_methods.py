import os
from concurrent import futures
import grpc
import service_pb2
import service_pb2_grpc
import DbManager as db

def get_stub():
    target = os.getenv("TARGET_GRPC_HOST", "localhost:50052")
    channel = grpc.insecure_channel(target)
    stub = service_pb2_grpc.UserServiceStub(channel)
    return stub

class Servicer(service_pb2_grpc.UserServiceServicer):
    def checkUser(self, request, context):
        email = request.email
        token = request.token
        if not email:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "L'email è obbligatoria")
        if not token:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Token obbligatorio")
        response = db.check_logging(email, token)
        if response == 0:
            return service_pb2.UserResponse(
                status=0,
                message="L'utente è loggato"
            )
        else:
            return service_pb2.UserResponse(
                status=1,
                message="Non so chi sia costui"
            )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    service_pb2_grpc.add_UserServiceServicer_to_server(
        Servicer(), server
    )
    port = "50051"
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"Server avviato sulla porta {port}")
    print("In attesa di client request...")
    print("")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
