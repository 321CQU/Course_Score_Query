import asyncio
import logging
import grpc

from micro_services_protobuf.course_score_query import service_pb2_grpc as csq_grpc

from _321CQU.tools.gRPCManager import gRPCManager
from _321CQU.service import ServiceEnum

from service import CourseScoreQuery


async def serve():
    port = gRPCManager().get_service_config(ServiceEnum.CourseScoreQuery)[1]

    server = grpc.aio.server()
    csq_grpc.add_CourseScoreQueryServicer_to_server(CourseScoreQuery(), server)
    server.add_insecure_port('[::]:' + port)
    await server.start()
    await server.wait_for_termination()


if __name__ == '__main__':
    print("启动 course score query 服务")
    logging.basicConfig(level=logging.INFO)
    asyncio.new_event_loop().run_until_complete(serve())
