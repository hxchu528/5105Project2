import json
import random
import grpc
import os
import statistics
from pathlib import Path

import project2_pb2
import project2_pb2_grpc

CONTROLLER_TARGET = os.environ.get("CONTROLLER_TARGET", "host.docker.internal:50050")

def print_cluster_status(resp : project2_pb2.ClusterStatusResponse):

    for i, node in enumerate(resp.nodes):
        print(f"Node {i} - [{node.target}]")
        print(f"    Num Vectors: [{node.stats.vector_count}]")
        print(f"    Mean Score:  [{node.stats.mean_score}]")
        print(f"    Score Stdev: [{node.stats.stdv_score}]")



def main():

    with grpc.insecure_channel(CONTROLLER_TARGET) as channel:
        stub = project2_pb2_grpc.ControllerServiceStub(channel)

        cluster_status_resp : project2_pb2.ClusterStatusResponse = stub.ClusterStatus(project2_pb2.ClusterStatusRequest())

        print_cluster_status(cluster_status_resp)


if __name__ == "__main__":
    main()