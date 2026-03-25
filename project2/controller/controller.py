from concurrent import futures
import threading

import grpc
from project2_pb2 import *
import project2_pb2_grpc
from utils.config import CONTROLLER_PORT, NODE_PORT
from utils.utils import choose_closest_node, create_storage_node


MAX_VECTORS_PER_NODE = 1000


class ControllerService(project2_pb2_grpc.ControllerServiceServicer):
    def __init__(self) -> None:
        self.repartitioning: bool = False
        self.next_node_num: int = 2
        self.total_vectors: int = 0
        self.nodes: list[dict[str, str | list[float]]] = [
            {
                "target": f"storage-node-1:{NODE_PORT}",
                "centroid": [],
            }
        ]

    def _run_split(self, old_target: str, new_node_num: int) -> None:
        try:
            new_target: str = create_storage_node(new_node_num)
            with grpc.insecure_channel(old_target) as channel:
                stub = project2_pb2_grpc.StorageNodeServiceStub(channel)
                response: SplitPartitionResponse = stub.SplitPartition(
                    SplitPartitionRequest(new_node_target=new_target)
                )

            self.nodes = [node for node in self.nodes if node["target"] != old_target]
            self.nodes.append(
                {
                    "target": response.old_target,
                    "centroid": list(response.old_centroid.values),
                }
            )
            self.nodes.append(
                {
                    "target": response.new_target,
                    "centroid": list(response.new_centroid.values),
                }
            )
        finally:
            self.repartitioning = False

    def Put(self, request: PutRequest, context: grpc.ServicerContext) -> PutResponse:
        # TODO:
        # Implement the controller-side Put path.
        #
        # High-level steps:
        # 1. Choose the closest node for request.record.embedding by calling:
        #       choose_closest_node(self.nodes, list(request.record.embedding))
        # 2. Open a gRPC channel to that node's target and forward the request with:
        #       StorageNodeServiceStub(...).StoreRecord(StoreRecordRequest(record=request.record))
        # 3. Update self.total_vectors.
        # 4. Update the centroid stored in self.nodes for the node that handled the Put.
        # 5. If the node's count exceeds MAX_VECTORS_PER_NODE and a split is not already running:
        #       - set self.repartitioning = True
        #       - choose the next node number
        #       - start a background thread that calls self._run_split(...)
        # 6. Return a PutResponse with the real values from the storage node response.
        #
        # Helpful fields:
        #   response.ok
        #   response.target
        #   response.centroid.values
        #   response.count
        #
        # Default placeholder return below lets the project run before you implement this.
        return PutResponse(
            ok=False,
            target="",
            target_count=0,
            split_triggered=False,
        )

    def Search(
        self, request: SearchRequest, context: grpc.ServicerContext
    ) -> SearchLocalResponse:
        # TODO:
        # Implement the controller-side Search path.
        #
        # High-level steps:
        # 1. Convert the incoming embedding into a plain Python list:
        #       query_embedding = list(request.embedding)
        # 2. Choose the closest node by calling:
        #       choose_closest_node(self.nodes, query_embedding)
        # 3. Forward the search to that node with:
        #       StorageNodeServiceStub(...).SearchLocal(
        #           SearchLocalRequest(query_embedding=query_embedding, top_k=5)
        #       )
        # 4. Return the SearchLocalResponse from that node.
        #
        # Note:
        # The proto returns SearchLocalResponse here, so you can directly return
        # the storage node's SearchLocal response object.
        #
        # Default placeholder return below lets the project run before you implement this.
        return SearchLocalResponse(
            hits=[],
            target="",
            vectors_searched=0,
        )

    def ClusterStatus(
        self, request: ClusterStatusRequest, context: grpc.ServicerContext
    ) -> ClusterStatusResponse:
        nodes_info: list[NodeInfo] = []
        for node in self.nodes:
            with grpc.insecure_channel(str(node["target"])) as channel:
                stub = project2_pb2_grpc.StorageNodeServiceStub(channel)
                stats: NodeStats = stub.GetNodeStats(GetNodeStatsRequest())

            node_info = NodeInfo(target=str(node["target"]))
            node_info.stats.CopyFrom(stats)
            nodes_info.append(node_info)

        return ClusterStatusResponse(nodes=nodes_info)


def serve() -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    project2_pb2_grpc.add_ControllerServiceServicer_to_server(ControllerService(), server)
    server.add_insecure_port(f"[::]:{CONTROLLER_PORT}")
    server.start()
    print(f"Controller listening on {CONTROLLER_PORT}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
