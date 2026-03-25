import os
import statistics
import threading
from concurrent import futures

import grpc
from project2_pb2 import *
import project2_pb2_grpc
from utils.utils import cosine_similarity, update_centroid, local_top_k, kmeans_split


GRPC_SERVER_PORT = int(os.environ.get("GRPC_SERVER_PORT", "50051"))
NODE_TARGET = os.environ.get("NODE_TARGET", f"localhost:{GRPC_SERVER_PORT}")


class StorageNodeService(project2_pb2_grpc.StorageNodeServiceServicer):
    def __init__(self) -> None:
        self.cv = threading.Condition()
        self.repartitioning: bool = False
        self.records: list[Record] = []
        self.centroid: list[float] = []

    def _wait_if_repartitioning(self) -> None:
        with self.cv:
            while self.repartitioning:
                self.cv.wait()

    def StoreRecord(
        self, request: StoreRecordRequest, context: grpc.ServicerContext
    ) -> StoreRecordResponse:
        # TODO:
        # Implement the local storage-node Put logic.
        #
        # High-level steps:
        # 1. Append request.record to self.records.
        # 2. Recompute the centroid for this node by calling:
        #       update_centroid(self.records)
        # 3. Return a StoreRecordResponse containing:
        #       - ok=True
        #       - target=NODE_TARGET
        #       - centroid=Centroid(values=self.centroid)
        #       - count=len(self.records)
        #
        # Default placeholder return below lets the project run before you implement this.
        return StoreRecordResponse(
            ok=False,
            target=NODE_TARGET,
            centroid=Centroid(values=[]),
            count=len(self.records),
        )

    def SearchLocal(
        self, request: SearchLocalRequest, context: grpc.ServicerContext
    ) -> SearchLocalResponse:
        # TODO:
        # Implement local top-k semantic search on this node.
        #
        # High-level steps:
        # 1. Use the helper:
        #       local_top_k(self.records, list(request.query_embedding), request.top_k)
        # 2. Return those hits in a SearchLocalResponse.
        # 3. vectors_searched should usually be len(self.records), since this node
        #    is doing a full scan over its own local partition.
        #
        # Default placeholder return below lets the project run before you implement this.
        return SearchLocalResponse(
            hits=[],
            target=NODE_TARGET,
            vectors_searched=0,
        )

    def ReplaceLocalPartition(
        self, request: ReplaceLocalPartitionRequest, context: grpc.ServicerContext
    ) -> ReplaceLocalPartitionResponse:
        with self.cv:
            self.records = list(request.records)
            self.centroid = list(request.centroid.values)
            return ReplaceLocalPartitionResponse(
                ok=True,
                target=NODE_TARGET,
                count=len(self.records),
            )

    def SplitPartition(
        self, request: SplitPartitionRequest, context: grpc.ServicerContext
    ) -> SplitPartitionResponse:
        # TODO:
        # Implement local repartitioning on the overloaded node.
        #
        # High-level steps:
        # 1. Copy the current local records into a temporary list.
        # 2. Split them into two partitions with:
        #       keep_records, move_records, keep_centroid, move_centroid = kmeans_split(local_records)
        # 3. Open a gRPC channel to request.new_node_target.
        # 4. Send the moved partition to the new node with:
        #       ReplaceLocalPartitionRequest(records=move_records, centroid=Centroid(values=move_centroid))
        # 5. Replace this node's local records/centroid with the "keep" partition.
        # 6. Return a SplitPartitionResponse with:
        #       - old_target = NODE_TARGET
        #       - old_centroid = keep centroid
        #       - old_count = len(keep_records)
        #       - new_target = request.new_node_target
        #       - new_centroid = move centroid
        #       - new_count = len(move_records)
        #
        # Default placeholder return below lets the project run before you implement this.
        return SplitPartitionResponse(
            ok=False,
            old_target=NODE_TARGET,
            old_centroid=Centroid(values=self.centroid),
            old_count=len(self.records),
            new_target=request.new_node_target,
            new_centroid=Centroid(values=[]),
            new_count=0,
        )

    def GetNodeStats(
        self, request: GetNodeStatsRequest, context: grpc.ServicerContext
    ) -> NodeStats:
        self._wait_if_repartitioning()
        with self.cv:
            if not self.records:
                return NodeStats(
                    vector_count=0,
                    mean_score=0.0,
                    stdv_score=0.0,
                )

            if not self.centroid:
                return NodeStats(
                    vector_count=len(self.records),
                    mean_score=0.0,
                    stdv_score=0.0,
                )

            scores: list[float] = [
                cosine_similarity(list(record.embedding), self.centroid)
                for record in self.records
            ]

            mean_score: float = statistics.mean(scores)
            stdv_score: float = statistics.stdev(scores) if len(scores) > 1 else 0.0

            return NodeStats(
                vector_count=len(self.records),
                mean_score=mean_score,
                stdv_score=stdv_score,
            )


def serve() -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    project2_pb2_grpc.add_StorageNodeServiceServicer_to_server(StorageNodeService(), server)
    server.add_insecure_port(f"[::]:{GRPC_SERVER_PORT}")
    server.start()
    print(f"Storage node listening on {GRPC_SERVER_PORT} ({NODE_TARGET})")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
