import json
import time

import docker
import docker.errors
import grpc
import numpy as np
from project2_pb2 import *

from utils.config import NODE_PORT, DOCKER_IMAGE, DOCKER_NETWORK


def cosine_similarity(a: list[float], b: list[float]) -> float:
    vec_a = np.array(a, dtype=float)
    vec_b = np.array(b, dtype=float)
    if vec_a.size == 0 or vec_b.size == 0:
        return 0.0
    denom = np.linalg.norm(vec_a) * np.linalg.norm(vec_b)
    if denom == 0:
        return 0.0
    return float(np.dot(vec_a, vec_b) / denom)


def cosine_distance(a: list[float], b: list[float]) -> float:
    return 1.0 - cosine_similarity(a, b)


def update_centroid(records: list[Record]) -> list[float]:
    if not records:
        return []
    matrix = np.array([list(record.embedding) for record in records], dtype=float)
    return matrix.mean(axis=0).tolist()


def local_top_k(records: list[Record], query_embedding: list[float], top_k: int) -> list[SearchHit]:
    hits: list[SearchHit] = [
        SearchHit(
            id=record.id,
            text=record.text,
            context=record.context,
            score=cosine_similarity(list(record.embedding), query_embedding),
        )
        for record in records
    ]
    hits.sort(key=lambda hit: hit.score, reverse=True)
    return hits[:top_k]


def kmeans_split(
    records: list[Record], max_iters: int = 6
) -> tuple[list[Record], list[Record], list[float], list[float]]:
    if len(records) < 2:
        centroid = update_centroid(records)
        return records, [], centroid, []

    embeddings: list[np.ndarray] = [
        np.array(list(record.embedding), dtype=float) for record in records
    ]
    c1: np.ndarray = embeddings[0].copy()
    c2: np.ndarray = embeddings[-1].copy()

    cluster1: list[Record] = []
    cluster2: list[Record] = []

    for _ in range(max_iters):
        cluster1 = []
        cluster2 = []

        for record, embedding in zip(records, embeddings):
            if cosine_similarity(embedding.tolist(), c1.tolist()) >= cosine_similarity(
                embedding.tolist(), c2.tolist()
            ):
                cluster1.append(record)
            else:
                cluster2.append(record)

        if not cluster1 or not cluster2:
            midpoint = len(records) // 2
            cluster1 = records[:midpoint]
            cluster2 = records[midpoint:]
            break

        c1 = np.mean([list(record.embedding) for record in cluster1], axis=0)
        c2 = np.mean([list(record.embedding) for record in cluster2], axis=0)

    centroid1 = update_centroid(cluster1)
    centroid2 = update_centroid(cluster2)
    return cluster1, cluster2, centroid1, centroid2


def corpus_line_to_record(jsonl_line: str) -> Record:
    obj: dict = json.loads(jsonl_line)
    return Record(
        id=obj["id"],
        text=obj["text"],
        context=Context(**obj["context"]),
        embedding=obj["embedding"],
    )


def choose_closest_node(nodes, embedding: list[float]):
    if len(nodes) == 1:
        return nodes[0]

    with_centroids = [
        node for node in nodes if node["centroid"]
    ]
    if not with_centroids:
        return nodes[0]

    return max(
        with_centroids,
        key=lambda node: cosine_similarity(embedding, list(node["centroid"])),
    )


def wait_for_grpc_target(target: str, retry_seconds: float = 0.5) -> None:
    while True:
        try:
            with grpc.insecure_channel(target) as channel:
                grpc.channel_ready_future(channel).result(timeout=1)
            return
        except grpc.FutureTimeoutError:
            time.sleep(retry_seconds)
        except grpc.RpcError:
            time.sleep(retry_seconds)


def create_storage_node(node_num: int) -> str:
    client = docker.from_env()
    name: str = f"storage-node-{node_num}"
    target: str = f"{name}:{NODE_PORT}"

    try:
        client.containers.get(name).remove(force=True)
    except docker.errors.NotFound:
        pass

    client.containers.run(
        DOCKER_IMAGE,
        name=name,
        hostname=name,
        network=DOCKER_NETWORK,
        detach=True,
        working_dir="/app",
        command=["python", "-u", "storage_node/node.py"],
        environment={
            "GRPC_SERVER_PORT": str(NODE_PORT),
            "NODE_TARGET": target,
            "PYTHONPATH": "/app:/app/proto/src",
        },
    )
    wait_for_grpc_target(target)
    return target
