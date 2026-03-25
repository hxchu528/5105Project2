from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Context(_message.Message):
    __slots__ = ("doc_type", "doc_name", "doc_locator")
    DOC_TYPE_FIELD_NUMBER: _ClassVar[int]
    DOC_NAME_FIELD_NUMBER: _ClassVar[int]
    DOC_LOCATOR_FIELD_NUMBER: _ClassVar[int]
    doc_type: str
    doc_name: str
    doc_locator: str
    def __init__(self, doc_type: _Optional[str] = ..., doc_name: _Optional[str] = ..., doc_locator: _Optional[str] = ...) -> None: ...

class Record(_message.Message):
    __slots__ = ("id", "text", "context", "embedding")
    ID_FIELD_NUMBER: _ClassVar[int]
    TEXT_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    EMBEDDING_FIELD_NUMBER: _ClassVar[int]
    id: str
    text: str
    context: Context
    embedding: _containers.RepeatedScalarFieldContainer[float]
    def __init__(self, id: _Optional[str] = ..., text: _Optional[str] = ..., context: _Optional[_Union[Context, _Mapping]] = ..., embedding: _Optional[_Iterable[float]] = ...) -> None: ...

class Centroid(_message.Message):
    __slots__ = ("values",)
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedScalarFieldContainer[float]
    def __init__(self, values: _Optional[_Iterable[float]] = ...) -> None: ...

class SearchHit(_message.Message):
    __slots__ = ("id", "text", "context", "score")
    ID_FIELD_NUMBER: _ClassVar[int]
    TEXT_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    id: str
    text: str
    context: Context
    score: float
    def __init__(self, id: _Optional[str] = ..., text: _Optional[str] = ..., context: _Optional[_Union[Context, _Mapping]] = ..., score: _Optional[float] = ...) -> None: ...

class NodeStats(_message.Message):
    __slots__ = ("vector_count", "mean_score", "stdv_score")
    VECTOR_COUNT_FIELD_NUMBER: _ClassVar[int]
    MEAN_SCORE_FIELD_NUMBER: _ClassVar[int]
    STDV_SCORE_FIELD_NUMBER: _ClassVar[int]
    vector_count: int
    mean_score: float
    stdv_score: float
    def __init__(self, vector_count: _Optional[int] = ..., mean_score: _Optional[float] = ..., stdv_score: _Optional[float] = ...) -> None: ...

class NodeInfo(_message.Message):
    __slots__ = ("target", "stats")
    TARGET_FIELD_NUMBER: _ClassVar[int]
    STATS_FIELD_NUMBER: _ClassVar[int]
    target: str
    stats: NodeStats
    def __init__(self, target: _Optional[str] = ..., stats: _Optional[_Union[NodeStats, _Mapping]] = ...) -> None: ...

class PutRequest(_message.Message):
    __slots__ = ("record",)
    RECORD_FIELD_NUMBER: _ClassVar[int]
    record: Record
    def __init__(self, record: _Optional[_Union[Record, _Mapping]] = ...) -> None: ...

class PutResponse(_message.Message):
    __slots__ = ("ok", "target", "target_count", "split_triggered")
    OK_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    TARGET_COUNT_FIELD_NUMBER: _ClassVar[int]
    SPLIT_TRIGGERED_FIELD_NUMBER: _ClassVar[int]
    ok: bool
    target: str
    target_count: int
    split_triggered: bool
    def __init__(self, ok: bool = ..., target: _Optional[str] = ..., target_count: _Optional[int] = ..., split_triggered: bool = ...) -> None: ...

class SearchRequest(_message.Message):
    __slots__ = ("embedding",)
    EMBEDDING_FIELD_NUMBER: _ClassVar[int]
    embedding: _containers.RepeatedScalarFieldContainer[float]
    def __init__(self, embedding: _Optional[_Iterable[float]] = ...) -> None: ...

class ClusterStatusRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ClusterStatusResponse(_message.Message):
    __slots__ = ("nodes",)
    NODES_FIELD_NUMBER: _ClassVar[int]
    nodes: _containers.RepeatedCompositeFieldContainer[NodeInfo]
    def __init__(self, nodes: _Optional[_Iterable[_Union[NodeInfo, _Mapping]]] = ...) -> None: ...

class StoreRecordRequest(_message.Message):
    __slots__ = ("record",)
    RECORD_FIELD_NUMBER: _ClassVar[int]
    record: Record
    def __init__(self, record: _Optional[_Union[Record, _Mapping]] = ...) -> None: ...

class StoreRecordResponse(_message.Message):
    __slots__ = ("ok", "target", "centroid", "count")
    OK_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    CENTROID_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    ok: bool
    target: str
    centroid: Centroid
    count: int
    def __init__(self, ok: bool = ..., target: _Optional[str] = ..., centroid: _Optional[_Union[Centroid, _Mapping]] = ..., count: _Optional[int] = ...) -> None: ...

class SearchLocalRequest(_message.Message):
    __slots__ = ("query_embedding", "top_k")
    QUERY_EMBEDDING_FIELD_NUMBER: _ClassVar[int]
    TOP_K_FIELD_NUMBER: _ClassVar[int]
    query_embedding: _containers.RepeatedScalarFieldContainer[float]
    top_k: int
    def __init__(self, query_embedding: _Optional[_Iterable[float]] = ..., top_k: _Optional[int] = ...) -> None: ...

class SearchLocalResponse(_message.Message):
    __slots__ = ("hits", "target", "vectors_searched")
    HITS_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    VECTORS_SEARCHED_FIELD_NUMBER: _ClassVar[int]
    hits: _containers.RepeatedCompositeFieldContainer[SearchHit]
    target: str
    vectors_searched: int
    def __init__(self, hits: _Optional[_Iterable[_Union[SearchHit, _Mapping]]] = ..., target: _Optional[str] = ..., vectors_searched: _Optional[int] = ...) -> None: ...

class ReplaceLocalPartitionRequest(_message.Message):
    __slots__ = ("records", "centroid")
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    CENTROID_FIELD_NUMBER: _ClassVar[int]
    records: _containers.RepeatedCompositeFieldContainer[Record]
    centroid: Centroid
    def __init__(self, records: _Optional[_Iterable[_Union[Record, _Mapping]]] = ..., centroid: _Optional[_Union[Centroid, _Mapping]] = ...) -> None: ...

class ReplaceLocalPartitionResponse(_message.Message):
    __slots__ = ("ok", "target", "count")
    OK_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    ok: bool
    target: str
    count: int
    def __init__(self, ok: bool = ..., target: _Optional[str] = ..., count: _Optional[int] = ...) -> None: ...

class SplitPartitionRequest(_message.Message):
    __slots__ = ("new_node_target",)
    NEW_NODE_TARGET_FIELD_NUMBER: _ClassVar[int]
    new_node_target: str
    def __init__(self, new_node_target: _Optional[str] = ...) -> None: ...

class SplitPartitionResponse(_message.Message):
    __slots__ = ("ok", "old_target", "old_centroid", "old_count", "new_target", "new_centroid", "new_count")
    OK_FIELD_NUMBER: _ClassVar[int]
    OLD_TARGET_FIELD_NUMBER: _ClassVar[int]
    OLD_CENTROID_FIELD_NUMBER: _ClassVar[int]
    OLD_COUNT_FIELD_NUMBER: _ClassVar[int]
    NEW_TARGET_FIELD_NUMBER: _ClassVar[int]
    NEW_CENTROID_FIELD_NUMBER: _ClassVar[int]
    NEW_COUNT_FIELD_NUMBER: _ClassVar[int]
    ok: bool
    old_target: str
    old_centroid: Centroid
    old_count: int
    new_target: str
    new_centroid: Centroid
    new_count: int
    def __init__(self, ok: bool = ..., old_target: _Optional[str] = ..., old_centroid: _Optional[_Union[Centroid, _Mapping]] = ..., old_count: _Optional[int] = ..., new_target: _Optional[str] = ..., new_centroid: _Optional[_Union[Centroid, _Mapping]] = ..., new_count: _Optional[int] = ...) -> None: ...

class GetNodeStatsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
