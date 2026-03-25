import json
import statistics
from pathlib import Path
import os

import grpc
from project2_pb2 import *
import project2_pb2_grpc
from utils.config import CONTROLLER_TARGET
from utils.utils import corpus_line_to_record


CORPUS_FILE = Path(os.environ["WORKSPACE_FOLDER"], "corpus", "full_corpus_shuffled.jsonl")
QUESTIONS_FILE = Path(os.environ["WORKSPACE_FOLDER"], "question_set", "questions_scored.jsonl")


def ingest_full_corpus(corpus_path: Path) -> int:
    total_vectors: int = 0

    with grpc.insecure_channel(CONTROLLER_TARGET) as channel:
        stub = project2_pb2_grpc.ControllerServiceStub(channel)

        with open(corpus_path, "r", encoding="utf-8") as file:
            for total_vectors, line in enumerate(file, start=1):
                record: Record = corpus_line_to_record(line)
                response: PutResponse = stub.Put(PutRequest(record=record))

                if total_vectors % 250 == 0:
                    print(
                        f"ingested {total_vectors} records "
                        f"(last target={response.target}, split_triggered={response.split_triggered})"
                    )

    print(f"Finished ingesting {total_vectors} total records.")
    return total_vectors


def load_questions(path: Path) -> list[dict]:
    with open(path, "r", encoding="utf-8") as file:
        return [json.loads(line) for line in file]


def mean_score_from_hits(hits: list[SearchHit]) -> float:
    if not hits:
        return 0.0
    return statistics.mean(hit.score for hit in hits)


def mean_oracle_score(question: dict) -> float:
    top5: list[dict] = question.get("top5", [])
    if not top5:
        return 0.0
    return statistics.mean(item["score"] for item in top5)


def search_fraction(vectors_searched: int, total_vectors: int) -> float:
    if total_vectors <= 0:
        return 1.0
    return vectors_searched / total_vectors


def score_accuracy(returned_mean_score: float, oracle_mean_score: float) -> float:
    if oracle_mean_score <= 0:
        return 0.0
    return min(1.0, returned_mean_score / oracle_mean_score)


def efficiency_score(
    returned_mean_score: float,
    oracle_mean_score_value: float,
    vectors_searched: int,
    total_vectors: int,
) -> float:
    fraction: float = search_fraction(vectors_searched, total_vectors)
    accuracy: float = score_accuracy(returned_mean_score, oracle_mean_score_value)

    if fraction <= 0:
        return 0.0
    return accuracy / fraction


def evaluate_all_questions(questions: list[dict], total_vectors: int) -> list[dict]:
    results: list[dict] = []

    with grpc.insecure_channel(CONTROLLER_TARGET) as channel:
        stub = project2_pb2_grpc.ControllerServiceStub(channel)

        for index, question in enumerate(questions, start=1):
            response: SearchLocalResponse = stub.Search(
                SearchRequest(embedding=question["embedding"])
            )

            returned_mean: float = mean_score_from_hits(list(response.hits))
            oracle_mean: float = mean_oracle_score(question)
            eff_score: float = efficiency_score(
                returned_mean,
                oracle_mean,
                response.vectors_searched,
                total_vectors,
            )

            expected_ids: set[str] = {item["id"] for item in question.get("top5", [])}
            actual_ids: set[str] = {hit.id for hit in response.hits}
            hit_rate: float = (
                len(expected_ids & actual_ids) / len(expected_ids)
                if expected_ids
                else 0.0
            )

            result: dict[str, float | int | str] = {
                "question_id": question["id"],
                "question": question["question"],
                "oracle_mean_score": oracle_mean,
                "returned_mean_score": returned_mean,
                "score_accuracy": score_accuracy(returned_mean, oracle_mean),
                "vectors_searched": response.vectors_searched,
                "search_fraction": search_fraction(response.vectors_searched, total_vectors),
                "hit_rate": hit_rate,
                "efficiency_score": eff_score,
                "target": response.target,
            }
            results.append(result)

            print(
                f"[{index}/{len(questions)}] "
                f"id={question['id']} "
                f"target={response.target} "
                f"vectors={response.vectors_searched}/{total_vectors} "
                f"score={eff_score:.4f}"
            )

    return results


def print_summary(results: list[dict], total_vectors: int) -> None:
    overall_score: float = statistics.mean(float(r["efficiency_score"]) for r in results) if results else 0.0
    avg_accuracy: float = statistics.mean(float(r["score_accuracy"]) for r in results) if results else 0.0
    avg_fraction: float = statistics.mean(float(r["search_fraction"]) for r in results) if results else 0.0
    avg_vectors: float = statistics.mean(float(r["vectors_searched"]) for r in results) if results else 0.0
    avg_hit_rate: float = statistics.mean(float(r["hit_rate"]) for r in results) if results else 0.0
    avg_returned_mean: float = statistics.mean(float(r["returned_mean_score"]) for r in results) if results else 0.0
    avg_oracle_mean: float = statistics.mean(float(r["oracle_mean_score"]) for r in results) if results else 0.0

    print("\n" + "=" * 80)
    print("NORMALIZED SCORING SUMMARY")
    print("=" * 80)
    print(f"Questions evaluated:      {len(results)}")
    print(f"Total corpus vectors:     {total_vectors}")
    print(f"Average hit rate:         {avg_hit_rate:.4f}")
    print(f"Average returned score:   {avg_returned_mean:.4f}")
    print(f"Average oracle score:     {avg_oracle_mean:.4f}")
    print(f"Average score accuracy:   {avg_accuracy:.4f}")
    print(f"Average vectors searched: {avg_vectors:.2f}")
    print(f"Average search fraction:  {avg_fraction:.4f}")
    print(f"FINAL TOTAL SCORE:        {overall_score:.4f}")
    print("=" * 80)
    print("Per-question metric:")
    print("  efficiency_score(q) = score_accuracy(q) / search_fraction(q)")
    print("Final total score:")
    print("  average efficiency_score(q) across all questions")
    print("=" * 80)


def main() -> None:
    print("Step 1: ingesting full corpus")
    total_vectors: int = ingest_full_corpus(CORPUS_FILE)

    print("\nStep 2: loading all scored questions")
    questions: list[dict] = load_questions(QUESTIONS_FILE)

    print("Step 3: evaluating all questions")
    results: list[dict] = evaluate_all_questions(questions, total_vectors)

    print_summary(results, total_vectors)


if __name__ == "__main__":
    main()
