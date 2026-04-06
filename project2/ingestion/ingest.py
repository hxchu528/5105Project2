import grpc
from numpy import full
import project2_pb2
import json
import project2_pb2_grpc
from utils.utils import corpus_line_to_record
from pathlib import Path
import os

CONTROLLER_HOST = os.environ.get("CONTROLLER_HOST", "host.docker.internal")
# CONTROLLER_HOST = f"172.18.0.1"
CONTROLLER_PORT = os.environ.get("CONTROLLER_PORT", 50050)
CONTROLLER_TARGET = f"{CONTROLLER_HOST}:{CONTROLLER_PORT}"

WORKSPACE_FOLDER = os.environ.get("WORKSPACE_FOLDER", ".")
CORPUS_FOLDER = Path(WORKSPACE_FOLDER, "corpus")

def put_mini_corpus():

    mini_corpus_path = Path(CORPUS_FOLDER, "mini_corpus.jsonl")

    # Parse the mini corpus jsonl into a list of Record objects
    records : list[project2_pb2.Record] = []
    with open(mini_corpus_path, "r") as f:
        for line in f:
            records.append(corpus_line_to_record(line))

    # Connect to the controller
    with grpc.insecure_channel(CONTROLLER_TARGET) as channel:

        # Create stub
        stub = project2_pb2_grpc.ControllerServiceStub(channel)

        # Iterate over records and Put into database
        for i, record in enumerate(records, start=1):

            response = stub.Put(project2_pb2.PutRequest(record=record))

            print(
                f"put {i}: target={response.target} "
                f"count={response.target_count} split_triggered={response.split_triggered}"
            )

def put_full_corpus():
    """TODO: Implement this function... recommended to Put one at at time with full corups to avoid reading whole file"""

    full_corpus_path = Path(CORPUS_FOLDER, "full_corpus_shuffled.jsonl")


    with grpc.insecure_channel(CONTROLLER_TARGET) as channel:

        # Create stub
        stub = project2_pb2_grpc.ControllerServiceStub(channel)

        with open(full_corpus_path, "r") as f:
            count = 0
            for line in f:
                # if count > 150:
                #     break

                record = corpus_line_to_record(line)
                response = stub.Put(project2_pb2.PutRequest(record=record))

                print(
                    f"put {count}: target={response.target} "
                    f"count={response.target_count} split_triggered={response.split_triggered}"
                )
                
                count += 1

def put_full_corpus_source_type():
    """TODO: Implement this function... recommended to Put one at at time with full corups to avoid reading whole file"""

    course_papers_path = Path(CORPUS_FOLDER, "course_papers")
    lecture_slides_path = Path(CORPUS_FOLDER, "lecture_slides")
    lecture_transcripts_path = Path(CORPUS_FOLDER, "lecture_transcripts")
    textbook_path = Path(CORPUS_FOLDER, "textbook")

    all_files = (
    list(course_papers_path.glob("*")) +
    list(lecture_slides_path.glob("*")) +
    list(lecture_transcripts_path.glob("*")) +
    list(textbook_path.glob("*")))

    with grpc.insecure_channel(CONTROLLER_TARGET) as channel:

        # Create stub
        stub = project2_pb2_grpc.ControllerServiceStub(channel)

        count = 0
        for file in all_files:
            with open(file, "r") as f:
                count = 0
                for line in f:
                    # if count > 150:
                    #     break

                    record = corpus_line_to_record(line)
                    response = stub.Put(project2_pb2.PutRequest(record=record))

                    print(
                        f"put {count}: target={response.target} "
                        f"count={response.target_count} split_triggered={response.split_triggered}"
                    )
                    
                    count += 1

        return count


def main():

    # put_mini_corpus()
    # put_full_corpus()
    put_full_corpus_source_type()


if __name__ == "__main__":
    main()
