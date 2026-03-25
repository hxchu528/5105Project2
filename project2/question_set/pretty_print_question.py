import json
import random

def pretty_print_record(rec):
    print("=" * 80)
    print(f"ID: {rec.get('id')}")
    print(f"Question: {rec.get('question')}")
    print(f"Topic: {rec.get('topic')}")
    print(f"Source: {rec.get('source_bucket')}")

    # embedding (hide values)
    emb = rec.get("embedding", [])
    print(f"\nEmbedding: [{len(emb)} dims] ...")

    # top5 neighbors
    print("\nTop 5 Neighbors:")
    for i, n in enumerate(rec.get("top5", []), 1):
        print(f"\n  #{i}")
        print(f"    ID: {n.get('id')}")
        print(f"    Score: {n.get('score'):.4f}")
        
        ctx = n.get("context", {})
        print(f"    Context:")
        print(f"      type: {ctx.get('doc_type')}")
        print(f"      name: {ctx.get('doc_name')}")
        print(f"      loc:  {ctx.get('doc_locator')}")

        # optional text preview
        text = n.get("text", "")
        if text:
            preview = text[:120].replace("\n", " ")
            print(f"    Text: {preview}...")

    print("=" * 80)

def example_print_random():

    with open("questions_scored.jsonl", "r") as f:
        records = [json.loads(line) for line in f]

    rec = random.choice(records)
    pretty_print_record(rec)

if __name__ == "__main__":
    example_print_random()