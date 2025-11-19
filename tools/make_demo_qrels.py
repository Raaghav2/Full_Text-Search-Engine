#!/usr/bin/env python3
import random
from pathlib import Path

def parse_run_file(run_path):
    """Parse a TREC run file to extract retrieved documents per topic"""
    topic_docs = {}
    with open(run_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) >= 6:
                qid, _, docno, rank, score, tag = parts[0], parts[1], parts[2], int(parts[3]), parts[4], parts[5]
                topic_docs.setdefault(qid, []).append(docno)
    return topic_docs

def create_realistic_qrels(topic_docs, out_qrels, relevance_rate=0.25):
    """Create pseudo qrels by marking some retrieved docs as relevant."""
    random.seed(42)  # reproducible
    with open(out_qrels, "w", encoding="utf-8") as f:
        for qid, docs in topic_docs.items():
            num_docs = len(docs)
            num_relevant = max(1, int(num_docs * relevance_rate))
            relevant_docs = []
            for i, doc in enumerate(docs):
                prob = relevance_rate * (1.0 - (i / num_docs) * 0.5)
                if random.random() < prob and len(relevant_docs) < num_relevant:
                    relevant_docs.append(doc)
            if not relevant_docs:
                relevant_docs = docs[:min(3, len(docs))]
            for docno in relevant_docs:
                f.write(f"{qid} 0 {docno} 1\n")

if __name__ == "__main__":
    run_path = Path("runs/student.run")
    out_qrels = Path("out/qrels.demo.txt")
    out_qrels.parent.mkdir(parents=True, exist_ok=True)

    topic_docs = parse_run_file(run_path)
    print(f"Found {len(topic_docs)} topics in run file")
    create_realistic_qrels(topic_docs, out_qrels, 0.25)
    print(f"Qrels written to: {out_qrels}")
