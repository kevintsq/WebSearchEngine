from sentence_transformers import SentenceTransformer, CrossEncoder, util
import torch
from typing import List, Dict

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

#We use the Bi-Encoder to encode all passages, so that we can use it with sematic search
bi_encoder = SentenceTransformer('multi-qa-MiniLM-L6-cos-v1', device=device)
bi_encoder.max_seq_length = 256     #Truncate long passages to 256 tokens
top_k = 32                          #Number of passages we want to retrieve with the bi-encoder

#The bi-encoder will retrieve 100 documents. We use a cross-encoder, to re-rank the results list to improve the quality
cross_encoder = CrossEncoder('cross-encoder/ms-marco-MiniLM-L-6-v2', device=device)

corpus_embeddings = torch.load('corpus_embeddings.pt', map_location=device, mmap=True)


def semantic_search(query: str) -> List[Dict]:
    question_embedding = bi_encoder.encode(query, convert_to_tensor=True, device=device)
    return util.semantic_search(question_embedding, corpus_embeddings, top_k=top_k)[0]


def rerank_inplace(query_doc_pairs: List[List[str]], results: List[Dict]):
    cross_scores = cross_encoder.predict(query_doc_pairs)
    for result, cross_score in zip(results, cross_scores):
        result['score'] = cross_score
    results.sort(key=lambda x: x['score'], reverse=True)
