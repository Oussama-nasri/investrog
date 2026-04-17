# FinInsight RAG: Personalized Investment Research Assistant

FinInsight RAG is a high-performance Retrieval-Augmented Generation system designed to help investors navigate market volatility by synthesizing vast amounts of unstructured financial data. 

## 🚀 Overview
The system provides grounded, citation-backed insights to support data-driven investment decisions by combining real-time news feeds, analyst reports, and historical market commentary.

## ✨ Key Features
- **Multi-Source Ingestion:** Seamlessly indexes RSS news feeds, PDF research reports, and financial transcripts.
- **Advanced RAG Pipeline:** Utilizes semantic search with a secondary re-ranking stage to ensure maximum precision.
- **Verified Citations:** Every insight is linked back to its original source document for manual verification.
- **Sentiment Contextualization:** Identifies shifting sentiments in analyst commentary over time.

## 🛠 Tech Stack
- **Language:** Python
- **Orchestration:** LangChain / LlamaIndex
- **Vector Database:** Qdrant
- **API Framework:** FastAPI
- **Containerization:** Docker

## 🛠 Setup
1. **Clone the repository:**
   ```bash
   git clone [https://github.com/Oussama-nasri/investrog](https://github.com/Oussama-nasri/investrog.git)
   



investrog/
├── api/                        # FastAPI endpoints
│   ├── main.py                 # App entry point
│   └── routes.py               # endpoints: /query, /ingest/trigger
├── ingestion/                  # LAYER 1: The Robust Ingestion Flow
│   ├── cdc_tracker.py          # SQLAlchemy logic to check file hashes/deltas
│   ├── extractors.py           # Unstructured.io logic for PDFs, HTML, APIs
│   ├── normalizers.py          # Regex/spaCy for stripping disclaimers & NER
│   ├── chunkers.py             # LlamaIndex Hierarchical & Semantic splitters
│   ├── embedders.py            # Sentence-Transformers (Dense + BM25 Sparse)
│   └── tasks.py                # Celery async tasks for the ingestion queue
├── agentic_rag/                # LAYER 2: Retrieval & Generation
│   ├── graph_builder.py        # LangGraph state machine (Agentic reasoning loop)
│   ├── tools.py                # LangChain/LlamaIndex retriever tools (for the agent)
│   ├── reranker.py             # Cross-encoder re-ranking logic
│   └── prompts.py              # Financial analyst system prompts
├── database/                   # LAYER 3: Infrastructure connectors
│   ├── postgres_client.py      # Relational DB connection (Metadata & CDC)
│   └── qdrant_client.py        # Vector DB connection (Indexes & Hybrid Search)
├── evaluation/                 # LAYER 4: Metrics & Observability
│   ├── ragas_evaluator.py      # Ragas for MRR, nDCG, and Faithfulness scoring
│   └── logger.py               # System observability (latency, cost mapping)
├── requirements.txt
└── docker-compose.yml          # Postgres, Qdrant, Redis, Celery workers