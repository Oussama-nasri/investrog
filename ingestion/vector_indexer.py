import os

from llama_index.core import Document, VectorStoreIndex, StorageContext
from llama_index.core.node_parser import SentenceSplitter
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.vector_stores.qdrant import QdrantVectorStore
from database.qdrant_client import QdrantDBClient



class VectorIndexer:
    def __init__(self):
        self.qdrant_db = QdrantDBClient()
        self.qdrant_db.ensure_collection_exists(vector_size=384)
        self.vector_store = QdrantVectorStore(
            client=self.qdrant_db.get_client(),
            collection_name=self.qdrant_db.collection_name
        )
        self.storage_context = StorageContext.from_defaults(vector_store=self.vector_store)

        # Use a fast, local embedding model suitable for finance/general text
        self.embed_model = HuggingFaceEmbedding(model_name="BAAI/bge-small-en-v1.5")

        # Setup context-aware chunking (semantic sentence splitting)
        self.node_parser = SentenceSplitter(chunk_size=512, chunk_overlap=50)

    def index_document(self, text: str, metadata: dict):
        """Chunks, embeds, and indexes a document."""
        # Create a LlamaIndex document with attached metadata
        doc = Document(text=text, metadata=metadata)

        # Extract nodes (chunks)
        nodes = self.node_parser.get_nodes_from_documents([doc])

        # Generate embeddings and store in Qdrant
        # In LlamaIndex v0.10+, we build the index directly with the nodes
        VectorStoreIndex(
            nodes,
            storage_context=self.storage_context,
            embed_model=self.embed_model,
            show_progress=True
        )