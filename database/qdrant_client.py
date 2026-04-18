import os
from dotenv import load_dotenv
import qdrant_client
from qdrant_client.http.models import VectorParams, Distance

load_dotenv()
class QdrantDBClient:
    def __init__(self):
        self.url = os.getenv("QDRANT_URL", "http://localhost:6333")
        self.api_key = os.getenv("QDRANT_API_KEY", None)
        self.collection_name = os.getenv("QDRANT_COLLECTION_NAME", "financial-docs")

        # Initialize the base client
        self.client = qdrant_client.QdrantClient(
            url=self.url,
            api_key=self.api_key
        )

    def get_client(self) -> qdrant_client.QdrantClient:
        """Returns the raw Qdrant client object."""
        return self.client

    def ensure_collection_exists(self, vector_size: int = 384):
        """
        Validates if the collection exists. If not, creates it.
        Note: vector_size=384 is for 'bge-small-en-v1.5'.
        If you use a larger model like OpenAI's (1536) or bge-large (1024), change this!
        """
        collections_response = self.client.get_collections()
        existing_collections = [col.name for col in collections_response.collections]

        if self.collection_name not in existing_collections:
            print(f"Creating Qdrant collection: '{self.collection_name}'...")
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=VectorParams(
                    size=vector_size,
                    distance=Distance.COSINE  # Cosine similarity is standard for text embeddings
                )
            )
            print("✅ Collection created successfully.")
        else:
            print(f"✅ Qdrant collection '{self.collection_name}' ready.")