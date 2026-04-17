import os
import glob
from cdc_tracker import CDCTracker
from normalizer import DocumentNormalizer
from vector_indexer import VectorIndexer


class IngestionPipeline:
    def __init__(self, data_dir="./data"):
        self.data_dir = data_dir
        self.tracker = CDCTracker()
        self.normalizer = DocumentNormalizer()
        self.indexer = VectorIndexer()

        # Ensure data directory exists
        os.makedirs(self.data_dir, exist_ok=True)

    def extract_metadata_mock(self, file_path: str) -> dict:
        """
        Mock function for metadata extraction.
        In production, use spaCy/Regex here to find Tickers, Dates, etc.
        """
        filename = os.path.basename(file_path)
        return {
            "source_file": filename,
            "document_type": "report" if "report" in filename.lower() else "unknown",
            "ingestion_date": "2026-04-18"
        }

    def run(self):
        print(f"Starting ingestion pipeline for directory: {self.data_dir}")
        # Find all PDF and TXT files in the directory
        files = glob.glob(os.path.join(self.data_dir, "*.*"))

        if not files:
            print("No files found to process.")
            return

        for file_path in files:
            # 1. CDC Check: Do we actually need to process this?
            if not self.tracker.needs_processing(file_path):
                print(f"⏭️ Skipping {os.path.basename(file_path)} (Unchanged)")
                continue

            print(f"🔄 Processing {os.path.basename(file_path)}...")

            try:
                # 2. Extract and Normalize
                clean_text = self.normalizer.extract_text(file_path)

                # 3. Extract Metadata
                metadata = self.extract_metadata_mock(file_path)

                # 4. Chunk, Embed, and Index
                self.indexer.index_document(clean_text, metadata)

                # 5. Mark as successful in CDC Tracker
                self.tracker.mark_as_processed(file_path)
                print(f"✅ Successfully ingested {os.path.basename(file_path)}")

            except Exception as e:
                print(f"❌ Error processing {os.path.basename(file_path)}: {str(e)}")


if __name__ == "__main__":
    # To test this:
    # 1. Create a folder named 'data' in the same directory as this script.
    # 2. Put some sample .txt or .pdf files in the 'data' folder.
    # 3. Run this script. Run it twice to see the CDC skip logic in action.
    pipeline = IngestionPipeline()
    pipeline.run()