import re
from unstructured.partition.auto import partition


class DocumentNormalizer:
    def extract_text(self, file_path: str) -> str:
        """Extracts text from PDFs, HTML, or TXT using unstructured."""
        elements = partition(filename=file_path)
        raw_text = "\n\n".join([str(el) for el in elements])
        return self.normalize_text(raw_text)

    def normalize_text(self, text: str) -> str:
        """Cleans and standardizes the financial text."""
        # Remove extra whitespace and newlines
        text = re.sub(r'\s+', ' ', text)

        # Strip common financial boilerplate (example)
        disclaimer_pattern = r"(?i)(Safe Harbor Statement|Forward-Looking Statements).*?(?=\n\n|\Z)"
        text = re.sub(disclaimer_pattern, '', text)

        # Normalize financial units (e.g., $1B -> 1 billion USD) - simplified example
        text = re.sub(r'\$([0-9\.]+)[Bb]', r'\1 billion USD', text)
        text = re.sub(r'\$([0-9\.]+)[Mm]', r'\1 million USD', text)

        return text.strip()