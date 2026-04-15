"""
Text chunking utility for processing large regulatory documents with LLMs
Intelligently splits text while preserving context and structure
"""

import re
from typing import List, Dict, Tuple


class TextChunker:
    """
    Intelligently chunk large text documents for LLM processing
    Preserves document structure and context across chunks
    """

    def __init__(
            self,
            max_chunk_size: int = 15000,  # characters per chunk
            overlap: int = 500,  # overlap between chunks
            preserve_structure: bool = True
    ):
        """
        Args:
            max_chunk_size: Maximum characters per chunk (for ~4000 tokens use 15000)
            overlap: Number of characters to overlap between chunks for context
            preserve_structure: Try to split at natural boundaries (paragraphs, sections)
        """
        self.max_chunk_size = max_chunk_size
        self.overlap = overlap
        self.preserve_structure = preserve_structure

    def chunk_text(self, text: str, metadata: Dict = None) -> List[Dict]:
        """
        Split text into overlapping chunks

        Args:
            text: The full text to chunk
            metadata: Optional metadata to include with each chunk

        Returns:
            List of dictionaries with 'text', 'chunk_num', 'total_chunks', and metadata
        """
        if not text:
            return []

        if len(text) <= self.max_chunk_size:
            # Document fits in one chunk
            return [{
                'text': text,
                'chunk_num': 1,
                'total_chunks': 1,
                'char_start': 0,
                'char_end': len(text),
                **(metadata or {})
            }]

        chunks = []

        if self.preserve_structure:
            chunks = self._chunk_by_structure(text)
        else:
            chunks = self._chunk_by_size(text)

        # Add metadata to each chunk
        total_chunks = len(chunks)
        for i, chunk_text in enumerate(chunks):
            chunk_dict = {
                'text': chunk_text,
                'chunk_num': i + 1,
                'total_chunks': total_chunks,
                'char_start': text.find(chunk_text),
                'char_end': text.find(chunk_text) + len(chunk_text),
                **(metadata or {})
            }
            chunks[i] = chunk_dict

        return chunks

    def _chunk_by_structure(self, text: str) -> List[str]:
        """
        Chunk text by preserving structure (sections, articles, paragraphs)
        """
        chunks = []
        current_chunk = ""

        # Split by major sections (e.g., "Chapter 1:", "Article 1", etc.)
        # Common in regulatory documents
        sections = self._split_by_sections(text)

        for section in sections:
            # If adding this section exceeds max size and we have content
            if len(current_chunk) + len(section) > self.max_chunk_size and current_chunk:
                # Save current chunk
                chunks.append(current_chunk.strip())

                # Start new chunk with overlap from previous
                if self.overlap > 0:
                    overlap_text = current_chunk[-self.overlap:]
                    current_chunk = overlap_text + "\n\n" + section
                else:
                    current_chunk = section
            else:
                # Add section to current chunk
                if current_chunk:
                    current_chunk += "\n\n" + section
                else:
                    current_chunk = section

        # Add the last chunk
        if current_chunk:
            chunks.append(current_chunk.strip())

        return chunks

    def _chunk_by_size(self, text: str) -> List[str]:
        """
        Simple chunking by character count with overlap
        """
        chunks = []
        start = 0

        while start < len(text):
            # Calculate end position
            end = min(start + self.max_chunk_size, len(text))

            # If not at the end, try to break at a paragraph or sentence
            if end < len(text):
                # Look for paragraph break
                last_para = text.rfind('\n\n', start, end)
                if last_para > start + self.max_chunk_size // 2:  # At least halfway
                    end = last_para
                else:
                    # Look for sentence break
                    last_period = max(
                        text.rfind('. ', start, end),
                        text.rfind('.\n', start, end),
                        text.rfind('؟', start, end),  # Arabic question mark
                        text.rfind('。', start, end),  # Other punctuation
                    )
                    if last_period > start + self.max_chunk_size // 2:
                        end = last_period + 1

            chunks.append(text[start:end].strip())

            # Move start position with overlap
            start = end - self.overlap if end < len(text) else end

            # Avoid getting stuck
            if start >= len(text):
                break

        return chunks

    def _split_by_sections(self, text: str) -> List[str]:
        """
        Split text by major sections (Chapter, Article, Section headers)
        """
        # Patterns for common regulatory document structure
        patterns = [
            r'\n(?=Chapter\s+\d+:)',  # "Chapter 1:"
            r'\n(?=Article\s+\d+)',  # "Article 1"
            r'\n(?=Section\s+\d+)',  # "Section 1"
            r'\n(?=CHAPTER\s+[IVXLCDM]+)',  # "CHAPTER I"
            r'\n(?=Part\s+[A-Z0-9]+)',  # "Part A"
            r'\n(?=الفصل\s+)',  # Arabic "Chapter"
            r'\n(?=المادة\s+)',  # Arabic "Article"
        ]

        # Combine all patterns
        combined_pattern = '|'.join(patterns)

        # Split text
        sections = re.split(combined_pattern, text)

        # Remove empty sections
        sections = [s.strip() for s in sections if s.strip()]

        # If no sections found, split by paragraphs
        if len(sections) <= 1:
            sections = [p for p in text.split('\n\n') if p.strip()]

        return sections

    def estimate_tokens(self, text: str) -> int:
        """
        Rough estimate of token count (1 token ≈ 4 characters for English/Arabic)
        """
        return len(text) // 4

    def get_chunk_statistics(self, chunks: List[Dict]) -> Dict:
        """
        Get statistics about the chunked text
        """
        if not chunks:
            return {
                'total_chunks': 0,
                'total_characters': 0,
                'estimated_tokens': 0,
                'avg_chunk_size': 0,
                'min_chunk_size': 0,
                'max_chunk_size': 0
            }

        chunk_sizes = [len(c['text']) for c in chunks]
        total_chars = sum(chunk_sizes)

        return {
            'total_chunks': len(chunks),
            'total_characters': total_chars,
            'estimated_tokens': self.estimate_tokens(''.join(c['text'] for c in chunks)),
            'avg_chunk_size': total_chars // len(chunks),
            'min_chunk_size': min(chunk_sizes),
            'max_chunk_size': max(chunk_sizes)
        }


def create_chunk_context(chunk: Dict, document_title: str = None) -> str:
    """
    Create a context string to prepend to each chunk for LLM processing
    Helps the LLM understand this is part of a larger document
    """
    context_parts = []

    if document_title:
        context_parts.append(f"Document: {document_title}")

    context_parts.append(f"[Chunk {chunk['chunk_num']} of {chunk['total_chunks']}]")

    if chunk['chunk_num'] > 1:
        context_parts.append("(Continuation of previous section)")

    if chunk['chunk_num'] < chunk['total_chunks']:
        context_parts.append("(Continues in next section)")

    context = " | ".join(context_parts)

    return f"\n{'=' * 70}\n{context}\n{'=' * 70}\n\n{chunk['text']}"


# Example usage
if __name__ == "__main__":
    # Example with a sample text
    sample_text = """
Chapter 1: Introduction

This is the first chapter with some content.
It has multiple paragraphs and sections.

Article 1: Definitions

Here are some definitions that span multiple lines.
These definitions are important for understanding the rest.

Article 2: Objectives

The objectives are listed here in detail.
There are several key points to consider.

Chapter 2: Implementation

This chapter describes implementation details.
It contains technical specifications and requirements.
""" * 10  # Repeat to make it longer

    # Create chunker
    chunker = TextChunker(max_chunk_size=500, overlap=100)

    # Chunk the text
    chunks = chunker.chunk_text(
        sample_text,
        metadata={'document_id': 'REG-001', 'title': 'Sample Regulation'}
    )

    # Print statistics
    stats = chunker.get_chunk_statistics(chunks)
    print(f"Total chunks: {stats['total_chunks']}")
    print(f"Average chunk size: {stats['avg_chunk_size']} chars")
    print(f"Estimated total tokens: {stats['estimated_tokens']}")

    # Print first chunk with context
    print("\n" + "=" * 70)
    print("FIRST CHUNK WITH CONTEXT:")
    print("=" * 70)
    print(create_chunk_context(chunks[0], "Sample Regulation"))