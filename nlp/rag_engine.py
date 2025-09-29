"""
Local LLM RAG Engine
Retrieval Augmented Generation with strict citation requirements
Uses quantized models for offline operation
"""

import json
import logging
import re
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from datetime import datetime
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
import llama_cpp
from sentence_transformers import SentenceTransformer

# Import our vector retriever
import sys
sys.path.append(str(Path(__file__).parent.parent))
from vector.retriever import VectorRetriever

logger = logging.getLogger(__name__)


@dataclass 
class RAGResponse:
    """Response from RAG engine with citations"""
    answer: str
    citations: List[Dict]
    confidence: float
    snippets: List[Dict]
    query_metadata: Dict
    
    def to_dict(self) -> Dict:
        return asdict(self)


class LocalLLMEngine:
    """Local LLM wrapper supporting multiple model formats"""
    
    def __init__(self, 
                 model_path: str,
                 model_type: str = "gguf",  # gguf, transformers, gptq
                 device: str = "cpu"):
        self.model_path = Path(model_path)
        self.model_type = model_type
        self.device = device
        
        if model_type == "gguf":
            self._init_llama_cpp()
        elif model_type == "transformers":
            self._init_transformers()
        else:
            raise ValueError(f"Unsupported model type: {model_type}")
    
    def _init_llama_cpp(self):
        """Initialize llama.cpp model (GGUF format)"""
        # Use quantized model for efficiency
        self.model = llama_cpp.Llama(
            model_path=str(self.model_path),
            n_ctx=4096,  # Context window
            n_threads=8,  # CPU threads
            n_gpu_layers=0 if self.device == "cpu" else 35,  # GPU layers if available
            verbose=False
        )
        logger.info(f"Loaded GGUF model from {self.model_path}")
    
    def _init_transformers(self):
        """Initialize HuggingFace transformers model"""
        # 4-bit quantization config for memory efficiency
        quantization_config = BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_compute_dtype=torch.float16,
            bnb_4bit_quant_type="nf4",
            bnb_4bit_use_double_quant=True
        )
        
        self.tokenizer = AutoTokenizer.from_pretrained(str(self.model_path))
        self.model = AutoModelForCausalLM.from_pretrained(
            str(self.model_path),
            quantization_config=quantization_config if self.device != "cpu" else None,
            device_map="auto" if self.device != "cpu" else None,
            torch_dtype=torch.float16 if self.device != "cpu" else torch.float32
        )
        
        if self.device == "cpu":
            self.model = self.model.to("cpu")
        
        logger.info(f"Loaded transformers model from {self.model_path}")
    
    def generate(self, 
                prompt: str,
                max_tokens: int = 512,
                temperature: float = 0.3) -> str:
        """Generate text from prompt"""
        
        if self.model_type == "gguf":
            response = self.model(
                prompt,
                max_tokens=max_tokens,
                temperature=temperature,
                top_p=0.95,
                stop=["</answer>", "\n\n", "Question:"]
            )
            return response['choices'][0]['text'].strip()
        
        else:  # transformers
            inputs = self.tokenizer(prompt, return_tensors="pt")
            
            if self.device != "cpu":
                inputs = {k: v.cuda() for k, v in inputs.items()}
            
            with torch.no_grad():
                outputs = self.model.generate(
                    **inputs,
                    max_new_tokens=max_tokens,
                    temperature=temperature,
                    do_sample=True,
                    top_p=0.95,
                    pad_token_id=self.tokenizer.eos_token_id
                )
            
            response = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
            # Remove the prompt from response
            response = response[len(prompt):].strip()
            
            return response


class RAGEngine:
    """Retrieval Augmented Generation with strict citations"""
    
    def __init__(self,
                 llm_model_path: str = "infra/models/llm/mistral-7b-instruct-v0.2.Q4_K_M.gguf",
                 vector_index_dir: str = "data/indices",
                 model_type: str = "gguf"):
        
        # Initialize vector retriever
        self.retriever = VectorRetriever(index_dir=vector_index_dir)
        
        # Initialize local LLM
        if Path(llm_model_path).exists():
            self.llm = LocalLLMEngine(llm_model_path, model_type)
        else:
            logger.warning(f"LLM model not found at {llm_model_path}, using mock mode")
            self.llm = None
        
        # Load prompt templates
        self.prompts_dir = Path(__file__).parent / "prompts"
        self.prompts_dir.mkdir(exist_ok=True)
        self._load_prompts()
    
    def _load_prompts(self):
        """Load prompt templates"""
        # Default RAG prompt with strict citation requirements
        self.rag_prompt_template = """You are a forensic analysis assistant. Answer the question using ONLY the provided snippets.
For each factual claim, include the source reference in square brackets [source_id].
If information is not in the snippets, say "No data found for this query."

SNIPPETS:
{snippets}

QUESTION: {question}

ANSWER (with citations):"""
        
        # Load custom prompts if available
        rag_prompt_file = self.prompts_dir / "rag_prompt.txt"
        if rag_prompt_file.exists():
            with open(rag_prompt_file, 'r') as f:
                self.rag_prompt_template = f.read()
    
    def query(self,
             question: str,
             case_ids: Optional[List[str]] = None,
             top_k: int = 10,
             require_citations: bool = True) -> RAGResponse:
        """
        Process natural language query with RAG
        
        Args:
            question: Natural language question
            case_ids: Filter to specific cases
            top_k: Number of snippets to retrieve
            require_citations: Enforce citation requirements
            
        Returns:
            RAGResponse with answer and citations
        """
        start_time = datetime.now()
        
        # Step 1: Retrieve relevant snippets
        snippets = self.retriever.retrieve(question, top_k, case_ids)
        
        if not snippets:
            return RAGResponse(
                answer="No relevant data found for your query.",
                citations=[],
                confidence=0.0,
                snippets=[],
                query_metadata={'query': question, 'processing_time': 0}
            )
        
        # Step 2: Format snippets for prompt
        formatted_snippets = self._format_snippets(snippets)
        
        # Step 3: Build prompt
        prompt = self.rag_prompt_template.format(
            snippets=formatted_snippets,
            question=question
        )
        
        # Step 4: Generate answer
        if self.llm:
            answer = self.llm.generate(prompt)
        else:
            # Mock response for testing without LLM
            answer = self._generate_mock_answer(question, snippets)
        
        # Step 5: Extract and validate citations
        if require_citations:
            answer, citations = self._extract_citations(answer, snippets)
            
            # Validate that all claims have citations
            if not self._validate_citations(answer, citations):
                logger.warning("Answer contains uncited claims")
        else:
            citations = []
        
        # Step 6: Calculate confidence
        confidence = self._calculate_confidence(snippets, answer)
        
        # Processing time
        processing_time = (datetime.now() - start_time).total_seconds()
        
        return RAGResponse(
            answer=answer,
            citations=citations,
            confidence=confidence,
            snippets=snippets[:5],  # Return top 5 snippets
            query_metadata={
                'query': question,
                'processing_time': processing_time,
                'num_snippets_retrieved': len(snippets),
                'model_used': 'llm' if self.llm else 'mock'
            }
        )
    
    def _format_snippets(self, snippets: List[Dict]) -> str:
        """Format snippets for inclusion in prompt"""
        formatted = []
        
        for i, snippet in enumerate(snippets, 1):
            source_path = snippet.get('source_file', 'unknown')
            content = snippet.get('content', '')
            artifact_type = snippet.get('artifact_type', 'unknown')
            
            formatted.append(
                f"[{i}] Source: {source_path} | Type: {artifact_type}\n"
                f"Content: {content}\n"
            )
        
        return "\n".join(formatted)
    
    def _extract_citations(self, answer: str, snippets: List[Dict]) -> Tuple[str, List[Dict]]:
        """Extract citations from answer"""
        citations = []
        citation_pattern = r'\[(\d+)\]'
        
        # Find all citation references
        citation_matches = re.findall(citation_pattern, answer)
        
        for match in citation_matches:
            snippet_idx = int(match) - 1
            
            if 0 <= snippet_idx < len(snippets):
                snippet = snippets[snippet_idx]
                citations.append({
                    'reference_id': match,
                    'source_file': snippet.get('source_file', 'unknown'),
                    'artifact_type': snippet.get('artifact_type', 'unknown'),
                    'content_preview': snippet.get('content', '')[:200]
                })
        
        return answer, citations
    
    def _validate_citations(self, answer: str, citations: List[Dict]) -> bool:
        """Validate that significant claims have citations"""
        # Simple heuristic: check if answer has factual statements without citations
        sentences = answer.split('.')
        
        for sentence in sentences:
            # Skip short sentences and non-factual statements
            if len(sentence.strip()) < 20:
                continue
            
            # Check if sentence contains numbers, names, or specific claims
            has_specific_info = any([
                re.search(r'\d+', sentence),  # Contains numbers
                re.search(r'\+\d+', sentence),  # Phone numbers
                re.search(r'[A-Z][a-z]+', sentence),  # Proper nouns
                'crypto' in sentence.lower(),
                'address' in sentence.lower(),
                'sent' in sentence.lower(),
                'received' in sentence.lower()
            ])
            
            if has_specific_info and '[' not in sentence:
                return False
        
        return True
    
    def _calculate_confidence(self, snippets: List[Dict], answer: str) -> float:
        """Calculate confidence score for the answer"""
        if not snippets:
            return 0.0
        
        # Base confidence on snippet scores
        avg_score = sum(s.get('score', 0) for s in snippets[:5]) / min(5, len(snippets))
        
        # Adjust based on answer characteristics
        if "No data found" in answer or "insufficient" in answer.lower():
            confidence = 0.2
        elif len(answer) < 50:
            confidence = min(avg_score * 0.7, 0.5)
        else:
            confidence = min(avg_score, 0.95)
        
        return confidence
    
    def _generate_mock_answer(self, question: str, snippets: List[Dict]) -> str:
        """Generate mock answer for testing without LLM"""
        if not snippets:
            return "No relevant data found."
        
        # Simple rule-based response
        answer_parts = []
        
        if "crypto" in question.lower():
            crypto_found = False
            for i, snippet in enumerate(snippets[:3], 1):
                if re.search(r'(bitcoin|ethereum|crypto|0x[a-fA-F0-9]{40})', 
                           snippet.get('content', ''), re.IGNORECASE):
                    answer_parts.append(
                        f"Cryptocurrency activity detected in communications [{i}]."
                    )
                    crypto_found = True
                    break
            
            if not crypto_found:
                answer_parts.append("No cryptocurrency addresses found in the data.")
        
        elif "foreign" in question.lower() or "international" in question.lower():
            foreign_found = False
            for i, snippet in enumerate(snippets[:3], 1):
                if re.search(r'\+(?!91)', snippet.get('content', '')):
                    answer_parts.append(
                        f"International communications identified [{i}]."
                    )
                    foreign_found = True
                    break
            
            if not foreign_found:
                answer_parts.append("No foreign communications detected.")
        
        else:
            # Generic response
            answer_parts.append(
                f"Found {len(snippets)} relevant records. "
                f"The data shows activity related to your query [1]."
            )
        
        return " ".join(answer_parts)
    
    def explain_finding(self, 
                       finding: Dict,
                       context_size: int = 3) -> str:
        """Generate explanation for a specific finding"""
        prompt = f"""Explain the significance of this forensic finding:

Finding: {json.dumps(finding, indent=2)}

Provide a brief, investigator-friendly explanation of what this means and why it might be important.

Explanation:"""
        
        if self.llm:
            return self.llm.generate(prompt, max_tokens=256)
        else:
            return "This finding contains potentially relevant information for the investigation."
    
    def summarize_case(self,
                      case_id: str,
                      max_length: int = 500) -> str:
        """Generate case summary"""
        # Retrieve diverse snippets from the case
        queries = [
            "communication patterns",
            "key contacts",
            "suspicious activity",
            "timeline of events"
        ]
        
        all_snippets = []
        for query in queries:
            snippets = self.retriever.retrieve(query, top_k=5, case_ids=[case_id])
            all_snippets.extend(snippets)
        
        if not all_snippets:
            return f"No data available for case {case_id}"
        
        # Remove duplicates
        seen = set()
        unique_snippets = []
        for s in all_snippets:
            content_hash = hash(s.get('content', ''))
            if content_hash not in seen:
                seen.add(content_hash)
                unique_snippets.append(s)
        
        prompt = f"""Summarize the key findings from this forensic case:

CASE ID: {case_id}

KEY EVIDENCE:
{self._format_snippets(unique_snippets[:10])}

Provide a concise summary highlighting:
1. Main communication patterns
2. Notable entities or contacts
3. Suspicious activities if any
4. Timeline insights

SUMMARY:"""
        
        if self.llm:
            return self.llm.generate(prompt, max_tokens=max_length)
        else:
            return f"Case {case_id} contains {len(unique_snippets)} relevant artifacts requiring detailed analysis."


def main():
    """CLI interface for RAG engine"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Query UFDR data using RAG")
    parser.add_argument("question", help="Natural language question")
    parser.add_argument("--case-id", help="Filter to specific case")
    parser.add_argument("--model", default="infra/models/llm/mistral-7b-instruct-v0.2.Q4_K_M.gguf",
                       help="Path to LLM model")
    parser.add_argument("--index-dir", default="data/indices", help="Vector index directory")
    parser.add_argument("--no-citations", action="store_true", help="Disable citation requirement")
    
    args = parser.parse_args()
    
    # Initialize RAG engine
    rag = RAGEngine(
        llm_model_path=args.model,
        vector_index_dir=args.index_dir
    )
    
    # Process query
    case_ids = [args.case_id] if args.case_id else None
    response = rag.query(
        args.question,
        case_ids=case_ids,
        require_citations=not args.no_citations
    )
    
    # Display results
    print("\n" + "="*60)
    print("ANSWER:")
    print("="*60)
    print(response.answer)
    
    if response.citations:
        print("\n" + "-"*60)
        print("CITATIONS:")
        print("-"*60)
        for citation in response.citations:
            print(f"[{citation['reference_id']}] {citation['source_file']}")
            print(f"    Preview: {citation['content_preview'][:100]}...")
    
    print("\n" + "-"*60)
    print(f"Confidence: {response.confidence:.2%}")
    print(f"Processing time: {response.query_metadata['processing_time']:.2f}s")
    print(f"Snippets retrieved: {response.query_metadata['num_snippets_retrieved']}")
    
    return 0


if __name__ == "__main__":
    exit(main())