"""
Cross-Case Analyzer
Uses DeepSeek 671B (cloud) for intelligent entity linking across multiple forensic cases
"""

import sqlite3
import logging
import json
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import time
from rag.llm_client import get_llm_client

logger = logging.getLogger(__name__)


@dataclass
class EntityLink:
    """Represents a shared entity across cases"""
    entity_type: str  # phone, email, crypto_wallet, name, device, location
    entity_value: str
    cases: List[str]
    occurrences: int
    confidence: float
    context: str


@dataclass
class CaseConnection:
    """Represents a connection between two cases"""
    case_1: str
    case_2: str
    shared_entities: List[EntityLink]
    connection_strength: float
    summary: str


class CrossCaseAnalyzer:
    """
    Cross-case analysis using cloud LLM (DeepSeek 671B / GPT-4 / Gemini)
    
    Finds sophisticated patterns and connections that simple SQL can't detect:
    - Phone number variations (+91 vs 91 vs raw)
    - Email aliases
    - Cryptocurrency wallet addresses
    - Name variations (nicknames, aliases)
    - Temporal patterns
    - Behavioral patterns
    """
    
    def __init__(self, db_path: str = "forensic_data.db"):
        self.db_path = db_path
        self._llm = get_llm_client()
        
        logger.info("🔗 Cross-Case Analyzer initialized")
        if self._llm:
            logger.info(f"   Using LLM provider: {self._llm[0]}")
        else:
            logger.warning("   No LLM configured. Analysis will be limited to basic SQL matching.")
    
    def analyze_cross_case_links(
        self,
        case_ids: List[str],
        progress_callback: Optional[callable] = None
    ) -> Dict[str, Any]:
        """
        Analyze connections between multiple cases
        
        Args:
            case_ids: List of case IDs to analyze
            progress_callback: Optional callback(current, total, status_msg)
        
        Returns:
            Dict with connections, entities, and analysis summary
        """
        
        start_time = time.time()
        
        logger.info(f"🔗 Starting cross-case analysis")
        logger.info(f"   Cases: {case_ids}")
        
        try:
            if len(case_ids) < 2:
                return {
                    'success': False,
                    'error': 'Need at least 2 cases for cross-case analysis',
                    'connections': []
                }
            
            # Step 1: Extract data from all cases
            if progress_callback:
                progress_callback(0, 100, "Extracting data from cases...")
            
            case_data = self._extract_case_data(case_ids)
            
            logger.info(f"📊 Extracted data from {len(case_data)} cases")
            
            # Step 2: Find obvious shared entities (SQL-based)
            if progress_callback:
                progress_callback(20, 100, "Finding shared entities...")
            
            shared_entities = self._find_shared_entities(case_ids)
            
            logger.info(f"🔍 Found {len(shared_entities)} shared entities (basic)")
            
            # Step 3: Use cloud LLM for intelligent analysis
            if progress_callback:
                progress_callback(40, 100, "Analyzing with AI...")
            
            llm_analysis = self._analyze_with_cloud_llm(case_data, shared_entities)
            
            # Step 4: Build connection graph
            if progress_callback:
                progress_callback(80, 100, "Building connection graph...")
            
            connections = self._build_connections(case_ids, llm_analysis, shared_entities)
            
            processing_time = time.time() - start_time
            
            if progress_callback:
                progress_callback(100, 100, f"Complete! Found {len(connections)} connections")
            
            logger.info(f"✅ Cross-case analysis complete")
            logger.info(f"   Connections found: {len(connections)}")
            logger.info(f"   Processing time: {processing_time:.1f}s")
            
            return {
                'success': True,
                'connections': [self._connection_to_dict(c) for c in connections],
                'shared_entities': [self._entity_to_dict(e) for e in shared_entities],
                'case_ids': case_ids,
                'processing_time': processing_time,
                'analysis_summary': llm_analysis.get('summary', ''),
                'total_connections': len(connections)
            }
        
        except Exception as e:
            logger.error(f"❌ Cross-case analysis failed: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'connections': []
            }
    
    def _extract_case_data(self, case_ids: List[str]) -> Dict[str, Dict]:
        """Extract key data from each case"""
        
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        case_data = {}
        
        for case_id in case_ids:
            data = {
                'case_id': case_id,
                'contacts': [],
                'messages_sample': [],
                'calls_sample': [],
                'devices': [],
                'locations_sample': []
            }
            
            # Get contacts
            cursor.execute("""
                SELECT name, phone_raw, email 
                FROM contacts 
                WHERE case_id = ? 
                LIMIT 50
            """, (case_id,))
            data['contacts'] = [dict(row) for row in cursor.fetchall()]
            
            # Get message sample
            cursor.execute("""
                SELECT msg_id, sender_raw, receiver_raw, text, timestamp
                FROM messages
                WHERE case_id = ?
                ORDER BY timestamp DESC
                LIMIT 100
            """, (case_id,))
            data['messages_sample'] = [dict(row) for row in cursor.fetchall()]
            
            # Get calls sample
            cursor.execute("""
                SELECT call_id, caller_raw, receiver_raw, duration_seconds, timestamp
                FROM calls
                WHERE case_id = ?
                ORDER BY timestamp DESC
                LIMIT 50
            """, (case_id,))
            data['calls_sample'] = [dict(row) for row in cursor.fetchall()]
            
            # Get devices
            cursor.execute("""
                SELECT device_id, model, os_type, os_version
                FROM devices
                WHERE case_id = ?
            """, (case_id,))
            data['devices'] = [dict(row) for row in cursor.fetchall()]
            
            case_data[case_id] = data
        
        conn.close()
        
        return case_data
    
    def _find_shared_entities(self, case_ids: List[str]) -> List[EntityLink]:
        """Find obviously shared entities using SQL"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        shared_entities = []
        
        # Find shared phone numbers
        placeholders = ','.join(['?'] * len(case_ids))
        
        # Shared phones in contacts
        query = f"""
            SELECT phone_raw, GROUP_CONCAT(DISTINCT case_id) as cases, COUNT(*) as count
            FROM contacts
            WHERE case_id IN ({placeholders})
            AND phone_raw IS NOT NULL
            AND phone_raw != ''
            GROUP BY phone_raw
            HAVING COUNT(DISTINCT case_id) > 1
        """
        
        cursor.execute(query, case_ids)
        for row in cursor.fetchall():
            phone, cases_str, count = row
            cases_list = cases_str.split(',')
            
            shared_entities.append(EntityLink(
                entity_type='phone',
                entity_value=phone,
                cases=cases_list,
                occurrences=count,
                confidence=1.0,
                context='Found in contacts'
            ))
        
        # Shared emails
        query = f"""
            SELECT email, GROUP_CONCAT(DISTINCT case_id) as cases, COUNT(*) as count
            FROM contacts
            WHERE case_id IN ({placeholders})
            AND email IS NOT NULL
            AND email != ''
            GROUP BY email
            HAVING COUNT(DISTINCT case_id) > 1
        """
        
        cursor.execute(query, case_ids)
        for row in cursor.fetchall():
            email, cases_str, count = row
            cases_list = cases_str.split(',')
            
            shared_entities.append(EntityLink(
                entity_type='email',
                entity_value=email,
                cases=cases_list,
                occurrences=count,
                confidence=1.0,
                context='Found in contacts'
            ))
        
        conn.close()
        
        return shared_entities
    
    def _analyze_with_cloud_llm(
        self,
        case_data: Dict[str, Dict],
        shared_entities: List[EntityLink]
    ) -> Dict[str, Any]:
        """
        Use cloud LLM (GPT-4/Claude/Gemini) for intelligent analysis
        """
        if not self._llm:
            return {
                'additional_entities': [],
                'patterns': [],
                'summary': 'LLM analysis skipped (no API key configured). Using basic SQL matching only.'
            }

        try:
            # Build analysis prompt
            prompt_text = self._build_analysis_prompt(case_data, shared_entities)
            
            # System prompt for forensic analysis
            system_prompt = """You are an expert forensic analyst specializing in finding connections between criminal cases.
You excel at identifying non-obvious patterns, aliases, and hidden relationships.
You always return valid JSON responses."""
            
            provider, client = self._llm
            response_text = ""
            
            # Call LLM based on provider
            if provider in ("openai", "openrouter"):
                response = client.chat.completions.create(
                    model="google/gemini-2.0-flash-001" if provider == "openrouter" else "gpt-4o",  # Default models
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": prompt_text}
                    ],
                    temperature=0.3,
                    response_format={"type": "json_object"}
                )
                response_text = response.choices[0].message.content
                
            elif provider == "gemini":
                # For Gemini, we include system prompt in the generation config or prepend it
                full_prompt = f"{system_prompt}\n\n{prompt_text}"
                response = client.generate_content(
                    full_prompt,
                    generation_config={"response_mime_type": "application/json"}
                )
                response_text = response.text
            
            # Parse JSON response
            try:
                parsed = json.loads(response_text)
                return parsed
            except json.JSONDecodeError:
                logger.warning("LLM returned invalid JSON, extracting what we can...")
                # Try to extract JSON from response
                import re
                json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
                if json_match:
                    try:
                        return json.loads(json_match.group())
                    except:
                        pass
                
                return {
                    'additional_entities': [],
                    'patterns': [],
                    'summary': f"Analysis completed but JSON parsing failed. length: {len(response_text)}"
                }
        
        except Exception as e:
            logger.error(f"LLM analysis failed: {e}", exc_info=True)
            return {
                'additional_entities': [],
                'patterns': [],
                'summary': f'LLM analysis error: {str(e)}'
            }
    
    def _build_analysis_prompt(
        self,
        case_data: Dict[str, Dict],
        shared_entities: List[EntityLink]
    ) -> str:
        """Build prompt for LLM analysis"""
        
        # Format case data
        cases_summary = []
        for case_id, data in case_data.items():
            summary = f"Case {case_id}:\n"
            summary += f"  - Contacts: {len(data['contacts'])}\n"
            summary += f"  - Messages: {len(data['messages_sample'])} (sample)\n"
            summary += f"  - Calls: {len(data['calls_sample'])} (sample)\n"
            
            # Sample contacts
            if data['contacts']:
                summary += "  - Sample contacts:\n"
                for contact in data['contacts'][:5]:
                    summary += f"    • {contact.get('name', 'Unknown')}: {contact.get('phone_raw', 'N/A')}\n"
            
            # Sample messages with crypto-related content
            crypto_keywords = ['crypto', 'bitcoin', 'btc', 'ethereum', 'eth', 'wallet', 'blockchain']
            relevant_messages = [
                msg for msg in data['messages_sample']
                if any(kw in msg.get('text', '').lower() for kw in crypto_keywords)
            ]
            
            if relevant_messages:
                summary += "  - Crypto-related messages:\n"
                for msg in relevant_messages[:3]:
                    summary += f"    • {msg.get('sender_raw', 'Unknown')}: {msg.get('text', '')[:100]}\n"
            
            cases_summary.append(summary)
        
        # Format shared entities
        entities_summary = []
        for entity in shared_entities:
            entities_summary.append(
                f"  - {entity.entity_type}: {entity.entity_value} "
                f"(in cases: {', '.join(entity.cases)})"
            )
        
        prompt = f"""You are a forensic analyst analyzing connections between multiple criminal cases.

CASES TO ANALYZE:
{chr(10).join(cases_summary)}

ALREADY FOUND SHARED ENTITIES:
{chr(10).join(entities_summary) if entities_summary else '  None'}

YOUR TASK:
1. Find ADDITIONAL connections not obvious from simple matching:
   - Name variations (nicknames, aliases, similar names)
   - Phone number variations (+91 vs 91 vs raw)
   - Email aliases
   - Cryptocurrency wallet addresses mentioned in messages
   - Behavioral patterns
   - Temporal patterns

2. Analyze the STRENGTH of connections

3. Provide a summary of case relationships

OUTPUT FORMAT (JSON):
{{
  "additional_entities": [
    {{
      "entity_type": "crypto_wallet",
      "entity_value": "1A2B3C4D...",
      "cases": ["case1", "case2"],
      "confidence": 0.9,
      "context": "Found in message texts"
    }}
  ],
  "patterns": [
    {{
      "pattern_type": "temporal",
      "description": "Both cases show activity in same time period",
      "cases": ["case1", "case2"],
      "confidence": 0.75
    }}
  ],
  "summary": "Overall analysis of how these cases are connected"
}}

Return ONLY valid JSON, no other text.
"""
        
        return prompt
    
    def _build_connections(
        self,
        case_ids: List[str],
        llm_analysis: Dict,
        shared_entities: List[EntityLink]
    ) -> List[CaseConnection]:
        """Build connection objects between cases"""
        
        connections = []
        
        # Build connections for each pair of cases
        for i, case1 in enumerate(case_ids):
            for case2 in case_ids[i+1:]:
                # Find entities shared between these two cases
                case_pair_entities = [
                    e for e in shared_entities
                    if case1 in e.cases and case2 in e.cases
                ]
                
                # Add LLM-discovered entities
                llm_entities = []
                for e in llm_analysis.get('additional_entities', []):
                    if case1 in e.get('cases', []) and case2 in e.get('cases', []):
                        llm_entities.append(EntityLink(
                            entity_type=e['entity_type'],
                            entity_value=e['entity_value'],
                            cases=e['cases'],
                            occurrences=len(e['cases']),
                            confidence=e.get('confidence', 0.5),
                            context=e.get('context', 'Found by AI')
                        ))
                
                all_entities = case_pair_entities + llm_entities
                
                if all_entities:
                    # Calculate connection strength
                    strength = min(1.0, len(all_entities) * 0.2)
                    
                    # Generate summary
                    summary = f"Found {len(all_entities)} shared entities: "
                    summary += ', '.join([f"{e.entity_type}={e.entity_value[:20]}" for e in all_entities[:3]])
                    if len(all_entities) > 3:
                        summary += f" and {len(all_entities) - 3} more"
                    
                    connection = CaseConnection(
                        case_1=case1,
                        case_2=case2,
                        shared_entities=all_entities,
                        connection_strength=strength,
                        summary=summary
                    )
                    
                    connections.append(connection)
        
        return connections
    
    def _entity_to_dict(self, entity: EntityLink) -> Dict[str, Any]:
        """Convert EntityLink to dictionary"""
        return {
            'entity_type': entity.entity_type,
            'entity_value': entity.entity_value,
            'cases': entity.cases,
            'occurrences': entity.occurrences,
            'confidence': entity.confidence,
            'context': entity.context
        }
    
    def _connection_to_dict(self, connection: CaseConnection) -> Dict[str, Any]:
        """Convert CaseConnection to dictionary"""
        return {
            'case_1': connection.case_1,
            'case_2': connection.case_2,
            'shared_entities': [self._entity_to_dict(e) for e in connection.shared_entities],
            'connection_strength': connection.connection_strength,
            'summary': connection.summary
        }


# Singleton instance
_cross_case_analyzer = None

def get_cross_case_analyzer(db_path: str = "forensic_data.db") -> CrossCaseAnalyzer:
    """Get or create cross-case analyzer singleton"""
    global _cross_case_analyzer
    
    if _cross_case_analyzer is None:
        _cross_case_analyzer = CrossCaseAnalyzer(db_path)
    
    return _cross_case_analyzer


# Convenience function
def analyze_cases(case_ids: List[str]) -> Dict[str, Any]:
    """
    Quick cross-case analysis function
    
    Example:
        results = analyze_cases(["case_001", "case_002", "case_003"])
    """
    
    analyzer = get_cross_case_analyzer()
    return analyzer.analyze_cross_case_links(case_ids)
