"""
AI Report Generator for UFDR Analysis Tool

Generates structured forensic reports by:
1. Retrieving ALL case data from ChromaDB + SQLite (offline)
2. Sending structured context to Gemini/OpenAI for report generation
3. Outputting formatted Markdown reports

Requires a cloud LLM API key (Gemini or OpenAI) for report generation.
"""

import os
import sqlite3
import logging
from typing import Optional
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv

from rag import DB_PATH
from rag.faiss_store import FAISSStore

logger = logging.getLogger(__name__)

# Load environment
_project_root = Path(__file__).resolve().parent.parent
load_dotenv(_project_root / ".env")

from utils.retry import retry_with_backoff


REPORT_TYPES = {
    "case_summary": {
        "title": "Case Summary Report",
        "description": "Comprehensive overview of all forensic data in the case",
    },
    "communication_analysis": {
        "title": "Communication Analysis Report",
        "description": "Analysis of messaging and call patterns, key contacts",
    },
    "timeline": {
        "title": "Timeline Reconstruction Report",
        "description": "Chronological reconstruction of events from all data sources",
    },
    "cross_case": {
        "title": "Cross-Case Linkage Report",
        "description": "Shared entities, contacts, and patterns across multiple cases",
    },
}


from rag.llm_client import get_llm_client


def _get_case_stats(case_id: str, db_path: str = DB_PATH) -> dict:
    """Get detailed stats for a case from SQLite."""
    stats = {"case_id": case_id}
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        for table in ["messages", "contacts", "calls", "media", "locations"]:
            try:
                cursor.execute(f'SELECT COUNT(*) FROM "{table}" WHERE case_id = ?', (case_id,))
                stats[f"{table}_count"] = cursor.fetchone()[0]
            except Exception:
                stats[f"{table}_count"] = 0
        
        # Get case metadata
        try:
            cursor.execute("SELECT * FROM cases WHERE case_id = ?", (case_id,))
            row = cursor.fetchone()
            if row:
                stats["case_info"] = dict(row)
        except Exception:
            pass
        
        # Get device info
        try:
            cursor.execute("SELECT * FROM devices WHERE case_id = ?", (case_id,))
            devices = [dict(r) for r in cursor.fetchall()]
            stats["devices"] = devices
        except Exception:
            pass
        
        # Get date range  
        try:
            cursor.execute("""
                SELECT MIN(timestamp), MAX(timestamp) FROM (
                    SELECT timestamp FROM messages WHERE case_id = ?
                    UNION ALL
                    SELECT timestamp FROM calls WHERE case_id = ?
                )
            """, (case_id, case_id))
            date_range = cursor.fetchone()
            stats["date_range"] = {
                "earliest": date_range[0],
                "latest": date_range[1],
            }
        except Exception:
            pass
        
        # Get top contacts by message frequency
        try:
            cursor.execute("""
                SELECT sender_raw as contact, COUNT(*) as msg_count
                FROM messages WHERE case_id = ?
                GROUP BY sender_raw ORDER BY msg_count DESC LIMIT 10
            """, (case_id,))
            stats["top_senders"] = [dict(r) for r in cursor.fetchall()]
        except Exception:
            pass
        
        # Get app distribution
        try:
            cursor.execute("""
                SELECT app, COUNT(*) as count FROM messages 
                WHERE case_id = ? GROUP BY app ORDER BY count DESC
            """, (case_id,))
            stats["apps"] = [dict(r) for r in cursor.fetchall()]
        except Exception:
            pass
        
        conn.close()
    except Exception as e:
        logger.error(f"Failed to get stats for case '{case_id}': {e}")
    
    return stats


def _get_sample_data(case_id: str, db_path: str = DB_PATH, samples: int = 20) -> dict:
    """Get sample records from each table for context."""
    data = {}
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        for table in ["messages", "contacts", "calls", "media", "locations"]:
            try:
                cursor.execute(
                    f'SELECT * FROM "{table}" WHERE case_id = ? LIMIT ?',
                    (case_id, samples)
                )
                data[table] = [dict(r) for r in cursor.fetchall()]
            except Exception:
                data[table] = []
        
        conn.close()
    except Exception as e:
        logger.error(f"Failed to get sample data: {e}")
    
    return data


class ReportGenerator:
    """
    Generates forensic reports using RAG + LLM.
    """
    
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._store = FAISSStore()
        self._llm = get_llm_client()
    
    @property
    def available_report_types(self) -> dict:
        return REPORT_TYPES
    
    @property
    def is_llm_available(self) -> bool:
        return self._llm is not None
    
    def generate(
        self,
        report_type: str,
        case_ids: list[str],
        custom_instructions: str = "",
    ) -> dict:
        """
        Generate a forensic report.
        
        Args:
            report_type: One of REPORT_TYPES keys
            case_ids: Cases to include
            custom_instructions: Additional instructions
            
        Returns:
            {"title": str, "content": str (Markdown), "metadata": dict}
        """
        if not self._llm:
            return {
                "title": "Report Generation Unavailable",
                "content": "⚠️ No LLM API key configured.\n\nSet `GEMINI_API_KEY` in your `.env` file to enable report generation.",
                "metadata": {"error": "no_api_key"},
            }
        
        report_info = REPORT_TYPES.get(report_type, REPORT_TYPES["case_summary"])
        
        # Gather data
        all_stats = [_get_case_stats(cid, self.db_path) for cid in case_ids]
        all_samples = {cid: _get_sample_data(cid, self.db_path) for cid in case_ids}
        
        # Build prompt
        prompt = self._build_prompt(report_type, report_info, all_stats, all_samples, custom_instructions)
        
        # Generate via LLM
        try:
            content = self._call_llm(prompt)
        except Exception as e:
            logger.error(f"Report generation failed: {e}")
            return {
                "title": report_info["title"],
                "content": f"❌ Report generation failed: {e}",
                "metadata": {"error": str(e)},
            }
        
        return {
            "title": report_info["title"],
            "content": content,
            "metadata": {
                "report_type": report_type,
                "case_ids": case_ids,
                "generated_at": datetime.now().isoformat(),
                "llm_provider": self._llm[0] if self._llm else None,
            },
        }
    
    def _build_prompt(self, report_type, report_info, all_stats, all_samples, custom_instructions):
        """Build the LLM prompt with forensic data context."""
        prompt = f"""You are an expert forensic data analyst. Generate a detailed, professional 
**{report_info['title']}** based on the following forensic evidence data.

REPORT TYPE: {report_info['description']}

"""
        for stats in all_stats:
            case_id = stats.get("case_id", "Unknown")
            prompt += f"\n## Case: {case_id}\n"
            prompt += f"- Messages: {stats.get('messages_count', 0)}\n"
            prompt += f"- Calls: {stats.get('calls_count', 0)}\n"
            prompt += f"- Contacts: {stats.get('contacts_count', 0)}\n"
            prompt += f"- Media: {stats.get('media_count', 0)}\n"
            prompt += f"- Locations: {stats.get('locations_count', 0)}\n"
            
            if stats.get("date_range"):
                dr = stats["date_range"]
                prompt += f"- Date Range: {dr.get('earliest', 'N/A')} to {dr.get('latest', 'N/A')}\n"
            
            if stats.get("devices"):
                for dev in stats["devices"]:
                    prompt += f"- Device: {dev.get('manufacturer', '')} {dev.get('model', '')} ({dev.get('os_type', '')} {dev.get('os_version', '')})\n"
            
            if stats.get("top_senders"):
                prompt += "\nTop Senders:\n"
                for s in stats["top_senders"][:5]:
                    prompt += f"  - {s.get('contact', 'Unknown')}: {s.get('msg_count', 0)} messages\n"
            
            if stats.get("apps"):
                prompt += "\nMessaging Apps:\n"
                for a in stats["apps"]:
                    prompt += f"  - {a.get('app', 'Unknown')}: {a.get('count', 0)} messages\n"
            
            # Add sample data
            samples = all_samples.get(case_id, {})
            for table, rows in samples.items():
                if rows:
                    prompt += f"\nSample {table} (first {len(rows)}):\n"
                    for row in rows[:10]:
                        # Compact representation
                        parts = [f"{k}={v}" for k, v in row.items() if v and k != "case_id"]
                        prompt += f"  - {', '.join(parts[:6])}\n"
        
        if custom_instructions:
            prompt += f"\nADDITIONAL INSTRUCTIONS: {custom_instructions}\n"
        
        prompt += """
FORMAT REQUIREMENTS:
- Output in clean Markdown
- Include an executive summary at the top
- Use tables where appropriate
- Include specific data references (names, phone numbers + dates)
- Note any patterns, anomalies, or CROSS-CASE LINKS
- End with key findings and recommendations
"""
        return prompt
    
    @retry_with_backoff(max_retries=3, initial_delay=2.0)
    def _call_llm(self, prompt: str) -> str:
        """Call the configured LLM."""
        provider, client = self._llm
        
        if provider == "gemini":
            response = client.generate_content(prompt)
            return response.text
        
        elif provider == "openai":
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=4000,
            )
            return response.choices[0].message.content
            
        elif provider == "openrouter":
            response = client.chat.completions.create(
                model="google/gemini-2.0-flash-001",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=4000,
                extra_headers={
                    "HTTP-Referer": "https://github.com/kartikay/ufdr-analysis-tool",
                    "X-Title": "UFDR Analysis Tool",
                },
            )
            return response.choices[0].message.content
        
        return "Unsupported LLM provider"
