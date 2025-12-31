"""LLM Integration utilities for AI Analytics Assistant.

This module provides placeholder functions for LLM-powered features.
Currently, these are stubs that will be activated when llm_interpretation.json
artifacts are produced by the analytics engine.

API Key Priority:
1. User-provided OPENAI_API_KEY from st.secrets or os.environ (gives you control over costs)
2. Replit's AI_INTEGRATIONS_OPENAI_API_KEY (fallback)
"""
import streamlit as st
from pathlib import Path
import json
import os
from typing import Optional, Dict, Any


def get_openai_api_key() -> Optional[str]:
    """
    Get OpenAI API key with user-provided key taking priority.
    
    Priority order:
    1. st.secrets["OPENAI_API_KEY"] - user-configured secret
    2. os.environ["OPENAI_API_KEY"] - environment variable
    3. os.environ["AI_INTEGRATIONS_OPENAI_API_KEY"] - Replit integration (fallback)
    
    Returns:
        API key string or None if not configured.
    """
    if hasattr(st, 'secrets') and "OPENAI_API_KEY" in st.secrets:
        return st.secrets["OPENAI_API_KEY"]
    if os.environ.get("OPENAI_API_KEY"):
        return os.environ.get("OPENAI_API_KEY")
    return os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY")


def get_openai_base_url() -> Optional[str]:
    """Get OpenAI base URL if configured."""
    if hasattr(st, 'secrets') and "OPENAI_BASE_URL" in st.secrets:
        return st.secrets["OPENAI_BASE_URL"]
    if os.environ.get("OPENAI_BASE_URL"):
        return os.environ.get("OPENAI_BASE_URL")
    return os.environ.get("AI_INTEGRATIONS_OPENAI_BASE_URL")


def load_llm_interpretation(run_path: Path) -> Optional[Dict[str, Any]]:
    """
    Load the LLM interpretation artifact if it exists.
    
    Args:
        run_path: Path to the run directory
        
    Returns:
        Dictionary containing LLM interpretation data, or None if not available.
        Expected structure:
        {
            "claims": [{"statement": str, "confidence": float, "evidence": list}],
            "evidence": [{"key": str, "value": any, "source": str}],
            "open_questions": [str],
            "summary": str,
            "generated_at": str
        }
    """
    llm_path = run_path / "llm_interpretation.json"
    if llm_path.exists():
        try:
            with open(llm_path, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return None
    return None


def render_llm_summary(run_path: Path) -> bool:
    """
    Render the LLM-generated summary in Streamlit UI.
    
    Args:
        run_path: Path to the run directory
        
    Returns:
        True if LLM interpretation was found and rendered, False otherwise.
    """
    interpretation = load_llm_interpretation(run_path)
    
    if interpretation is None:
        return False
    
    st.subheader("AI-Generated Insights")
    
    if "summary" in interpretation:
        st.markdown(interpretation["summary"])
    
    if "claims" in interpretation and interpretation["claims"]:
        st.markdown("**Key Claims:**")
        for claim in interpretation["claims"]:
            confidence = claim.get("confidence", 0)
            confidence_badge = "🟢" if confidence > 0.8 else "🟡" if confidence > 0.5 else "🔴"
            st.markdown(f"{confidence_badge} {claim.get('statement', 'No statement')}")
            if "evidence" in claim and claim["evidence"]:
                with st.expander("Supporting evidence"):
                    for ev in claim["evidence"]:
                        st.markdown(f"- `{ev}`")
    
    if "open_questions" in interpretation and interpretation["open_questions"]:
        st.markdown("**Open Questions:**")
        for question in interpretation["open_questions"]:
            st.markdown(f"- {question}")
    
    if "generated_at" in interpretation:
        st.caption(f"Generated: {interpretation['generated_at']}")
    
    return True


def render_llm_interpretation(run_path: Path) -> bool:
    """
    Render the LLM interpretation for the Key Findings tab.
    
    Displays claims with evidence references, open questions,
    and recommended next analyses in a user-friendly format.
    
    Args:
        run_path: Path to the run directory
        
    Returns:
        True if LLM interpretation was found and rendered, False otherwise.
    """
    interpretation = load_llm_interpretation(run_path)
    
    if interpretation is None:
        return False
    
    st.markdown("---")
    st.subheader("LLM-Enhanced Interpretation")
    
    if "summary" in interpretation:
        st.markdown(f"**Summary:** {interpretation['summary']}")
    
    if "claims" in interpretation and interpretation["claims"]:
        st.markdown("**Claims:**")
        for i, claim in enumerate(interpretation["claims"], 1):
            confidence = claim.get("confidence", 0)
            if confidence >= 0.8:
                conf_label = "High confidence"
                conf_color = "#28a745"
            elif confidence >= 0.5:
                conf_label = "Medium confidence"
                conf_color = "#ffc107"
            else:
                conf_label = "Low confidence"
                conf_color = "#dc3545"
            
            statement = claim.get("statement", "No statement")
            evidence_refs = claim.get("evidence", [])
            
            st.markdown(f"""
<div style="border-left: 3px solid {conf_color}; padding: 8px 12px; margin: 8px 0; background: rgba(0,0,0,0.03); border-radius: 4px;">
    <div style="display: flex; justify-content: space-between; align-items: center;">
        <strong>{i}. {statement}</strong>
        <span style="font-size: 0.8em; color: {conf_color}; background: rgba(0,0,0,0.05); padding: 2px 8px; border-radius: 10px;">{conf_label} ({confidence:.0%})</span>
    </div>
</div>
            """, unsafe_allow_html=True)
            
            if evidence_refs:
                with st.expander(f"Evidence references ({len(evidence_refs)})"):
                    for ref in evidence_refs:
                        if isinstance(ref, dict):
                            key = ref.get("key", "")
                            value = ref.get("value", "")
                            source = ref.get("source", "")
                            st.markdown(f"- **{key}**: `{value}` _(from {source})_")
                        else:
                            st.markdown(f"- `{ref}`")
    
    if "open_questions" in interpretation and interpretation["open_questions"]:
        st.markdown("**Open Questions:**")
        st.markdown("_These questions remain unanswered and may warrant further analysis:_")
        for question in interpretation["open_questions"]:
            st.markdown(f"- {question}")
    
    if "recommended_analyses" in interpretation and interpretation["recommended_analyses"]:
        st.markdown("**Recommended Next Analyses:**")
        for rec in interpretation["recommended_analyses"]:
            if isinstance(rec, dict):
                title = rec.get("title", "Analysis")
                description = rec.get("description", "")
                priority = rec.get("priority", "medium")
                priority_icon = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(priority, "⚪")
                st.markdown(f"- {priority_icon} **{title}**: {description}")
            else:
                st.markdown(f"- {rec}")
    
    if "generated_at" in interpretation:
        st.caption(f"LLM interpretation generated: {interpretation['generated_at']}")
    
    return True


def render_llm_placeholder():
    """Render a placeholder for LLM features when not available."""
    st.info("""
**LLM interpretation not available**

Run the analysis with `--llm` flag to generate AI-powered insights including:
- Claims with confidence scores and evidence references
- Open questions for further investigation
- Recommended next analyses
    """)


def load_profile_llm_summary(run_path: Path) -> Optional[Dict[str, Any]]:
    """
    Load the LLM-generated profile summary if it exists.
    
    Args:
        run_path: Path to the run directory
        
    Returns:
        Dictionary containing LLM profile summary data, or None if not available.
        Expected structure:
        {
            "summary": str,
            "key_observations": [str],
            "data_quality_assessment": str,
            "column_insights": [{"column": str, "insight": str}],
            "recommendations": [str],
            "generated_at": str
        }
    """
    llm_path = run_path / "profile_llm_summary.json"
    if llm_path.exists():
        try:
            with open(llm_path, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return None
    return None


def render_llm_profile(run_path: Path) -> bool:
    """
    Render the LLM-generated profile summary in Streamlit UI.
    
    Displays AI-synthesized insights about the data profile including
    key observations, data quality assessment, and recommendations.
    
    Args:
        run_path: Path to the run directory
        
    Returns:
        True if LLM profile summary was found and rendered, False otherwise.
    """
    profile_summary = load_profile_llm_summary(run_path)
    
    if profile_summary is None:
        return False
    
    st.info("**AI-generated profile summary** — The insights below were synthesized by a language model based on the data profile.")
    st.subheader("AI-Generated Data Profile Summary")
    
    if "summary" in profile_summary:
        st.markdown(profile_summary["summary"])
    
    if "key_observations" in profile_summary and profile_summary["key_observations"]:
        st.markdown("**Key Observations:**")
        for obs in profile_summary["key_observations"]:
            st.markdown(f"- {obs}")
    
    if "data_quality_assessment" in profile_summary:
        st.markdown("**Data Quality Assessment:**")
        st.markdown(profile_summary["data_quality_assessment"])
    
    if "column_insights" in profile_summary and profile_summary["column_insights"]:
        with st.expander("Column-Level Insights"):
            for insight in profile_summary["column_insights"]:
                if isinstance(insight, dict):
                    col = insight.get("column", "Unknown")
                    desc = insight.get("insight", "")
                    st.markdown(f"**{col}:** {desc}")
                else:
                    st.markdown(f"- {insight}")
    
    if "recommendations" in profile_summary and profile_summary["recommendations"]:
        st.markdown("**Recommendations:**")
        for rec in profile_summary["recommendations"]:
            st.markdown(f"- {rec}")
    
    if "generated_at" in profile_summary:
        st.caption(f"Profile summary generated: {profile_summary['generated_at']}")
    
    return True


def render_llm_profile_placeholder():
    """Render a placeholder for LLM profile features when not available."""
    st.info("""
**LLM profile synthesis not available**

Run the analysis with `--llm` flag to generate AI-powered profile insights including:
- Natural language summary of the dataset
- Key observations about data distributions
- Data quality assessment
- Column-level insights and recommendations
    """)
