"""LLM Integration utilities for AI Analytics Assistant.

This module provides placeholder functions for LLM-powered features.
Currently, these are stubs that will be activated when llm_interpretation.json
artifacts are produced by the analytics engine.
"""
import streamlit as st
from pathlib import Path
import json
from typing import Optional, Dict, Any


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


def render_llm_placeholder():
    """Render a placeholder for LLM features when not available."""
    st.info("""
    **LLM-Powered Insights Coming Soon**
    
    When the analytics engine produces `llm_interpretation.json`, this section will display:
    - AI-generated claims with confidence scores
    - Supporting evidence from your data
    - Open questions for further investigation
    """)
