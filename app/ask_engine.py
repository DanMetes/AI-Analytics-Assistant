"""Ask Engine - CLI wrapper for analyst-agent ask command and LLM Q&A."""
import subprocess
import json
import re
import os
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass


@dataclass
class AskResult:
    """Result from the ask engine."""
    success: bool
    answerable: bool
    answer: Optional[str] = None
    evidence_keys: Optional[list] = None
    plan: Optional[str] = None
    plan_steps: Optional[list] = None
    code: Optional[str] = None
    error: Optional[str] = None
    raw_output: Optional[str] = None


def parse_cli_output(output: str) -> AskResult:
    """
    Parse the CLI output to extract answer or plan information.
    
    The CLI output format varies:
    - For answerable questions: returns a direct answer with evidence
    - For unanswerable questions: returns a methodology plan and code
    """
    if not output or not output.strip():
        return AskResult(
            success=False,
            answerable=False,
            error="Empty response from CLI",
            raw_output=output
        )
    
    output_lower = output.lower()
    
    if "error" in output_lower and ("dataset" in output_lower or "project" in output_lower or "run" in output_lower):
        return AskResult(
            success=False,
            answerable=False,
            error=output.strip(),
            raw_output=output
        )
    
    evidence_pattern = r'\[evidence:\s*([^\]]+)\]'
    evidence_matches = re.findall(evidence_pattern, output, re.IGNORECASE)
    
    evidence_key_pattern = r'`([^`]+)`'
    evidence_keys = []
    for match in evidence_matches:
        keys = re.findall(evidence_key_pattern, match)
        evidence_keys.extend(keys)
    
    if not evidence_keys:
        key_pattern = r'(?:evidence|source|from):\s*`([^`]+)`'
        evidence_keys = re.findall(key_pattern, output, re.IGNORECASE)
    
    if "methodology" in output_lower or "plan:" in output_lower or "scaffold" in output_lower:
        plan_text = output.strip()
        
        plan_steps = []
        step_pattern = r'(?:^|\n)\s*(?:\d+[\.\)]\s*|[-*]\s*)(.+?)(?=\n\s*(?:\d+[\.\)]|[-*])|\n\n|$)'
        step_matches = re.findall(step_pattern, output, re.MULTILINE)
        if step_matches:
            plan_steps = [s.strip() for s in step_matches if s.strip()]
        
        return AskResult(
            success=True,
            answerable=False,
            plan=plan_text,
            plan_steps=plan_steps if plan_steps else None,
            raw_output=output
        )
    
    if evidence_keys or len(output.strip()) > 20:
        return AskResult(
            success=True,
            answerable=True,
            answer=output.strip(),
            evidence_keys=evidence_keys if evidence_keys else None,
            raw_output=output
        )
    
    return AskResult(
        success=True,
        answerable=True,
        answer=output.strip(),
        raw_output=output
    )


def load_generated_code(project_id: str) -> Optional[str]:
    """
    Load generated code from the ask/ directory if it exists.
    
    The CLI writes scaffolds to projects/<project_id>/runs/<latest>/ask/
    """
    projects_dir = Path("projects")
    project_dir = projects_dir / project_id
    
    if not project_dir.exists():
        return None
    
    runs_dir = project_dir / "runs"
    if not runs_dir.exists():
        return None
    
    run_dirs = sorted(runs_dir.iterdir(), key=lambda x: x.stat().st_mtime, reverse=True)
    if not run_dirs:
        return None
    
    latest_run = run_dirs[0]
    ask_dir = latest_run / "ask"
    
    if not ask_dir.exists():
        return None
    
    py_files = list(ask_dir.glob("*.py"))
    if not py_files:
        return None
    
    latest_py = sorted(py_files, key=lambda x: x.stat().st_mtime, reverse=True)[0]
    
    try:
        with open(latest_py, "r") as f:
            return f.read()
    except IOError:
        return None


def run_ask(project_id: str, run_id: str, question: str, use_llm: bool = False, timeout: int = 60) -> tuple:
    """
    Execute the analyst-agent ask command and return parsed results.
    
    Args:
        project_id: The project ID to query
        run_id: The run ID (currently unused by CLI, reserved for future)
        question: The natural language question
        use_llm: Whether to enable LLM features (default: False)
        timeout: Timeout in seconds (default: 60)
        
    Returns:
        Tuple of (answer, plan, code, evidence_keys):
        - answer: str if answerable, None otherwise
        - plan: dict with methodology if not answerable, None otherwise
        - code: str with generated Python code if any, None otherwise
        - evidence_keys: list of evidence keys if answerable, None otherwise
    """
    result = run_ask_query(project_id, question, use_llm=use_llm, timeout=timeout)
    
    if not result.success:
        return (None, {"error": result.error}, None, None)
    
    if result.answerable:
        return (result.answer, None, None, result.evidence_keys)
    else:
        plan = {
            "methodology": result.plan,
            "steps": result.plan_steps or []
        }
        return (None, plan, result.code, None)


def run_ask_query(project_id: str, question: str, use_llm: bool = False, timeout: int = 60) -> AskResult:
    """
    Execute the analyst-agent ask command and parse the result.
    
    Args:
        project_id: The project ID to query
        question: The natural language question
        use_llm: Whether to enable LLM features (default: False)
        timeout: Timeout in seconds (default: 60)
        
    Returns:
        AskResult with answer/plan/code or error information
    """
    if not project_id or not project_id.strip():
        return AskResult(
            success=False,
            answerable=False,
            error="Project ID is required"
        )
    
    project_id = project_id.strip()
    if not re.match(r'^[a-zA-Z0-9_-]+$', project_id):
        return AskResult(
            success=False,
            answerable=False,
            error="Invalid project ID format"
        )
    
    if not question or not question.strip():
        return AskResult(
            success=False,
            answerable=False,
            error="Question is required"
        )
    
    cmd = [
        "analyst-agent", "ask",
        "--project", project_id,
        "--question", question.strip(),
    ]
    
    if not use_llm:
        cmd.append("--no-llm")
    
    cmd.append("--")  # End of options marker to prevent argument injection
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(Path(__file__).parent.parent)
        )
        
        if result.returncode != 0:
            stderr = result.stderr.strip()
            stdout = result.stdout.strip()
            
            if "no active dataset" in stderr.lower() or "no active dataset" in stdout.lower():
                return AskResult(
                    success=False,
                    answerable=False,
                    error="No active dataset found. Please ensure the project has a dataset configured.",
                    raw_output=stderr or stdout
                )
            elif "no runs found" in stderr.lower() or "no runs found" in stdout.lower():
                return AskResult(
                    success=False,
                    answerable=False,
                    error="No analysis runs found. Please run an analysis first.",
                    raw_output=stderr or stdout
                )
            elif "project not found" in stderr.lower() or "project not found" in stdout.lower():
                return AskResult(
                    success=False,
                    answerable=False,
                    error=f"Project '{project_id}' not found.",
                    raw_output=stderr or stdout
                )
            else:
                return AskResult(
                    success=False,
                    answerable=False,
                    error=stderr or stdout or f"CLI returned exit code {result.returncode}",
                    raw_output=stderr or stdout
                )
        
        parsed = parse_cli_output(result.stdout)
        
        if not parsed.answerable and parsed.success:
            code = load_generated_code(project_id)
            if code:
                parsed.code = code
        
        return parsed
        
    except subprocess.TimeoutExpired:
        return AskResult(
            success=False,
            answerable=False,
            error=f"Request timed out after {timeout} seconds. The question may require more processing time."
        )
    except FileNotFoundError:
        return AskResult(
            success=False,
            answerable=False,
            error="The analyst-agent CLI is not installed or not in PATH."
        )
    except PermissionError:
        return AskResult(
            success=False,
            answerable=False,
            error="Permission denied when trying to execute the CLI."
        )
    except Exception as e:
        return AskResult(
            success=False,
            answerable=False,
            error=f"Unexpected error: {str(e)}"
        )


def is_llm_available() -> bool:
    """Check if LLM API is available via environment variables or secrets."""
    import sys
    from pathlib import Path
    app_dir = Path(__file__).parent
    if str(app_dir) not in sys.path:
        sys.path.insert(0, str(app_dir))
    from llm_utils import get_openai_api_key
    return bool(get_openai_api_key())


def run_llm_ask(question: str, context: Dict[str, Any]) -> Tuple[Optional[str], Optional[list]]:
    """
    Ask a question using LLM with artifact context.
    
    Args:
        question: The user's question
        context: Dictionary containing artifact summaries (metrics, anomalies, profile)
        
    Returns:
        Tuple of (answer, references) where references are artifact keys used
    """
    import sys
    app_dir = Path(__file__).parent
    if str(app_dir) not in sys.path:
        sys.path.insert(0, str(app_dir))
    from llm_utils import get_openai_api_key, get_openai_base_url
    
    api_key = get_openai_api_key()
    base_url = get_openai_base_url()
    
    if not api_key:
        return None, None
    
    try:
        from openai import OpenAI
        
        client_kwargs = {"api_key": api_key}
        if base_url:
            client_kwargs["base_url"] = base_url
        
        client = OpenAI(**client_kwargs)
        
        context_str = ""
        if context.get("metrics_summary"):
            context_str += f"\n## Metrics Summary\n{context['metrics_summary']}\n"
        if context.get("anomalies_summary"):
            context_str += f"\n## Anomalies Detected\n{context['anomalies_summary']}\n"
        if context.get("profile_summary"):
            context_str += f"\n## Data Profile\n{context['profile_summary']}\n"
        
        system_prompt = """You are an AI analytics assistant helping users understand their data analysis results.
You have access to analysis artifacts including metrics, anomalies, and data profiles.
Provide concise, actionable answers based on the context provided.
When referencing specific metrics or findings, mention the source artifact.
If you cannot answer from the provided context, say so clearly."""
        
        user_prompt = f"""Based on the following analysis context, answer the user's question.

{context_str}

User's Question: {question}

Provide a clear, concise answer. If referencing specific data, cite the source (e.g., "from metrics", "from anomalies")."""
        
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            max_tokens=500,
            temperature=0.3
        )
        
        answer = response.choices[0].message.content
        references = []
        if "metrics" in answer.lower():
            references.append("metrics.csv")
        if "anomal" in answer.lower():
            references.append("anomalies_normalized.json")
        if "profile" in answer.lower() or "distribution" in answer.lower():
            references.append("data_profile.json")
        
        return answer, references if references else None
        
    except Exception as e:
        return f"Error: {str(e)}", None


def build_llm_context(run_path: Path) -> Dict[str, Any]:
    """Build context dictionary from run artifacts for LLM Q&A."""
    context = {}
    
    metrics_path = run_path / "metrics.csv"
    if metrics_path.exists():
        try:
            import pandas as pd
            df = pd.read_csv(metrics_path)
            summary_lines = []
            for section in df["section"].unique()[:3]:
                section_df = df[df["section"] == section]
                summary_lines.append(f"Section: {section}")
                for _, row in section_df.head(5).iterrows():
                    summary_lines.append(f"  - {row['key']}: {row['value']}")
            context["metrics_summary"] = "\n".join(summary_lines[:20])
        except Exception:
            pass
    
    anomalies_path = run_path / "anomalies_normalized.json"
    if anomalies_path.exists():
        try:
            with open(anomalies_path, "r") as f:
                data = json.load(f)
            anomalies = data.get("anomalies", []) if isinstance(data, dict) else data
            summary_lines = []
            for a in anomalies[:5]:
                severity = a.get("severity", "unknown")
                metric = a.get("metric", "unknown")
                summary = a.get("summary", "")
                summary_lines.append(f"- [{severity}] {metric}: {summary}")
            context["anomalies_summary"] = "\n".join(summary_lines)
        except Exception:
            pass
    
    profile_path = run_path / "data_profile.json"
    if profile_path.exists():
        try:
            with open(profile_path, "r") as f:
                profile = json.load(f)
            summary_lines = []
            if "row_count" in profile:
                summary_lines.append(f"Rows: {profile['row_count']}")
            if "column_count" in profile:
                summary_lines.append(f"Columns: {profile['column_count']}")
            if "columns" in profile:
                for col_name, col_data in list(profile["columns"].items())[:5]:
                    if isinstance(col_data, dict):
                        col_type = col_data.get("dtype", "unknown")
                        summary_lines.append(f"  - {col_name} ({col_type})")
            context["profile_summary"] = "\n".join(summary_lines)
        except Exception:
            pass
    
    return context
