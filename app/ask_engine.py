"""Ask Engine - CLI wrapper for analyst-agent ask command."""
import subprocess
import json
import re
from pathlib import Path
from typing import Dict, Any, Optional
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
    
    if not question or not question.strip():
        return AskResult(
            success=False,
            answerable=False,
            error="Question is required"
        )
    
    cmd = [
        "analyst-agent", "ask",
        "--project", project_id,
        "--question", question.strip()
    ]
    
    if not use_llm:
        cmd.append("--no-llm")
    
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
