"""Entry point for Streamlit deployment - redirects to app/app.py"""
import runpy
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
runpy.run_path("app/app.py", run_name="__main__")
