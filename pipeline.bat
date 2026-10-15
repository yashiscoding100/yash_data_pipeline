@echo off
set SCRIPT_DIR=%~dp0
set PYTHONPATH=%SCRIPT_DIR%;%PYTHONPATH%
python "%SCRIPT_DIR%pipeline.py" %*
