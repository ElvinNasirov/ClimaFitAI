@echo off
echo Running Weather Pipeline

IF NOT EXIST ".venv\Scripts\activate.bat" (
    echo Virtual environment not found.
    echo Please run setup_and_run.bat first.
    pause
    exit /b 1
)

call .venv\Scripts\activate.bat

python src\pipeline.py

echo.
echo Pipeline finished.

pause