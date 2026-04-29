@echo off
echo Weather Intelligence Pipeline Setup & Run

echo.
echo Checking virtual environment...

IF NOT EXIST ".venv\Scripts\activate.bat" (
    echo Virtual environment not found. Creating .venv...
    python -m venv .venv
) ELSE (
    echo Virtual environment found.
)

echo.
echo Activating virtual environment...
call .venv\Scripts\activate.bat

echo.
echo Upgrading pip...
python -m pip install --upgrade pip

echo.
echo Installing project requirements...
pip install -r requirements.txt

echo.
echo Running full weather pipeline...
python src\pipeline.py

echo.
echo Pipeline finished.

pause