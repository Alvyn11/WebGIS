@echo off
cd /d "C:\Users\alvin\OneDrive\Desktop\Thesis\farm webgis\backend"
python -m uvicorn main:app --reload --port 8000
pause
