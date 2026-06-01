@echo off
echo ===================================================
echo [WAYANG DEMO] Dang ve bieu do so sanh hieu nang...
echo ===================================================
echo.

:: 1. Chay file python plot
python plot.py

:: 2. Mo bieu do vua sinh ra bang Windows Explorer
if exist benchmark_result.png (
    echo [OK] Da sinh bieu do benchmark_result.png thanh cong!
    explorer benchmark_result.png
) else (
    echo [Loi] Khong tim thay file benchmark_result.png. Vui long kiem tra kết quả results.csv.
)

pause
