REM anyerror資料夾不存在就創建
REM if exist anyerror (echo directory CHECK OK) else (mkdir anyerror)

REM 如果錯誤回報部為0則暫停
python combine_table.py 2>error.log
if %errorlevel% neq 0 pause exit
timeout /t 2

python KPI.py 2>error.log
if %errorlevel% neq 0 pause exit
timeout /t 2

python YTBP.py 2>error.log
if %errorlevel% neq 0 pause exit
timeout /t 2

python audience.py 2>error.log
if %errorlevel% neq 0 pause exit
timeout /t 2

python bangumi.py 2>error.log
if %errorlevel% neq 0 pause exit

REM 如果錯誤回報為0，則刪除error.log
if %errorlevel% equ 0 (del /q error.log)
timeout /t 5
