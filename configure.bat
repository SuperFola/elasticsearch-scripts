@echo off

echo Checking if python3 is installed
echo ================================
where py 1>nul
if %ERRORLEVEL% neq 0 (
	echo No python installation found
	echo Go to https://www.python.org/downloads/release/python-370/
	exit
) else (
	echo Python found
)
set errorlevel=0

py -c "import platform as p; v=p.python_version(); print(1 if v[0]=='3' and 54<=ord(v[1])<=57 else 0)" > tmpfile
set /p py_version_check= < tmpfile
del tmpfile
if py_version_check == "0" (
	echo Wrong version of Python installed
	echo Consider installing a version of Python greater or equal to 3 to continue
	exit
) else (
	echo Python versions matching successfully
)
echo.

echo Installing python modules
echo =========================
py -m pip install elasticsearch==1.7.0
py -m pip install colorama