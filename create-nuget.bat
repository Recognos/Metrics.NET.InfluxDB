rd /S /Q .\Publishing\lib

call build.bat
if %errorlevel% neq 0 exit /b %errorlevel%

md .\Publishing\lib
md .\Publishing\lib\net45

copy .\bin\Release\Metrics.InfluxDB.dll .\Publishing\lib\net45\
copy .\bin\Release\Metrics.InfluxDB.xml .\Publishing\lib\net45\
copy .\bin\Release\Metrics.InfluxDB.pdb .\Publishing\lib\net45\

.nuget\nuget.exe pack .\Publishing\Metrics.NET.InfluxDB.nuspec -OutputDirectory .\Publishing
if %errorlevel% neq 0 exit /b %errorlevel%
