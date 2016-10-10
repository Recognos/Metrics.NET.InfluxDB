rem Download nuget.exe if the file doesn't exist
if not exist .nuget\nuget.exe powershell -Command "& { wget https://dist.nuget.org/win-x86-commandline/latest/nuget.exe -OutFile .nuget\nuget.exe }"

.nuget\nuget.exe restore Metrics.InfluxDB.sln

set MSBUILD="C:\Program Files (x86)\MSBuild\14.0\Bin\MSBuild.exe"
set XUNIT=".\packages\xunit.runner.console.2.0.0\tools\xunit.console.exe"

rd /S /Q .\bin\Debug
rd /S /Q .\bin\Release

%MSBUILD% Metrics.InfluxDB.Sln /p:Configuration="Debug"
if %errorlevel% neq 0 exit /b %errorlevel%

%MSBUILD% Metrics.InfluxDB.Sln /p:Configuration="Release"
if %errorlevel% neq 0 exit /b %errorlevel%

%XUNIT% .\bin\Debug\Metrics.InfluxDB.Tests.dll -maxthreads 1
if %errorlevel% neq 0 exit /b %errorlevel%

%XUNIT% .\bin\Release\Metrics.InfluxDB.Tests.dll -maxthreads 1
if %errorlevel% neq 0 exit /b %errorlevel%