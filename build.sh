wget https://dist.nuget.org/win-x86-commandline/latest/nuget.exe -O .nuget/nuget.exe

mono .nuget/nuget.exe restore Metrics.InfluxDB.sln 

xbuild Metrics.InfluxDB.sln /p:Configuration="Debug"
xbuild Metrics.InfluxDB.sln /p:Configuration="Release"

mono ./packages/xunit.runner.console.2.0.0/tools/xunit.console.exe ./bin/Debug/Metrics.InfluxDB.Tests.dll -parallel none
mono ./packages/xunit.runner.console.2.0.0/tools/xunit.console.exe ./bin/Release/Metrics.InfluxDB.Tests.dll -parallel none