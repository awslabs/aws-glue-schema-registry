#!/bin/bash
echo "Starting Protobuf GSR Kafka Producer..."
export LD_LIBRARY_PATH=../../csharp/AWSGsrSerDe/Libs:$LD_LIBRARY_PATH
dotnet run --project Producer.csproj