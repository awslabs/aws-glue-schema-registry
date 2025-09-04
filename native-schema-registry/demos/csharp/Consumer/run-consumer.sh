#!/bin/bash
echo "Starting Protobuf GSR Kafka Consumer..."
export LD_LIBRARY_PATH=../../csharp/AWSGsrSerDe/Libs:$LD_LIBRARY_PATH
dotnet run --project Consumer.csproj