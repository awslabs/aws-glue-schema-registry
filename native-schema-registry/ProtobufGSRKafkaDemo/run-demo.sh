#!/bin/bash
# Run the demo script for Protobuf GSR Kafka

# Build the shared, producer and consumer projects using dotnet build for each of them
dotnet build ../csharp/AWSGsrSerDe/AWSGsrSerDe/AWSGsrSerDe.csproj -c Release 
dotnet build Shared/Shared.csproj -c Release
dotnet build Producer/Producer.csproj -c Release
dotnet build Consumer/Consumer.csproj -c Release

# Start Producer (sends 10 messages per type = 50 total):
cd Producer/
./run-producer.sh | tee ./producer.log


# Start Consumer (in a separate sub terminal if possible):
cd ../Consumer/
./run-consumer.sh | tee ./consumer.log

echo "✅ Tests completed successfully"