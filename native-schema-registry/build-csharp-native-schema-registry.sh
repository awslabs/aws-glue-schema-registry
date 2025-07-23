#!/bin/bash

echo "=== AWS Glue Schema Registry Native Build Script ==="
echo "Starting build process..."
echo

# Ensure we're in the right directory
echo "Current directory: $(pwd)"
echo "Expected: /home/isaurab/workplace/jun23_polyglot_gsr/aws-glue-schema-registry"
echo

echo "=== Step 1: Root Maven Build ==="
# skip if failing
mvn clean install -DskipTests
echo

echo "=== Step 2: First Native Schema Registry Build ==="
cd native-schema-registry
mvn install -P native-image
echo

echo "=== Step 3: First C Library Build ==="
cd c
cmake -S. -Bbuild 
cd build 
cmake --build .
echo

echo "=== Step 4: Second Native Schema Registry Build ==="
cd ../..
mvn install -P native-image
echo

echo "=== Step 5: Second C Library Build ==="
cd c
cmake -S. -Bbuild 
cd build 
cmake --build .
echo

echo "=== Step 6: C# Build and Test ==="
cd ../../csharp/AWSGsrSerDe
dotnet clean .
dotnet build .
dotnet build .
dotnet test .
echo

echo "=== Build Complete ==="
echo "Returning to original directory..."
cd ..