#!/bin/bash

# AI Generated
# TODO: We should make this part of build process

# GraalVM Tracing Agent for Protocol Buffers Reflection Discovery
# This script helps discover reflection requirements for Protocol Buffers classes
# that cause NoSuchMethodException during native image compilation.
#


set -e

JAVA_HOME=${GRAALVM_HOME:-$JAVA_HOME}
CONFIG_DIR="src/main/resources/META-INF/native-image/software.amazon.glue/native-schema-registry"
TEMP_CONFIG_DIR="temp-protobuf-config"

echo "Protocol Buffers Reflection Tracing Agent"
echo "========================================="

# Clean up
rm -rf $TEMP_CONFIG_DIR
mkdir -p $TEMP_CONFIG_DIR

# Get classpath from Maven
echo "Resolving classpath from Maven..."
MAVEN_CLASSPATH=$(cd .. && mvn dependency:build-classpath -q -Dmdep.outputFile=/dev/stdout | tail -1)

if [ -z "$MAVEN_CLASSPATH" ]; then
    echo "❌ Failed to resolve classpath from Maven"
    echo "Run: mvn dependency:resolve"
    exit 1
fi

CLASSPATH=".:$MAVEN_CLASSPATH"
echo "✅ Classpath resolved from pom.xml dependencies"

# Write classpath to file to avoid "Argument list too long" error
echo "$CLASSPATH" > classpath.txt

# Create test that triggers Protocol Buffers reflection
cat > TestProtobufReflection.java << 'EOF'
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.amazonaws.services.schemaregistry.utils.apicurio.FileDescriptorUtils;

public class TestProtobufReflection {
    public static void main(String[] args) {
        try {
            System.out.println("Testing Protocol Buffers reflection paths...");
            
            // Create FileDescriptor with field options that trigger hasExtension calls
            DescriptorProtos.FileDescriptorProto.Builder fileBuilder = 
                DescriptorProtos.FileDescriptorProto.newBuilder();
            fileBuilder.setName("test.proto");
            fileBuilder.setPackage("test");
            
            DescriptorProtos.DescriptorProto.Builder messageBuilder = 
                DescriptorProtos.DescriptorProto.newBuilder();
            messageBuilder.setName("TestMessage");
            
            DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder = 
                DescriptorProtos.FieldDescriptorProto.newBuilder();
            fieldBuilder.setName("test_field");
            fieldBuilder.setNumber(1);
            fieldBuilder.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);
            
            // Add field options that trigger reflection
            DescriptorProtos.FieldOptions.Builder optionsBuilder = 
                DescriptorProtos.FieldOptions.newBuilder();
            optionsBuilder.setCtype(DescriptorProtos.FieldOptions.CType.STRING);
            optionsBuilder.setJstype(DescriptorProtos.FieldOptions.JSType.JS_STRING);
            
            fieldBuilder.setOptions(optionsBuilder.build());
            messageBuilder.addField(fieldBuilder.build());
            fileBuilder.addMessageType(messageBuilder.build());
            
            DescriptorProtos.FileDescriptorProto fileProto = fileBuilder.build();
            
            // Trigger FileDescriptorUtils processing that uses hasExtension
            var protoFile = FileDescriptorUtils.fileDescriptorToProtoFile(fileProto);
            System.out.println("✅ FileDescriptorUtils processing completed: " + protoFile.getPackageName());
            
            System.out.println("✅ Protocol Buffers reflection test completed successfully!");
            
        } catch (Exception e) {
            System.err.println("❌ Error during Protocol Buffers reflection test:");
            e.printStackTrace();
        }
    }
}
EOF

echo "Compiling Protocol Buffers reflection test..."
$JAVA_HOME/bin/javac -cp @classpath.txt TestProtobufReflection.java

echo "Running with GraalVM tracing agent..."
$JAVA_HOME/bin/java \
  -agentlib:native-image-agent=config-output-dir=$TEMP_CONFIG_DIR \
  -cp @classpath.txt \
  TestProtobufReflection

if [ -f "$TEMP_CONFIG_DIR/reflect-config.json" ]; then
    echo ""
    echo "Generated Protocol Buffers reflection config:"
    echo "============================================="
    cat $TEMP_CONFIG_DIR/reflect-config.json | jq '.' 2>/dev/null || cat $TEMP_CONFIG_DIR/reflect-config.json
    echo "============================================="
    
    # Save discovered config
    mkdir -p $CONFIG_DIR
    cp $TEMP_CONFIG_DIR/reflect-config.json $CONFIG_DIR/reflect-config-discovered.json
    echo ""
    echo "✅ Saved discovered config to: $CONFIG_DIR/reflect-config-discovered.json"
    echo ""
    echo "To use this configuration:"
    echo "1. Review the discovered classes and methods"
    echo "2. Merge with existing reflect-config.json if needed"
    echo "3. Test native image compilation: ./buildall"
else
    echo "❌ No reflection config was generated"
fi

# Cleanup
rm -f TestProtobufReflection.java TestProtobufReflection.class classpath.txt
rm -rf $TEMP_CONFIG_DIR

echo ""
echo "Protocol Buffers reflection tracing completed!"
