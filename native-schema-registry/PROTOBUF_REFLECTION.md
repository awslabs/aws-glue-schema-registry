# Protocol Buffers Reflection Configuration

This document explains how to resolve Protocol Buffers reflection issues in GraalVM native image compilation.

## Problem

GraalVM native image compilation fails with `NoSuchMethodException` for Protocol Buffers methods like:
- `getCtype()`
- `valueOf()`
- `hasExtension()`

These errors occur because Protocol Buffers uses reflection for field options and enum value lookups that aren't automatically detected by GraalVM.

## Solution

Use the GraalVM tracing agent to automatically discover reflection requirements:

```bash
./scripts/trace-protobuf-reflection.sh
```

This script:
1. Creates a test that exercises Protocol Buffers reflection paths
2. Runs the test with GraalVM tracing agent
3. Generates comprehensive reflection configuration
4. Saves the configuration for review and integration

## Manual Configuration

If automatic tracing isn't sufficient, manually add these classes to `reflect-config.json`:

```json
{
  "name": "com.google.protobuf.DescriptorProtos$FieldOptions",
  "methods": [
    {"name": "getCtype", "parameterTypes": []},
    {"name": "hasCtype", "parameterTypes": []},
    {"name": "getJstype", "parameterTypes": []},
    {"name": "hasJstype", "parameterTypes": []}
  ]
},
{
  "name": "com.google.protobuf.DescriptorProtos$FieldOptions$CType",
  "methods": [
    {"name": "valueOf", "parameterTypes": ["com.google.protobuf.Descriptors$EnumValueDescriptor"]}
  ]
}
```

## Key Classes

The following Protocol Buffers classes commonly need reflection configuration:

- `DescriptorProtos$FieldOptions` - Field option getters/setters
- `DescriptorProtos$FieldOptions$CType` - Field type enums
- `DescriptorProtos$FieldOptions$JSType` - JavaScript type enums
- `DescriptorProtos$FeatureSet` - Protocol Buffers features
- `sun.misc.Unsafe` - Low-level memory operations

