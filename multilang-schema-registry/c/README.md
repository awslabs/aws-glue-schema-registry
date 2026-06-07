# Native Schema Registry in C 

This module provides a C language based API for the schema registry serializer / de-serializers.

## Build
We use CMake to build the targets in this module.

### Compile
```bash
#Run in c directory

cmake -S. -Bbuild 
cd build 
cmake --build .
```

**Note:** Quality gates (tests, static analysis, and coverage) run automatically by default on every build.

#### Clean
```bash
cmake --build . --target clean
```

### Testing
```bash
ctest .
#Re-run failed tests with verbose output
ctest --rerun-failed --output-on-failure
```

### Quality Gates
Quality gates include tests, static analysis (clang-tidy), and code coverage checks.

#### Default Behavior (Automatic Quality Gates)
Quality gates run automatically on every build:
```bash
cmake -S. -Bbuild
cd build
cmake --build .  # Quality gates run automatically
```

#### Manual Quality Gates Only
Run quality gates manually when needed:
```bash
cd build
make quality_gates
```

#### Disable Automatic Quality Gates
If you need to disable automatic quality gates (e.g., for faster development builds):
```bash
cmake -S. -Bbuild -DDISABLE_QUALITY_GATES=ON
cd build
cmake --build .  # Quality gates won't run automatically
make quality_gates  # But you can still run them manually
```

### Code Analysis
Code is statically analyzed using clang-tidy.

### Coverage
Code coverage checks using gcov and lcov and fail if the coverage is below threshold.

#### Installation
You might have to install these modules using your OS package manager.

1. cmake
2. lcov
3. gcov
4. clang-tidy

### Sanitizers
We use address, leak sanitizers to detect memory leaks and any potential issues during build. As of now, they only work on Linux.

### Add Class
In order to add a class that is accessible by target language, these steps are required:
1. Create the .h file in `include` directory
2. Create the .i file in `src/swig` directory
3. Create the .c file in `src` directory 
4. Add the .i file to `glue_schema_registry_serde.i` file in `src/swig` directory
5. Add the .c file to the `CMakeLists.txt` file in `src` directory

## License

**Project License** [Apache License Version 2.0](https://github.com/awslabs/aws-glue-schema-registry/blob/master/LICENSE.txt)

N.B.: Although this repository is released under the Apache-2.0 license, its build dependencies include the third party Swig project. The Swig project's licensing includes the GPL-3.0 license.
