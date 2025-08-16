# Instructions to build the native library with musl libc and run Csharp tests on an Alpine image

1. Set AWS credentials AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_SESSION_TOKEN

2. From top directory of the pkg `aws-glue-schema-registry`, run
```
docker build \
--build-arg AWS_ACCESS_KEY_ID=<Your AWS Access Key ID> \
--build-arg AWS_SECRET_ACCESS_KEY=<Your AWS Secret Access Key> \
--build-arg AWS_SESSION_TOKEN=<Your AWS Session Token> \
-t native-schema-registry-musl \ 
-f native-schema-registry/build/musl/Dockerfile . \
--progress=plain
```