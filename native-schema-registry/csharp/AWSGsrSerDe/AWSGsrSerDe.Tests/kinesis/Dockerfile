FROM mcr.microsoft.com/dotnet/sdk:8.0

# Install Java (required for KCL MultiLangDaemon)
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    maven \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /app

# Copy project files
COPY . .

# Clone and setup KCL .NET if not exists
RUN if [ ! -d "amazon-kinesis-client-net" ]; then \
        git clone https://github.com/awslabs/amazon-kinesis-client-net.git; \
    fi

# Build KCL .NET
RUN cd amazon-kinesis-client-net && \
    dotnet build ClientLibrary/ClientLibrary.csproj

# Build test project
RUN dotnet restore KinesisGsrTest/KinesisGsrTest.csproj && \
    dotnet build KinesisGsrTest/KinesisGsrTest.csproj

CMD ["dotnet", "test", "KinesisGsrTest/KinesisGsrTest.csproj", "--logger", "console;verbosity=detailed"]
