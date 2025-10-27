# JavaKV

A key-value store implementation in Java 21.

## Prerequisites

- Java 21+
- Maven 3.9+

```bash
# Linux
sudo pacman -S jdk21-openjdk maven
```

## Quick Start

```bash
# Build
mvn clean install

# Run demo
mvn compile exec:java -Dexec.mainClass="com.javakv.cli.Main"

# Run all tests (81 tests: 71 unit + 10 integration)
mvn test

# Run only unit tests (fast, ~0.5s)
mvn test -Dgroups="!integration"

# Run only integration tests (end-to-end with real WAL)
mvn test -Dgroups="integration"

# Run specific test class
mvn test -Dtest=WalIntegrationTest

# Generate coverage report
mvn clean test jacoco:report

# View coverage
xdg-open target/site/jacoco/index.html

# Run Checkstyle for code style checking
mvn checkstyle:check
```
