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

# Run tests
mvn test

# View coverage
xdg-open target/site/jacoco/index.html
```
