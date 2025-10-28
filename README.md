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

# Run interactive CLI
java -jar target/javakv.jar

# Run all tests 
mvn test

# Run only unit tests
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

## CLI Commands

```
PUT <key> <value>    # Store a key-value pair
GET <key>            # Retrieve a value
DELETE <key>         # Remove a key
EXISTS <key>         # Check if a key exists
SIZE                 # Display number of entries
CLEAR                # Remove all entries
HELP                 # Display help
EXIT                 # Exit
```
