# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
[markdownlint](https://dlaa.me/markdownlint/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.2] - 2024-12-05

### Changed in 1.0.2

- Updated dependencies:
  - Updated `senzing-commons` from version `3.3.1` to `3.3.2`
  - Updated `amqp-client` from version `5.22.0` to `5.23.0`
  - Updated Amazon AWS dependencies from version `2.28.17` to `2.29.26`
  - Updated `junit-jupiter` from version `5.11.2` to version `5.11.3`
  - Updated `maven-surefire-plugin` from version `3.5.1` to `3.5.2`
  - Updated `maven-javadoc-plugin` from version `3.10.1` to `3.11.1`

## [1.0.1] - 2024-10-08

### Changed in 1.0.1

- Updated dependencies:
  - Updated `senzing-commons` from version `3.3.0` to `3.3.1`
  - Updated `amqp-client` from version `5.21.0` to `5.22.0`
  - Updated Amazon AWS dependencies from version `2.26.27` to `2.28.17`
  - Updated `commons-cli` from version `1.8.0` to `1.9.0`
  - Updated `postgresql` from version `42.7.3` to `42.7.4`
  - Updated `junit-jupiter` from version `5.10.3` to version `5.11.2`
  - Updated `maven-surefire-plugin` from version `3.3.31` to `3.5.1`
  - Updated `maven-javadoc-plugin` from version `3.8.0` to `3.10.1`
  - Updated `maven-gpg-plugin` from version `3.2.4` to `3.2.7`

## [1.0.0] - 2024-08-02

### Changed in 1.0.0

- Promoted to version `1.0.0` release
- In `pom.xml`:
  - Updated Amazon AWS dependencies from version `2.26.4` to `2.26.27`
  - Updated `junit-jupiter` from version `5.10.2` to version `5.10.3`
  - Updated `maven-surefire-plugin` from version `3.3.0` to `3.3.31`
  - Updated `maven-javadoc-plugin` from version `3.7.0` to `3.8.0`

## [0.5.10] - 2024-06-24

### Changed in 0.5.10

- In `Dockerfile`, updated 
  - FROM instruction to `senzing/senzingapi-runtime:3.10.3`
  - `senzing/base-image-debian:1.0.24`

## [0.5.9] - 2024-06-20

### Changed in 0.5.9

- Updated `senzing-commons-java` dependency from version `3.2.0` to version `3.3.0`
- Updated Amazon AWS dependencies to version `2.26.4`
- Updated `commons-configuration2` dependency from version `2.10.1` to version `2.11.0`
- Updated `amqp-client` dependency from version `5.20.0` to version `5.21.0`
- Updated `commons-cli` dependency from version `1.6.0` to version `1.8.0`
- Updated `postgresql` dependency from version `42.7.2` to version `42.7.3`
- Updated `maven-compiler-plugin` dependency from version `3.12.1` to version `3.13.0`
- Updated `maven-surefire-plugin` dependency from version `3.2.5` to version `3.3.0`
- Updated `maven-source-plugin` dependency from version `3.3.0` to version `3.3.1`
- Updated `maven-javadoc-plugin` dependency from version `3.6.3` to version `3.7.0`
- Updated `maven-gpg-plugin` dependency from version `3.1.0` to version `3.2.4`
- Updated `nexus-staging-maven-plugin` dependency from version `1.6.13` to version `1.7.0`

## [0.5.8] - 2024-05-22

### Changed in 0.5.8

- In `Dockerfile`, updated 
  - FROM instruction to `senzing/senzingapi-runtime:3.10.1`
  - `senzing/base-image-debian:1.0.23`

## [0.5.7] - 2024-03-21

### Changed in 0.5.7

- Fixed bug in PostgreSQL v13.x support for `com.senzing.listener.communication.sql.PostgreSQLClient`
- Fixed bug in `com.senzing.listener.service.scheduling.PostgreSQLSchedulingService` prevnting the
  trigger from being dropped properly.
- Updated `commons-configuration2` dependency from version `2.9.0` to version `2.10.1`

## [0.5.6] - 2024-03-20

### Changed in 0.5.6

- Added support for PostgreSQL v13.x by changing changing `CREATE OR REPLACE TRIGGER`
  to `DROP TRIGGER` and `CREATE TRIGGER`

## [0.5.5] - 2024-02-29

### Changed in 0.5.5

- Updated dependency versions:
  - Updated `senzing-commons-java` to version `3.2.0`
  - Updated Amazon AWS dependencies from version `2.22.12` to `2.24.12`
  - Updated `junit-jupiter` from version `5.10.1` to `5.10.2`
  - Updated `postgresql` from version `42.7.1` to `42.7.2`
  - Updated `maven-surefire-plugin` from version `3.2.3` to `3.2.5`

## [0.5.4] - 2024-01-10

### Changed in 0.5.4

- Updated dependency versions:
  - Updated AWS dependencies to version `2.22.12`
  - Updated `maven-compiler-plugin` to version `3.12.1`
  - Updated `maven-surefire-plugin` to version `3.2.3`
  - Updated `maven-javadoc-plugin` to version `3.6.3`

## [0.5.3] - 2023-12-07

### Changed in 0.5.3

- Changed `sqlite-jdbc` dependency to version `3.42.0.1` to avoid the problematic
  version `3.43.x.x` which carelessly breaks backwards compatibility by removing
  functionality that has been supported for sixteen (16) years. 
- Added `com.senzing.listener.communication.sql.SQLConsumer` implementation of 
  `com.senzing.listener.communication.MessageConsumer`.  
- Made `G2Service.getG2InitDataAsJson(String)` function `public` and renamed the
  function from `getG2IniDataAsJson(String)`
- Added `ConsumerType.DATABASE` enumerated constant for `SQLConsumer`.
- Updated `MessageConsumerFactory` to support `ConsumerType.DATABASE` constant 
  and creation of `SQLConsumer` instances.

## [0.5.2] - 2023-10-13

### Changed in 0.5.2

- Changed dependency on `senzing-commons` to a minimum version of `3.1.2` to address 
  bug related to setting SQLite timestamp values.
- Streamlined message consumptio throttling in `AbstractMessageConsumer` to wait until 
  pending count falls below half of the maximum value.
- Fixed `RabbitMQMessageConsumer` to override throttling by doing a `basicCancel()` and
  then do a `basicConsume()` when pending message count drops.  This prevents RabbitMQ
  from timing out the connection (especially with SQLite since we only have one database
  connection to work with)
- Updated third-party dependencies to newer versions.

## [0.5.1] - 2023-09-30

### Changed in 0.5.1

- In `Dockerfile`, updated FROM instruction to `senzing/senzingapi-runtime:3.7.1`

## [0.5.0] - 2023-09-22

### Changed in 0.5.0

- Major overhaul and refactoring of class hierarchy
- Enhancements to `G2Service`
- Adds `com.senzing.listener.service.locking` with `LockingService` and implementations
- Adds `com.senzing.listener.model` with `SzInfoMessage` and subordinate classes for parsing INFO messges
- Adds `com.senzing.listener.service.scheduling` with `SchedulingService` and implementations

## [0.3.2] - 2023-04-04

### Changed in 0.3.2

- In `Dockerfile`, updated FROM instruction to `senzing/senzingapi-runtime:3.5.0`

## [0.3.1] - 2023-03-08

### Changed in 0.3.1

- Added initWithG2Config method for initializing G2 with json configuration string instead of path to the G2 ini file

## [0.3.0] - 2022-09-29

### Changed in 0.3.0

- In `Dockerfile`, updated FROM instruction to `senzing/senzingapi-runtime:3.3.0`

## [0.2.0] - 2022-08-26

### Changed in 0.2.0

- In `Dockerfile`, bump from `senzing/senzingapi-runtime:3.1.0` to `senzing/senzingapi-runtime:3.2.0`

## [0.1.0] - 2022-05-11

### Changed in 0.2.0

- ???

## [0.0.4] - 2021-10-20

### Changed in 0.0.4

- Added Javadoc description for @throws in G2Service.java
- Removed SLF4J dependencies to remediate security vulnerabilities

## [0.0.3] - 2021-10-15

### Changed in 0.0.3

- Updated dependencies in pom.xml

## [0.0.2] - 2021-10-06

### Changed in 0.0.2

- Prep for central repo
- Added Javadoc
- Refactored constants for initialization parameters to make them specific to
  the messaging vendor
- Removed some unused classes
- Changed `cleanUp()` functions to `destroy()`
- Changed lower case / camelCase enums to UPPER_CASE
- Renamed `com.senzing.listener.senzing.*` packages to `com.senzing.listener.*`

## [0.0.1] - 2021-02-17

### Initial pre-release

- This is a retroactive release of version 0.0.1, on which several other git projects may be dependent.
- This release maintains the com.senzing.listener.senzing package naming as well as the com.senzing.listener.senzing.data package.
- These things were cleaned up in version 0.0.2 but no prior release was made for anything dependent on them.
