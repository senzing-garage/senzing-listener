# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
[markdownlint](https://dlaa.me/markdownlint/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.0] - 2023-09-22

### Changed in 0.5.0

- Major overhaul and refactoring of class hierarchy
- Enhancements to `G2Service`
- Adds `com.senzing.listener.service.locking` with `LockingService` and implementations
- Adds `com.senzing.listener.model` with `SzInfoMessage` and subordinate classes for parsing INFO messges
- Adds `com.senzing.listener.service.scheduling` with `SchedulingService` and implementations

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
