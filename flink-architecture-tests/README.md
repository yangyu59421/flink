# flink-architecture-tests

This module contains architecture tests using [ArchUnit](https://www.archunit.org/). These tests 
reside in their own module in order to control the classpath of which modules should be tested.
Running these tests together (rather than individually per module) allows caching the imported
classes for better performance.

Tests are currently only execute against Java code, but not Scala code. This is due to missing 
(explicit) support for Scala in ArchUnit.

In order to add a module to be tested against, simply add it to the classpath in this module's 
`pom.xml`. Otherwise no specific setup is required to run these tests.
