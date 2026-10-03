---
paths:
  - "**/*.java"
---

# Code style

## Readability

The purpose of code style rules is to maintain code readability and developer
efficiency when working with the code. All the code style rules explained below
are good guidelines to follow but there may be exceptional situations where we
purposefully depart from them. When readability and code style rule are at odds,
the readability is more important.

## Consistency

Keep code consistent with surrounding code where possible.

## Alphabetize

Alphabetize sections in the documentation source files (both in the table of
contents files and other regular documentation files).

## Use streams

When appropriate, use the stream API. However, note that the stream
implementation does not perform well so avoid using it in inner loops or
otherwise performance sensitive sections.

## Categorize errors when throwing exceptions

Categorize errors when throwing exceptions. For example, `TrinoException` takes
an error code as an argument, `TrinoException(HIVE_TOO_MANY_OPEN_PARTITIONS)`.
This categorization lets you generate reports so you can monitor the frequency
of various failures.

## Add license header

Ensure that all files have the appropriate license header; you can generate the
license by running `./mvnw license:format`.

## Prefer String formatting

Consider using String formatting with the `String.formatted` method:
`"Session property %s is invalid: %s".formatted(name, value)`.
Sometimes, if you only need to append something, consider using the `+` operator.
Please avoid `formatted()` or concatenation in performance critical sections of
code.

## Avoid ternary operator

Avoid using the ternary operator except for trivial expressions.

## Avoid `get` in method names, unless an object must be a Java bean

In most cases, replace `get` with a more specific verb that describes what is
happening in the method, like `find` or `fetch`. If there isn't a more specific
verb or the method is a getter, omit `get` because it isn't helpful to readers
and makes method names longer.

## Define class API for private inner classes too

It is suggested to declare members in private inner classes as public if they
are part of the class API.

## Avoid mocks

Do not use mocking libraries. These libraries encourage testing specific call
sequences, interactions, and other internal behavior, which we believe leads to
fragile tests.  They also make it possible to mock complex interfaces or
classes, which hides the fact that these classes are not (easily) testable. We
prefer to write mocks by hand, which forces code to be written in a certain
testable style.

## Use AssertJ

Prefer AssertJ for complex assertions.

## Use Airlift's `Assertions`

For thing not easily expressible with AssertJ, use Airlift's `Assertions` class
if there is one that covers your case.

## Use `var` judiciously

Use `var` only when it improves readability. Prefer it when the type
is obvious from the initializer, such as with `new` expressions, or
when the explicit type is long or heavily generic and adds noise
without improving clarity.

Avoid `var` when the inferred type is unclear, surprising, or
important to understanding the code.

## Prefer Guava immutable collections

Prefer using immutable collections from Guava over unmodifiable collections from
JDK. The main motivation behind this is deterministic iteration.

## Maintain production quality for test code

Maintain the same quality for production and test code.

## Avoid abbreviations

Please avoid abbreviations, slang or inside jokes as this makes harder for
non-native english speaker to understand the code. Very well known
abbreviations like `max` or `min` and ones already very commonly used across
the code base like `ttl` are allowed and encouraged.

## Avoid default clause in exhaustive enum-based switch statements

Avoid using the `default` clause when the switch statement is meant to cover all
the enum values. Handling the unknown option case after the switch statement
allows static code analysis tools (e.g. Error Prone's `MissingCasesInEnumSwitch`
check) report a problem when the enum definition is updated but the code using
it is not.

## Use braces for control statement bodies

Use braces around `if`, `for` and `while` bodies, even when the body is a
single statement. The formatter does not add missing braces.

## Do not use `@author`

Do not add `@author` tags to Javadoc. Commit history is the record.

## Annotate embedded languages with `@Language`

The `@org.intellij.lang.annotations.Language` annotation is useful for
documenting the API's intent, even if the IDE does not support language
injection. We recommend annotating with `@Language`:

- All API parameters which are expecting to take a `String` containing an SQL
  statement (or any other language, like regular expressions),
- Local variables which otherwise would not be properly recognized by IDE for
  language injection.

## Vector API
It's safe to assume that the JVM has the Vector API
([JEP 508](https://openjdk.org/jeps/508)) enabled and available at runtime, but
not safe to assume that the Vector API implementation will perform faster than
equivalent scalar code on whatever hardware the engine happens to be running on.

Different CPU hardware can exhibit dramatically different performance
characteristics, so it's important to use hardware feature detection to
determine under which scenarios a vectorized approach will be faster for
each implementation. Vectorized code should be tested on AMD, ARM, and Intel
CPUs to verify the benefits hold on each of those platforms before deciding
to enable a given code path on each of those platforms. Also note that ARM CPUs
can exhibit significant differences from between hardware generations as well
as between Apple Silicon and datacenter class CPUs.

When adding implementations that use the Vector API, prefer the following
approach unless the specifics of the situation dictate otherwise:
* Provide an equivalent scalar implementation in code, if one does not already
exist.
* Use configuration flags and hardware support detection to ensure that
vectorized implementation is only selected when running on hardware where it is
expected to perform better than its scalar equivalent.
* Add tests that ensure the behavior of the vectorized and scalar
implementations match.
* Include micro-benchmarks that demonstrate the performance benefits of the
vectorized implementation compared to the scalar equivalent logic. Ensure that
the benefits hold for all CPU architectures on which the vectorized
implementation is enabled.
