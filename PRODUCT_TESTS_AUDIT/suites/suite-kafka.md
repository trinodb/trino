# Suite Audit: SuiteKafka

## Suite Summary

- Purpose: JUnit 5 test suite for Kafka connector tests.
- Owning lane: `kafka`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteKafka.java`
- CI bucket: `auth-and-clients`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `KafkaBasicEnvironment`
- Include tags: `Kafka`.
- Exclude tags: `KafkaSchemaRegistry`, `KafkaConfluentLicense`.
- Expected mapped classes covered: `TestKafkaAvroReadsSmokeTest`, `TestKafkaAvroWritesSmokeTest`,
  `TestKafkaProtobufReadsSmokeTest`, `TestKafkaProtobufWritesSmokeTest`, `TestKafkaPushdownSmokeTest`,
  `TestKafkaReadsSmokeTest`, `TestKafkaWritesSmokeTest`.
- Expected mapped methods covered: `20` method(s).

### Run 2

- Run name: `default`
- Environment: `KafkaConfluentBasicEnvironment`
- Include tags: `Kafka`.
- Exclude tags: `KafkaSchemaRegistry`, `KafkaConfluentLicense`.
- Expected mapped classes covered: `TestKafkaAvroReadsSmokeTest`, `TestKafkaAvroWritesSmokeTest`,
  `TestKafkaProtobufReadsSmokeTest`, `TestKafkaProtobufWritesSmokeTest`, `TestKafkaPushdownSmokeTest`,
  `TestKafkaReadsSmokeTest`, `TestKafkaWritesSmokeTest`.
- Expected mapped methods covered: `20` method(s).

### Run 3

- Run name: `default`
- Environment: `KafkaSslEnvironment`
- Include tags: `Kafka`.
- Exclude tags: `KafkaSchemaRegistry`, `KafkaConfluentLicense`.
- Expected mapped classes covered: `TestKafkaAvroReadsSmokeTest`, `TestKafkaAvroWritesSmokeTest`,
  `TestKafkaProtobufReadsSmokeTest`, `TestKafkaProtobufWritesSmokeTest`, `TestKafkaPushdownSmokeTest`,
  `TestKafkaReadsSmokeTest`, `TestKafkaWritesSmokeTest`.
- Expected mapped methods covered: `20` method(s).

### Run 4

- Run name: `default`
- Environment: `KafkaSaslPlaintextEnvironment`
- Include tags: `Kafka`.
- Exclude tags: `KafkaSchemaRegistry`, `KafkaConfluentLicense`.
- Expected mapped classes covered: `TestKafkaAvroReadsSmokeTest`, `TestKafkaAvroWritesSmokeTest`,
  `TestKafkaProtobufReadsSmokeTest`, `TestKafkaProtobufWritesSmokeTest`, `TestKafkaPushdownSmokeTest`,
  `TestKafkaReadsSmokeTest`, `TestKafkaWritesSmokeTest`.
- Expected mapped methods covered: `20` method(s).

### Run 5

- Run name: `default`
- Environment: `KafkaSchemaRegistryEnvironment`
- Include tags: `KafkaSchemaRegistry`.
- Exclude tags: `KafkaConfluentLicense`.
- Expected mapped classes covered: `TestKafkaAvroReadsSchemaRegistrySmokeTest`.
- Expected mapped methods covered: `4` method(s).

### Run 6

- Run name: `default`
- Environment: `KafkaSchemaRegistryEnvironment`
- Include tags: `KafkaConfluentLicense`.
- Exclude tags: none.
- Expected mapped classes covered: `TestKafkaProtobufReadsSchemaRegistrySmokeTest`.
- Expected mapped methods covered: `3` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `auth-and-clients`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `Remove legacy SuiteKafka`

## Parity Checklist

- Legacy suite or lane source: `kafka` lane and the corresponding legacy launcher coverage.
- Current suite class: `SuiteKafka`
- Explicit runs and environments: verified from current suite source.
- Include tags: verified from current suite source.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `9`
- Expected migrated method count: `27`
- Expected migrated classes covered: `TestKafkaAvroReadsSchemaRegistrySmokeTest`, `TestKafkaAvroReadsSmokeTest`,
  `TestKafkaAvroWritesSmokeTest`, `TestKafkaProtobufReadsSchemaRegistrySmokeTest`, `TestKafkaProtobufReadsSmokeTest`,
  `TestKafkaProtobufWritesSmokeTest`, `TestKafkaPushdownSmokeTest`, `TestKafkaReadsSmokeTest`,
  `TestKafkaWritesSmokeTest`.
- Expected migrated methods covered: `TestKafkaAvroReadsSchemaRegistrySmokeTest.testAvroWithSchemaReferences`,
  `TestKafkaAvroReadsSchemaRegistrySmokeTest.testNullType`,
  `TestKafkaAvroReadsSchemaRegistrySmokeTest.testSelectPrimitiveDataType`,
  `TestKafkaAvroReadsSchemaRegistrySmokeTest.testSelectStructuralDataType`, `TestKafkaAvroReadsSmokeTest.testNullType`,
  `TestKafkaAvroReadsSmokeTest.testSelectPrimitiveDataType`, `TestKafkaAvroReadsSmokeTest.testSelectStructuralDataType`,
  `TestKafkaAvroWritesSmokeTest.testInsertPrimitiveDataType`,
  `TestKafkaAvroWritesSmokeTest.testInsertStructuralDataType`,
  `TestKafkaProtobufReadsSchemaRegistrySmokeTest.testProtobufWithSchemaReferences`,
  `TestKafkaProtobufReadsSchemaRegistrySmokeTest.testSelectPrimitiveDataType`,
  `TestKafkaProtobufReadsSchemaRegistrySmokeTest.testSelectStructuralDataType`,
  `TestKafkaProtobufReadsSmokeTest.testSelectPrimitiveDataType`,
  `TestKafkaProtobufReadsSmokeTest.testSelectStructuralDataType`,
  `TestKafkaProtobufWritesSmokeTest.testInsertAllDataType`,
  `TestKafkaProtobufWritesSmokeTest.testInsertStructuralDataType`, `TestKafkaPushdownSmokeTest.testCreateTimePushdown`,
  `TestKafkaPushdownSmokeTest.testOffsetPushdown`, `TestKafkaPushdownSmokeTest.testPartitionPushdown`,
  `TestKafkaReadsSmokeTest.testSelectAllCsvTable`, `TestKafkaReadsSmokeTest.testSelectAllJsonTable`,
  `TestKafkaReadsSmokeTest.testSelectAllRawTable`, `TestKafkaReadsSmokeTest.testSelectSimpleKeyAndValue`,
  `TestKafkaWritesSmokeTest.testInsertCsvTable`, `TestKafkaWritesSmokeTest.testInsertJsonTable`,
  `TestKafkaWritesSmokeTest.testInsertRawTable`, `TestKafkaWritesSmokeTest.testInsertSimpleKeyAndValue`.
- Observed differences:
  - current suite restores the legacy `EnvMultinodeKafka`, `EnvMultinodeConfluentKafka`,
    `EnvMultinodeKafkaSsl`, and `EnvMultinodeKafkaSaslPlaintext` coverage as explicit JUnit environment runs, while
    still keeping the current schema-registry split for the dedicated Confluent-license classes.
- Parity status: `verified`
