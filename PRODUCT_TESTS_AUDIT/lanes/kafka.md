# Lane Audit: Kafka

## Lane Summary

- Audit status: `manual-pass-complete`
- Section start commit: `Add Kafka environments`
- Section end commit: `Remove legacy SuiteKafka`
- Introduced JUnit suites: `SuiteKafka`.
- Extended existing suites: none.
- Retired legacy suites: `SuiteKafka`.
- Environment classes introduced: `KafkaEnvironment`.
- Method status counts: verified `20`, intentional difference `7`, needs follow-up `0`.

## Semantic Audit Status

- Method semantic audit: `complete`
- Environment semantic audit: `complete`
- Suite semantic audit: `complete`

## Lane-Level Open Questions / Intentional Differences

- Lane-wide approved difference set: none beyond the framework baseline documented in the reviewer guide.
- The prior suite-breadth gap is resolved on this branch: current `SuiteKafka` again reruns the Kafka-tagged smoke
  classes under the restored Confluent/basic, SSL, and SASL/PLAINTEXT environment variants in addition to the
  existing schema-registry and Confluent-license runs.
- Needs-follow-up methods: none currently identified in this source/history pass.

## Environment Semantic Audit
### `KafkaBasicEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: manually compared against `EnvMultinodeKafka`.
- Recorded differences:
  - legacy launcher environment configured both `kafka` and `kafka_schema_registry` catalogs on coordinator and worker
    containers and delegated topic/data seeding to Tempto requirements;
  - current environment runs outside Docker, starts Kafka/Schema Registry via `TestingKafka`, preloads table definition
    files into a single Trino container, and requires each JUnit method to create topics and seed messages explicitly.
- Reviewer note: basic Kafka query/write coverage is preserved, but the environment shape is simplified from a
  multinode launcher environment to a single Trino container plus direct topic/message setup.

### `KafkaSchemaRegistryEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: manually compared against `EnvMultinodeConfluentKafka`.
- Recorded differences:
  - legacy Confluent launcher environment reran the general `KAFKA` group and the
    `KAFKA_CONFLUENT_LICENSE` group together to verify that copying licensed protobuf jars did not break non-Confluent
    Kafka functionality;
  - current environment only backs schema-registry and Confluent-specific JUnit classes and does not rerun the basic
    Kafka smoke classes under the Confluent variant;
  - both shapes copy the Confluent protobuf jars into the Kafka plugin, but the current environment resolves them from
    the test runtime classpath instead of launcher-built artifacts.
- Reviewer note: schema-registry and Confluent-specific coverage remains present, and the legacy Confluent/basic rerun
  is now restored through `KafkaConfluentBasicEnvironment`.

### `KafkaEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: manually compared against the common behavior shared by `EnvMultinodeKafka`,
  `EnvMultinodeConfluentKafka`, `EnvMultinodeKafkaSsl`, and `EnvMultinodeKafkaSaslPlaintext`.
- Recorded differences:
  - current abstract environment exposes direct topic creation and message-send helpers to each test method;
  - legacy launcher environments encoded security/topology variants as separate environment classes rather than as
    direct helper APIs;
  - no current environment class exists for the SSL or SASL/PLAINTEXT variants that the legacy suite exercised.
- Reviewer note: the abstract environment now backs the full restored legacy environment matrix through dedicated basic,
  Confluent/basic, SSL, and SASL/PLAINTEXT subclasses.

## Suite Semantic Audit
### `SuiteKafka`
- Suite semantic audit status: `complete`
- CI bucket: `auth-and-clients`
- Relationship to lane: `owned by this lane`.
- Reviewer note: current `SuiteKafka` now preserves the legacy launcher suite's Confluent/basic rerun together with
  the SSL and SASL/PLAINTEXT environment runs.

## Ported Test Classes

### `TestKafkaAvroReadsSmokeTest`


- Owning migration commit: `Migrate TestKafkaAvroReadsSmokeTest to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaAvroReadsSmokeTest.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaAvroReadsSmokeTest.java`
- Class-level environment requirement: `KafkaBasicEnvironment`.
- Class-level tags: `Kafka`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `4`. Current methods: `3`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly. Cross-class or merged legacy
  coverage accounted for: `testAvroWithSchemaReferences` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaAvroReadsSchemaRegistrySmokeTest.java` ->
  `testAvroWithSchemaReferences`.
- Intentional differences summary: None identified at class scope.
- Method statuses present: `verified`.

#### Methods

##### `testSelectPrimitiveDataType`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaAvroReadsSmokeTest.java` ->
  `testSelectPrimitiveDataType`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaAvroReadsSmokeTest.java` ->
  `TestKafkaAvroReadsSmokeTest.testSelectPrimitiveDataType`
- Mapping type: `direct`
- Environment parity: Current class requires `KafkaBasicEnvironment`. Routed by source review into `SuiteKafka` run 1.
- Tag parity: Current tags: `Kafka`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createAvroTable`. Current setup shape: `env.createTopic`, `env.createTrinoConnection`, `conn.createStatement`.
- Action parity: Legacy action shape: `SELECT`, `ImmutableMap.of`, `kafkaCatalog.topicNameSuffix`, `kafkaCatalog.messageSerializer`, `uration`, `onTrino`, `executeQuery`, `format`, `kafkaCatalog.catalogName`. Current action shape: `SELECT`, `loadAvroSchema`, `enericData.Record`, `record.put`, `serializeAvro`, `sendMessage`, `Thread.sleep`, `stmt.executeQuery`, `rs.next`, `rs.getString`, `rs.getLong`, `rs.getDouble`, `rs.getBoolean`.
- Assertion parity: Legacy assertion helpers: `assertEventually`, `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `isTrue`, `isEqualTo`, `isFalse`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [ImmutableMap.of, kafkaCatalog.topicNameSuffix, createAvroTable, kafkaCatalog.messageSerializer, uration, onTrino, executeQuery, format, kafkaCatalog.catalogName] vs current [env.createTopic, loadAvroSchema, enericData.Record, record.put, serializeAvro, sendMessage, Thread.sleep, env.createTrinoConnection, conn.createStatement, stmt.executeQuery, rs.next, rs.getString, rs.getLong, rs.getDouble, rs.getBoolean]; assertion helpers differ: legacy [assertEventually, assertThat, containsOnly, row] vs current [assertThat, isTrue, isEqualTo, isFalse]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [ImmutableMap.of, kafkaCatalog.topicNameSuffix, createAvroTable, kafkaCatalog.messageSerializer, uration, onTrino, executeQuery, format, kafkaCatalog.catalogName], verbs [SELECT]. Current flow summary -> helpers [env.createTopic, loadAvroSchema, enericData.Record, record.put, serializeAvro, sendMessage, Thread.sleep, env.createTrinoConnection, conn.createStatement, stmt.executeQuery, rs.next, rs.getString, rs.getLong, rs.getDouble, rs.getBoolean], verbs [SELECT].
- Audit status: `verified`

##### `testNullType`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaAvroReadsSmokeTest.java` ->
  `testNullType`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaAvroReadsSmokeTest.java` ->
  `TestKafkaAvroReadsSmokeTest.testNullType`
- Mapping type: `direct`
- Environment parity: Current class requires `KafkaBasicEnvironment`. Routed by source review into `SuiteKafka` run 1.
- Tag parity: Current tags: `Kafka`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createAvroTable`. Current setup shape: `env.createTopic`, `env.createTrinoConnection`, `conn.createStatement`.
- Action parity: Legacy action shape: `SELECT`, `kafkaCatalog.topicNameSuffix`, `ImmutableMap.of`, `kafkaCatalog.messageSerializer`, `uration`, `onTrino`, `executeQuery`, `format`, `kafkaCatalog.catalogName`. Current action shape: `SELECT`, `loadAvroSchema`, `enericData.Record`, `serializeAvro`, `sendMessage`, `Thread.sleep`, `stmt.executeQuery`, `rs.next`, `rs.getObject`.
- Assertion parity: Legacy assertion helpers: `assertEventually`, `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `isTrue`, `isNull`, `isFalse`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [kafkaCatalog.topicNameSuffix, createAvroTable, ImmutableMap.of, kafkaCatalog.messageSerializer, uration, onTrino, executeQuery, format, kafkaCatalog.catalogName] vs current [env.createTopic, loadAvroSchema, enericData.Record, serializeAvro, sendMessage, Thread.sleep, env.createTrinoConnection, conn.createStatement, stmt.executeQuery, rs.next, rs.getObject]; assertion helpers differ: legacy [assertEventually, assertThat, containsOnly, row] vs current [assertThat, isTrue, isNull, isFalse]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [kafkaCatalog.topicNameSuffix, createAvroTable, ImmutableMap.of, kafkaCatalog.messageSerializer, uration, onTrino, executeQuery, format, kafkaCatalog.catalogName], verbs [SELECT]. Current flow summary -> helpers [env.createTopic, loadAvroSchema, enericData.Record, serializeAvro, sendMessage, Thread.sleep, env.createTrinoConnection, conn.createStatement, stmt.executeQuery, rs.next, rs.getObject], verbs [SELECT].
- Audit status: `verified`

##### `testSelectStructuralDataType`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaAvroReadsSmokeTest.java` ->
  `testSelectStructuralDataType`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaAvroReadsSmokeTest.java` ->
  `TestKafkaAvroReadsSmokeTest.testSelectStructuralDataType`
- Mapping type: `direct`
- Environment parity: Current class requires `KafkaBasicEnvironment`. Routed by source review into `SuiteKafka` run 1.
- Tag parity: Current tags: `Kafka`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createAvroTable`. Current setup shape: `env.createTopic`, `env.createTrinoConnection`, `conn.createStatement`.
- Action parity: Legacy action shape: `SELECT`, `ImmutableMap.of`, `ImmutableList.of`, `kafkaCatalog.topicNameSuffix`, `kafkaCatalog.messageSerializer`, `uration`, `onTrino`, `executeQuery`, `format`, `FROM`, `kafkaCatalog.columnMappingSupported`, `kafkaCatalog.catalogName`. Current action shape: `SELECT`, `loadAvroSchema`, `enericData.Record`, `record.put`, `List.of`, `Map.of`, `serializeAvro`, `sendMessage`, `Thread.sleep`, `stmt.executeQuery`, `FROM`, `rs.next`, `rs.getLong`, `rs.getString`.
- Assertion parity: Legacy assertion helpers: `assertEventually`, `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `isTrue`, `isEqualTo`, `isFalse`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [ImmutableMap.of, ImmutableList.of, kafkaCatalog.topicNameSuffix, createAvroTable, kafkaCatalog.messageSerializer, uration, onTrino, executeQuery, format, FROM, kafkaCatalog.columnMappingSupported, kafkaCatalog.catalogName] vs current [env.createTopic, loadAvroSchema, enericData.Record, record.put, List.of, Map.of, serializeAvro, sendMessage, Thread.sleep, env.createTrinoConnection, conn.createStatement, stmt.executeQuery, FROM, rs.next, rs.getLong, rs.getString]; assertion helpers differ: legacy [assertEventually, assertThat, containsOnly, row] vs current [assertThat, isTrue, isEqualTo, isFalse]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [ImmutableMap.of, ImmutableList.of, kafkaCatalog.topicNameSuffix, createAvroTable, kafkaCatalog.messageSerializer, uration, onTrino, executeQuery, format, FROM, kafkaCatalog.columnMappingSupported, kafkaCatalog.catalogName], verbs [SELECT]. Current flow summary -> helpers [env.createTopic, loadAvroSchema, enericData.Record, record.put, List.of, Map.of, serializeAvro, sendMessage, Thread.sleep, env.createTrinoConnection, conn.createStatement, stmt.executeQuery, FROM, rs.next, rs.getLong, rs.getString], verbs [SELECT].
- Audit status: `verified`

### `TestKafkaAvroReadsSchemaRegistrySmokeTest`


- Owning migration commit: `Add TestKafkaAvroReadsSchemaRegistrySmokeTest JUnit coverage`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaAvroReadsSchemaRegistrySmokeTest.java`
- Legacy class removed in same migration commit:
    - None. This commit added new JUnit-side verification coverage rather than removing a legacy class.
- Class-level environment requirement: `KafkaSchemaRegistryEnvironment`.
- Class-level tags: `Kafka`, `KafkaSchemaRegistry`, `ProfileSpecificTests`.
- Method inventory complete: Not applicable. No legacy class or resource source exists for this new verification
  coverage.
- Legacy helper/resource dependencies accounted for: New JUnit-side verification coverage without a removed legacy
  counterpart.
- Intentional differences summary: `JUnit/AssertJ/Testcontainers framework replacement`
- Method statuses present: `intentional difference`.

#### Methods

##### `testSelectPrimitiveDataType`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaAvroReadsSchemaRegistrySmokeTest.java` ->
  `TestKafkaAvroReadsSchemaRegistrySmokeTest.testSelectPrimitiveDataType`
- Mapping type: `new coverage helper`
- Environment parity: Current class requires `KafkaSchemaRegistryEnvironment`. Routed by source review into `SuiteKafka` run 2.
- Tag parity: Current tags: `Kafka`, `KafkaSchemaRegistry`, `ProfileSpecificTests`. Tag routing matches the current
  suite selection.
- Setup parity: No legacy counterpart. Current setup shape: `env.createTopic`, `env.createTrinoConnection`, `conn.createStatement`.
- Action parity: No legacy counterpart. Current action shape: `SELECT`, `loadAvroSchema`, `enericData.Record`, `record.put`, `serializeAvroSchemaRegistry`, `sendMessage`, `Thread.sleep`, `stmt.executeQuery`, `rs.next`, `names`, `rs.getString`, `rs.getLong`, `rs.getDouble`, `rs.getBoolean`.
- Assertion parity: No legacy counterpart. Current assertion helpers: `assertThat`, `isTrue`, `isEqualTo`, `isFalse`.
- Cleanup parity: No legacy counterpart. Current cleanup shape: none.
- Any observed difference, however small: Current-only verification coverage; no legacy parity comparison applies.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Current-only flow summary -> helpers [env.createTopic, loadAvroSchema, enericData.Record, record.put, serializeAvroSchemaRegistry, sendMessage, Thread.sleep, env.createTrinoConnection, conn.createStatement, stmt.executeQuery, rs.next, names, rs.getString, rs.getLong, rs.getDouble, rs.getBoolean], verbs [SELECT].
- Audit status: `intentional difference`

##### `testNullType`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaAvroReadsSmokeTest.java` ->
  `testNullType`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaAvroReadsSchemaRegistrySmokeTest.java` ->
  `TestKafkaAvroReadsSchemaRegistrySmokeTest.testNullType`
- Mapping type: `new coverage helper`
- Environment parity: Current class requires `KafkaSchemaRegistryEnvironment`. Routed by source review into `SuiteKafka` run 2.
- Tag parity: Current tags: `Kafka`, `KafkaSchemaRegistry`, `ProfileSpecificTests`. Tag routing matches the current
  suite selection.
- Setup parity: No legacy counterpart. Current setup shape: `env.createTopic`, `env.createTrinoConnection`, `conn.createStatement`.
- Action parity: No legacy counterpart. Current action shape: `SELECT`, `loadAvroSchema`, `enericData.Record`, `serializeAvroSchemaRegistry`, `sendMessage`, `Thread.sleep`, `stmt.executeQuery`, `rs.next`, `rs.getObject`.
- Assertion parity: No legacy counterpart. Current assertion helpers: `assertThat`, `isTrue`, `isNull`, `isFalse`.
- Cleanup parity: No legacy counterpart. Current cleanup shape: none.
- Any observed difference, however small: Current-only verification coverage; no legacy parity comparison applies.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Current-only flow summary -> helpers [env.createTopic, loadAvroSchema, enericData.Record, serializeAvroSchemaRegistry, sendMessage, Thread.sleep, env.createTrinoConnection, conn.createStatement, stmt.executeQuery, rs.next, rs.getObject], verbs [SELECT].
- Audit status: `intentional difference`

##### `testSelectStructuralDataType`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaAvroReadsSchemaRegistrySmokeTest.java` ->
  `TestKafkaAvroReadsSchemaRegistrySmokeTest.testSelectStructuralDataType`
- Mapping type: `new coverage helper`
- Environment parity: Current class requires `KafkaSchemaRegistryEnvironment`. Routed by source review into `SuiteKafka` run 2.
- Tag parity: Current tags: `Kafka`, `KafkaSchemaRegistry`, `ProfileSpecificTests`. Tag routing matches the current
  suite selection.
- Setup parity: No legacy counterpart. Current setup shape: `env.createTopic`, `env.createTrinoConnection`, `conn.createStatement`.
- Action parity: No legacy counterpart. Current action shape: `SELECT`, `loadAvroSchema`, `enericData.Record`, `record.put`, `List.of`, `Map.of`, `serializeAvroSchemaRegistry`, `sendMessage`, `Thread.sleep`, `stmt.executeQuery`, `FROM`, `rs.next`, `rs.getLong`, `rs.getString`.
- Assertion parity: No legacy counterpart. Current assertion helpers: `assertThat`, `isTrue`, `isEqualTo`, `isFalse`.
- Cleanup parity: No legacy counterpart. Current cleanup shape: none.
- Any observed difference, however small: Current-only verification coverage; no legacy parity comparison applies.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Current-only flow summary -> helpers [env.createTopic, loadAvroSchema, enericData.Record, record.put, List.of, Map.of, serializeAvroSchemaRegistry, sendMessage, Thread.sleep, env.createTrinoConnection, conn.createStatement, stmt.executeQuery, FROM, rs.next, rs.getLong, rs.getString], verbs [SELECT].
- Audit status: `intentional difference`

##### `testAvroWithSchemaReferences`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaAvroReadsSmokeTest.java` ->
  `testAvroWithSchemaReferences`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaAvroReadsSchemaRegistrySmokeTest.java` ->
  `TestKafkaAvroReadsSchemaRegistrySmokeTest.testAvroWithSchemaReferences`
- Mapping type: `new coverage helper`
- Environment parity: Current class requires `KafkaSchemaRegistryEnvironment`. Routed by source review into `SuiteKafka` run 2.
- Tag parity: Current tags: `Kafka`, `KafkaSchemaRegistry`, `ProfileSpecificTests`. Tag routing matches the current
  suite selection.
- Setup parity: No legacy counterpart. Current setup shape: `env.createTopic`, `env.createTrinoConnection`, `conn.createStatement`.
- Action parity: No legacy counterpart. Current action shape: `SELECT`, `loadAvroSchema`, `vroSchema`, `env.getSchemaRegistryClient`, `register`, `enericData.Record`, `innerRecord.put`, `loadAvroSchemaWithReferences`, `outerRecord.put`, `chemaReference`, `referredSchema.getName`, `schemaWithReferences.toString`, `ImmutableList.of`, `ImmutableMap.of`, `serializeAvroSchemaRegistryWithRefs`, `sendMessage`, `Thread.sleep`, `stmt.executeQuery`, `rs.next`, `rs.getString`, `rs.getDouble`.
- Assertion parity: No legacy counterpart. Current assertion helpers: `assertThat`, `isTrue`, `isEqualTo`, `isFalse`.
- Cleanup parity: No legacy counterpart. Current cleanup shape: none.
- Any observed difference, however small: Current-only verification coverage; no legacy parity comparison applies.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Current-only flow summary -> helpers [env.createTopic, loadAvroSchema, vroSchema, env.getSchemaRegistryClient, register, enericData.Record, innerRecord.put, loadAvroSchemaWithReferences, outerRecord.put, chemaReference, referredSchema.getName, schemaWithReferences.toString, ImmutableList.of, ImmutableMap.of, serializeAvroSchemaRegistryWithRefs, sendMessage, Thread.sleep, env.createTrinoConnection, conn.createStatement, stmt.executeQuery, rs.next, rs.getString, rs.getDouble], verbs [SELECT].
- Audit status: `intentional difference`

### `TestKafkaAvroWritesSmokeTest`


- Owning migration commit: `Migrate TestKafkaAvroWritesSmokeTest to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaAvroWritesSmokeTest.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaAvroWritesSmokeTest.java`
- Class-level environment requirement: `KafkaBasicEnvironment`.
- Class-level tags: `Kafka`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `2`. Current methods: `2`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: None identified at class scope.
- Method statuses present: `verified`.

#### Methods

##### `testInsertPrimitiveDataType`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaAvroWritesSmokeTest.java` ->
  `testInsertPrimitiveDataType`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaAvroWritesSmokeTest.java` ->
  `TestKafkaAvroWritesSmokeTest.testInsertPrimitiveDataType`
- Mapping type: `direct`
- Environment parity: Current class requires `KafkaBasicEnvironment`. Routed by source review into `SuiteKafka` run 1.
- Tag parity: Current tags: `Kafka`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `INSERT`. Current setup shape: `INSERT`, `env.createTopic`, `env.createTrinoConnection`, `conn.createStatement`.
- Action parity: Legacy action shape: `SELECT`, `onTrino`, `executeQuery`, `format`, `updatedRowsCountIsEqualTo`. Current action shape: `SELECT`, `stmt.executeUpdate`, `Thread.sleep`, `stmt.executeQuery`, `collectRowsNullable`, `containsExactlyInAnyOrder`, `List.of`, `nullList`, `listWithNulls`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, format, updatedRowsCountIsEqualTo] vs current [env.createTopic, env.createTrinoConnection, conn.createStatement, stmt.executeUpdate, Thread.sleep, stmt.executeQuery, collectRowsNullable, containsExactlyInAnyOrder, List.of, nullList, listWithNulls]; assertion helpers differ: legacy [assertThat, containsOnly, row] vs current [assertThat, isEqualTo]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, format, updatedRowsCountIsEqualTo], verbs [INSERT, SELECT]. Current flow summary -> helpers [env.createTopic, env.createTrinoConnection, conn.createStatement, stmt.executeUpdate, Thread.sleep, stmt.executeQuery, collectRowsNullable, containsExactlyInAnyOrder, List.of, nullList, listWithNulls], verbs [INSERT, SELECT].
- Audit status: `verified`

##### `testInsertStructuralDataType`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaAvroWritesSmokeTest.java` ->
  `testInsertStructuralDataType`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaAvroWritesSmokeTest.java` ->
  `TestKafkaAvroWritesSmokeTest.testInsertStructuralDataType`
- Mapping type: `direct`
- Environment parity: Current class requires `KafkaBasicEnvironment`. Routed by source review into `SuiteKafka` run 1.
- Tag parity: Current tags: `Kafka`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `INSERT`. Current setup shape: `INSERT`, `env.createTopic`, `env.createTrinoConnection`, `conn.createStatement`.
- Action parity: Legacy action shape: `onTrino`, `executeQuery`, `format`, `map_from_entries`. Current action shape: `stmt.executeUpdate`, `map_from_entries`, `array`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageMatching`. Current assertion helpers: `assertThatThrownBy`, `isInstanceOf`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, format, map_from_entries] vs current [env.createTopic, env.createTrinoConnection, conn.createStatement, stmt.executeUpdate, map_from_entries, array]; assertion helpers differ: legacy [assertQueryFailure, hasMessageMatching] vs current [assertThatThrownBy, isInstanceOf, hasMessageContaining]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, format, map_from_entries], verbs [INSERT]. Current flow summary -> helpers [env.createTopic, env.createTrinoConnection, conn.createStatement, stmt.executeUpdate, map_from_entries, array], verbs [INSERT].
- Audit status: `verified`

### `TestKafkaProtobufReadsSmokeTest`


- Owning migration commit: `Migrate TestKafkaProtobufReadsSmokeTest to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaProtobufReadsSmokeTest.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaProtobufReadsSmokeTest.java`
- Class-level environment requirement: `KafkaBasicEnvironment`.
- Class-level tags: `Kafka`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `5`. Current methods: `2`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly. Cross-class or merged legacy
  coverage accounted for: `testProtobufWithSchemaReferences` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaProtobufReadsSchemaRegistrySmokeTest.java` ->
  `testProtobufWithSchemaReferences`; `testSelectPrimitiveDataTypeWithSchemaRegistry` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaProtobufReadsSchemaRegistrySmokeTest.java` ->
  `testSelectPrimitiveDataType`; `testSelectStructuralDataTypeWithSchemaRegistry` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaProtobufReadsSchemaRegistrySmokeTest.java` ->
  `testSelectStructuralDataType`.
- Intentional differences summary: None identified at class scope.
- Method statuses present: `verified`.

#### Methods

##### `testSelectPrimitiveDataType`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaProtobufReadsSmokeTest.java` ->
  `testSelectPrimitiveDataType`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaProtobufReadsSmokeTest.java` ->
  `TestKafkaProtobufReadsSmokeTest.testSelectPrimitiveDataType`
- Mapping type: `direct`
- Environment parity: Current class requires `KafkaBasicEnvironment`. Routed by source review into `SuiteKafka` run 1.
- Tag parity: Current tags: `Kafka`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: `env.createTopic`, `env.createTrinoConnection`, `conn.createStatement`.
- Action parity: Legacy action shape: `selectPrimitiveDataType`. Current action shape: `SELECT`, `loadProtobufSchema`, `builder`, `put`, `buildOrThrow`, `serializeProtobuf`, `sendMessage`, `Thread.sleep`, `stmt.executeQuery`, `rs.next`, `rs.getString`, `rs.getInt`, `rs.getLong`, `rs.getDouble`, `rs.getFloat`, `rs.getBoolean`. Legacy delegate calls: `selectPrimitiveDataType`. Current delegate calls: none.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: `assertThat`, `isTrue`, `isEqualTo`, `isFalse`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [selectPrimitiveDataType] vs current [none]; helper calls differ: legacy [selectPrimitiveDataType] vs current [env.createTopic, loadProtobufSchema, builder, put, buildOrThrow, serializeProtobuf, sendMessage, Thread.sleep, env.createTrinoConnection, conn.createStatement, stmt.executeQuery, rs.next, rs.getString, rs.getInt, rs.getLong, rs.getDouble, rs.getFloat, rs.getBoolean]; SQL verbs differ: legacy [none] vs current [SELECT]; assertion helpers differ: legacy [none] vs current [assertThat, isTrue, isEqualTo, isFalse]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [selectPrimitiveDataType], verbs [none]. Current flow summary -> helpers [env.createTopic, loadProtobufSchema, builder, put, buildOrThrow, serializeProtobuf, sendMessage, Thread.sleep, env.createTrinoConnection, conn.createStatement, stmt.executeQuery, rs.next, rs.getString, rs.getInt, rs.getLong, rs.getDouble, rs.getFloat, rs.getBoolean], verbs [SELECT].
- Audit status: `verified`

##### `testSelectStructuralDataType`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaProtobufReadsSmokeTest.java` ->
  `testSelectStructuralDataType`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaProtobufReadsSmokeTest.java` ->
  `TestKafkaProtobufReadsSmokeTest.testSelectStructuralDataType`
- Mapping type: `direct`
- Environment parity: Current class requires `KafkaBasicEnvironment`. Routed by source review into `SuiteKafka` run 1.
- Tag parity: Current tags: `Kafka`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: `env.createTopic`, `env.createTrinoConnection`, `conn.createStatement`.
- Action parity: Legacy action shape: `selectStructuralDataType`. Current action shape: `SELECT`, `loadProtobufSchema`, `ImmutableMap.of`, `ImmutableList.of`, `serializeProtobuf`, `sendMessage`, `Thread.sleep`, `stmt.executeQuery`, `FROM`, `rs.next`, `rs.getLong`, `rs.getDouble`. Legacy delegate calls: `selectStructuralDataType`. Current delegate calls: none.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: `assertThat`, `isTrue`, `isEqualTo`, `isFalse`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [selectStructuralDataType] vs current [none]; helper calls differ: legacy [selectStructuralDataType] vs current [env.createTopic, loadProtobufSchema, ImmutableMap.of, ImmutableList.of, serializeProtobuf, sendMessage, Thread.sleep, env.createTrinoConnection, conn.createStatement, stmt.executeQuery, FROM, rs.next, rs.getLong, rs.getDouble]; SQL verbs differ: legacy [none] vs current [SELECT]; assertion helpers differ: legacy [none] vs current [assertThat, isTrue, isEqualTo, isFalse]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [selectStructuralDataType], verbs [none]. Current flow summary -> helpers [env.createTopic, loadProtobufSchema, ImmutableMap.of, ImmutableList.of, serializeProtobuf, sendMessage, Thread.sleep, env.createTrinoConnection, conn.createStatement, stmt.executeQuery, FROM, rs.next, rs.getLong, rs.getDouble], verbs [SELECT].
- Audit status: `verified`

### `TestKafkaProtobufReadsSchemaRegistrySmokeTest`


- Owning migration commit: `Add TestKafkaProtobufReadsSchemaRegistrySmokeTest JUnit coverage`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaProtobufReadsSchemaRegistrySmokeTest.java`
- Legacy class removed in same migration commit:
    - None. This commit added new JUnit-side verification coverage rather than removing a legacy class.
- Class-level environment requirement: `KafkaSchemaRegistryEnvironment`.
- Class-level tags: `KafkaConfluentLicense`, `KafkaSchemaRegistry`, `ProfileSpecificTests`.
- Method inventory complete: Not applicable. No legacy class or resource source exists for this new verification
  coverage.
- Legacy helper/resource dependencies accounted for: New JUnit-side verification coverage without a removed legacy
  counterpart.
- Intentional differences summary: `JUnit/AssertJ/Testcontainers framework replacement`
- Method statuses present: `intentional difference`.

#### Methods

##### `testSelectPrimitiveDataType`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaProtobufReadsSmokeTest.java` ->
  `testSelectPrimitiveDataTypeWithSchemaRegistry`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaProtobufReadsSchemaRegistrySmokeTest.java` ->
  `TestKafkaProtobufReadsSchemaRegistrySmokeTest.testSelectPrimitiveDataType`
- Mapping type: `moved`
- Environment parity: Current class requires `KafkaSchemaRegistryEnvironment`. Routed by source review into `SuiteKafka` run 3.
- Tag parity: Current tags: `KafkaConfluentLicense`, `KafkaSchemaRegistry`, `ProfileSpecificTests`. Tag routing matches
  the current suite selection.
- Setup parity: No legacy counterpart. Current setup shape: `env.createTopic`, `env.createTrinoConnection`, `conn.createStatement`.
- Action parity: No legacy counterpart. Current action shape: `SELECT`, `loadProtobufSchema`, `builder`, `put`, `buildOrThrow`, `serializeProtobufSchemaRegistry`, `sendMessage`, `Thread.sleep`, `stmt.executeQuery`, `rs.next`, `names`, `rs.getString`, `rs.getInt`, `rs.getLong`, `rs.getDouble`, `rs.getFloat`, `rs.getBoolean`.
- Assertion parity: No legacy counterpart. Current assertion helpers: `assertThat`, `isTrue`, `isEqualTo`, `isFalse`.
- Cleanup parity: No legacy counterpart. Current cleanup shape: none.
- Any observed difference, however small: Current-only verification coverage; no legacy parity comparison applies.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Current-only flow summary -> helpers [env.createTopic, loadProtobufSchema, builder, put, buildOrThrow, serializeProtobufSchemaRegistry, sendMessage, Thread.sleep, env.createTrinoConnection, conn.createStatement, stmt.executeQuery, rs.next, names, rs.getString, rs.getInt, rs.getLong, rs.getDouble, rs.getFloat, rs.getBoolean], verbs [SELECT].
- Audit status: `intentional difference`

##### `testSelectStructuralDataType`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaProtobufReadsSmokeTest.java` ->
  `testSelectStructuralDataTypeWithSchemaRegistry`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaProtobufReadsSchemaRegistrySmokeTest.java` ->
  `TestKafkaProtobufReadsSchemaRegistrySmokeTest.testSelectStructuralDataType`
- Mapping type: `moved`
- Environment parity: Current class requires `KafkaSchemaRegistryEnvironment`. Routed by source review into `SuiteKafka` run 3.
- Tag parity: Current tags: `KafkaConfluentLicense`, `KafkaSchemaRegistry`, `ProfileSpecificTests`. Tag routing matches
  the current suite selection.
- Setup parity: No legacy counterpart. Current setup shape: `env.createTopic`, `env.createTrinoConnection`, `conn.createStatement`.
- Action parity: No legacy counterpart. Current action shape: `SELECT`, `loadProtobufSchema`, `ImmutableMap.of`, `ImmutableList.of`, `serializeProtobufSchemaRegistry`, `sendMessage`, `Thread.sleep`, `stmt.executeQuery`, `FROM`, `rs.next`, `rs.getLong`, `rs.getDouble`.
- Assertion parity: No legacy counterpart. Current assertion helpers: `assertThat`, `isTrue`, `isEqualTo`, `isFalse`.
- Cleanup parity: No legacy counterpart. Current cleanup shape: none.
- Any observed difference, however small: Current-only verification coverage; no legacy parity comparison applies.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Current-only flow summary -> helpers [env.createTopic, loadProtobufSchema, ImmutableMap.of, ImmutableList.of, serializeProtobufSchemaRegistry, sendMessage, Thread.sleep, env.createTrinoConnection, conn.createStatement, stmt.executeQuery, FROM, rs.next, rs.getLong, rs.getDouble], verbs [SELECT].
- Audit status: `intentional difference`

##### `testProtobufWithSchemaReferences`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaProtobufReadsSmokeTest.java` ->
  `testProtobufWithSchemaReferences`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaProtobufReadsSchemaRegistrySmokeTest.java` ->
  `TestKafkaProtobufReadsSchemaRegistrySmokeTest.testProtobufWithSchemaReferences`
- Mapping type: `new coverage helper`
- Environment parity: Current class requires `KafkaSchemaRegistryEnvironment`. Routed by source review into `SuiteKafka` run 3.
- Tag parity: Current tags: `KafkaConfluentLicense`, `KafkaSchemaRegistry`, `ProfileSpecificTests`. Tag routing matches
  the current suite selection.
- Setup parity: No legacy counterpart. Current setup shape: `env.createTopic`, `setSeconds`, `setNanos`, `env.createTrinoConnection`, `conn.createStatement`.
- Action parity: No legacy counterpart. Current action shape: `SELECT`, `rotobufSchema`, `Resources.toString`, `Resources.getResource`, `ImmutableList.of`, `ImmutableMap.of`, `env.getSchemaRegistryClient`, `register`, `loadResourceString`, `chemaReference`, `timestampSchema.name`, `timestampSchema.canonicalString`, `LocalDateTime.parse`, `Timestamp.newBuilder`, `timestamp.toEpochSecond`, `timestamp.getNano`, `build`, `builder`, `put`, `buildOrThrow`, `serializeProtobufSchemaRegistryWithRefs`, `sendMessage`, `Thread.sleep`, `stmt.executeQuery`, `rs.next`, `rs.getString`, `rs.getInt`, `rs.getLong`, `rs.getDouble`, `rs.getFloat`, `rs.getBoolean`, `rs.getTimestamp`, `Timestamp.valueOf`.
- Assertion parity: No legacy counterpart. Current assertion helpers: `assertThat`, `isTrue`, `isEqualTo`, `isFalse`.
- Cleanup parity: No legacy counterpart. Current cleanup shape: none.
- Any observed difference, however small: Current-only verification coverage; no legacy parity comparison applies.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Current-only flow summary -> helpers [env.createTopic, rotobufSchema, Resources.toString, Resources.getResource, ImmutableList.of, ImmutableMap.of, env.getSchemaRegistryClient, register, loadResourceString, chemaReference, timestampSchema.name, timestampSchema.canonicalString, LocalDateTime.parse, Timestamp.newBuilder, setSeconds, timestamp.toEpochSecond, setNanos, timestamp.getNano, build, builder, put, buildOrThrow, serializeProtobufSchemaRegistryWithRefs, sendMessage, Thread.sleep, env.createTrinoConnection, conn.createStatement, stmt.executeQuery, rs.next, rs.getString, rs.getInt, rs.getLong, rs.getDouble, rs.getFloat, rs.getBoolean, rs.getTimestamp, Timestamp.valueOf], verbs [SELECT].
- Audit status: `intentional difference`

### `TestKafkaProtobufWritesSmokeTest`


- Owning migration commit: `Migrate TestKafkaProtobufWritesSmokeTest to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaProtobufWritesSmokeTest.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaProtobufWritesSmokeTest.java`
- Class-level environment requirement: `KafkaBasicEnvironment`.
- Class-level tags: `Kafka`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `2`. Current methods: `2`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: None identified at class scope.
- Method statuses present: `verified`.

#### Methods

##### `testInsertAllDataType`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaProtobufWritesSmokeTest.java` ->
  `testInsertAllDataType`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaProtobufWritesSmokeTest.java` ->
  `TestKafkaProtobufWritesSmokeTest.testInsertAllDataType`
- Mapping type: `direct`
- Environment parity: Current class requires `KafkaBasicEnvironment`. Routed by source review into `SuiteKafka` run 1.
- Tag parity: Current tags: `Kafka`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `INSERT`, `createProtobufTable`. Current setup shape: `INSERT`, `env.createTopic`, `env.createTrinoConnection`, `conn.createStatement`.
- Action parity: Legacy action shape: `SELECT`, `onTrino`, `executeQuery`, `format`, `updatedRowsCountIsEqualTo`, `Timestamp.valueOf`, `s`, `VALUES`. Current action shape: `SELECT`, `stmt.executeUpdate`, `Thread.sleep`, `stmt.executeQuery`, `collectRows`, `containsExactlyInAnyOrder`, `List.of`, `Timestamp.valueOf`, `VALUES`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`, `assertQueryFailure`, `isInstanceOf`, `hasMessageMatching`. Current assertion helpers: `assertThat`, `isEqualTo`, `assertThatThrownBy`, `isInstanceOf`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [createProtobufTable, onTrino, executeQuery, format, updatedRowsCountIsEqualTo, Timestamp.valueOf, s, VALUES] vs current [env.createTopic, env.createTrinoConnection, conn.createStatement, stmt.executeUpdate, Thread.sleep, stmt.executeQuery, collectRows, containsExactlyInAnyOrder, List.of, Timestamp.valueOf, VALUES]; assertion helpers differ: legacy [assertThat, containsOnly, row, assertQueryFailure, isInstanceOf, hasMessageMatching] vs current [assertThat, isEqualTo, assertThatThrownBy, isInstanceOf, hasMessageContaining]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [createProtobufTable, onTrino, executeQuery, format, updatedRowsCountIsEqualTo, Timestamp.valueOf, s, VALUES], verbs [INSERT, SELECT]. Current flow summary -> helpers [env.createTopic, env.createTrinoConnection, conn.createStatement, stmt.executeUpdate, Thread.sleep, stmt.executeQuery, collectRows, containsExactlyInAnyOrder, List.of, Timestamp.valueOf, VALUES], verbs [INSERT, SELECT].
- Audit status: `verified`

##### `testInsertStructuralDataType`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaProtobufWritesSmokeTest.java` ->
  `testInsertStructuralDataType`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaProtobufWritesSmokeTest.java` ->
  `TestKafkaProtobufWritesSmokeTest.testInsertStructuralDataType`
- Mapping type: `direct`
- Environment parity: Current class requires `KafkaBasicEnvironment`. Routed by source review into `SuiteKafka` run 1.
- Tag parity: Current tags: `Kafka`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `INSERT`, `createProtobufTable`. Current setup shape: `INSERT`, `env.createTopic`, `env.createTrinoConnection`, `conn.createStatement`.
- Action parity: Legacy action shape: `SELECT`, `onTrino`, `executeQuery`, `format`, `CAST`, `ROW`, `map_from_entries`, `updatedRowsCountIsEqualTo`, `Timestamp.valueOf`. Current action shape: `SELECT`, `stmt.executeUpdate`, `CAST`, `ROW`, `map_from_entries`, `Thread.sleep`, `stmt.executeQuery`, `rs.next`, `rs.getString`, `rs.getDouble`, `rs.getFloat`, `rs.getInt`, `rs.getLong`, `rs.getTimestamp`, `Timestamp.valueOf`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `isEqualTo`, `isTrue`, `isFalse`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [createProtobufTable, onTrino, executeQuery, format, CAST, ROW, map_from_entries, updatedRowsCountIsEqualTo, Timestamp.valueOf] vs current [env.createTopic, env.createTrinoConnection, conn.createStatement, stmt.executeUpdate, CAST, ROW, map_from_entries, Thread.sleep, stmt.executeQuery, rs.next, rs.getString, rs.getDouble, rs.getFloat, rs.getInt, rs.getLong, rs.getTimestamp, Timestamp.valueOf]; assertion helpers differ: legacy [assertThat, containsOnly, row] vs current [assertThat, isEqualTo, isTrue, isFalse]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [createProtobufTable, onTrino, executeQuery, format, CAST, ROW, map_from_entries, updatedRowsCountIsEqualTo, Timestamp.valueOf], verbs [INSERT, SELECT]. Current flow summary -> helpers [env.createTopic, env.createTrinoConnection, conn.createStatement, stmt.executeUpdate, CAST, ROW, map_from_entries, Thread.sleep, stmt.executeQuery, rs.next, rs.getString, rs.getDouble, rs.getFloat, rs.getInt, rs.getLong, rs.getTimestamp, Timestamp.valueOf], verbs [INSERT, SELECT].
- Audit status: `verified`

### `TestKafkaPushdownSmokeTest`


- Owning migration commit: `Migrate TestKafkaPushdownSmokeTest to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaPushdownSmokeTest.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaPushdownSmokeTest.java`
- Class-level environment requirement: `KafkaBasicEnvironment`.
- Class-level tags: `Kafka`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `3`. Current methods: `3`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: None identified at class scope.
- Method statuses present: `verified`.

#### Methods

##### `testPartitionPushdown`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaPushdownSmokeTest.java` ->
  `testPartitionPushdown`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaPushdownSmokeTest.java` ->
  `TestKafkaPushdownSmokeTest.testPartitionPushdown`
- Mapping type: `direct`
- Environment parity: Current class requires `KafkaBasicEnvironment`. Routed by source review into `SuiteKafka` run 1.
- Tag parity: Current tags: `Kafka`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: `createTopicWithConfig`, `env.createTrinoConnection`, `conn.createStatement`.
- Action parity: Legacy action shape: `SELECT`, `onTrino`, `executeQuery`, `format`, `COUNT`. Current action shape: `SELECT`, `env.getKafka`, `String.valueOf`, `env.sendMessages`, `key.getBytes`, `value.getBytes`, `Thread.sleep`, `stmt.executeQuery`, `COUNT`, `rs.next`, `rs.getLong`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsExactlyInOrder`, `row`. Current assertion helpers: `assertThat`, `isTrue`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, format, COUNT] vs current [env.getKafka, createTopicWithConfig, String.valueOf, env.sendMessages, key.getBytes, value.getBytes, Thread.sleep, env.createTrinoConnection, conn.createStatement, stmt.executeQuery, COUNT, rs.next, rs.getLong]; assertion helpers differ: legacy [assertThat, containsExactlyInOrder, row] vs current [assertThat, isTrue, isEqualTo]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, format, COUNT], verbs [SELECT]. Current flow summary -> helpers [env.getKafka, createTopicWithConfig, String.valueOf, env.sendMessages, key.getBytes, value.getBytes, Thread.sleep, env.createTrinoConnection, conn.createStatement, stmt.executeQuery, COUNT, rs.next, rs.getLong], verbs [SELECT].
- Audit status: `verified`

##### `testOffsetPushdown`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaPushdownSmokeTest.java` ->
  `testOffsetPushdown`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaPushdownSmokeTest.java` ->
  `TestKafkaPushdownSmokeTest.testOffsetPushdown`
- Mapping type: `direct`
- Environment parity: Current class requires `KafkaBasicEnvironment`. Routed by source review into `SuiteKafka` run 1.
- Tag parity: Current tags: `Kafka`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: `createTopicWithConfig`, `env.createTrinoConnection`, `conn.createStatement`, `offsets`.
- Action parity: Legacy action shape: `SELECT`, `onTrino`, `executeQuery`, `format`, `COUNT`. Current action shape: `SELECT`, `env.getKafka`, `String.valueOf`, `env.sendMessages`, `key.getBytes`, `value.getBytes`, `Thread.sleep`, `stmt.executeQuery`, `COUNT`, `rs.next`, `rs.getLong`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsExactlyInOrder`, `row`. Current assertion helpers: `assertThat`, `isTrue`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, format, COUNT] vs current [env.getKafka, createTopicWithConfig, String.valueOf, env.sendMessages, key.getBytes, value.getBytes, Thread.sleep, env.createTrinoConnection, conn.createStatement, stmt.executeQuery, COUNT, rs.next, offsets, rs.getLong]; assertion helpers differ: legacy [assertThat, containsExactlyInOrder, row] vs current [assertThat, isTrue, isEqualTo]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, format, COUNT], verbs [SELECT]. Current flow summary -> helpers [env.getKafka, createTopicWithConfig, String.valueOf, env.sendMessages, key.getBytes, value.getBytes, Thread.sleep, env.createTrinoConnection, conn.createStatement, stmt.executeQuery, COUNT, rs.next, offsets, rs.getLong], verbs [SELECT].
- Audit status: `verified`

##### `testCreateTimePushdown`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaPushdownSmokeTest.java` ->
  `testCreateTimePushdown`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaPushdownSmokeTest.java` ->
  `TestKafkaPushdownSmokeTest.testCreateTimePushdown`
- Mapping type: `direct`
- Environment parity: Current class requires `KafkaBasicEnvironment`. Routed by source review into `SuiteKafka` run 1.
- Tag parity: Current tags: `Kafka`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `INSERT`. Current setup shape: `INSERT`, `env.createTopic`, `env.createTrinoConnection`, `conn.createStatement`.
- Action parity: Legacy action shape: `SELECT`, `onTrino`, `executeQuery`, `format`, `s`, `VALUES`, `Thread.sleep`, `CAST`, `IN`, `rows`, `rows.get`, `get`, `COUNT`. Current action shape: `SELECT`, `stmt.executeUpdate`, `VALUES`, `Thread.sleep`, `stmt.executeQuery`, `CAST`, `IN`, `rs.next`, `timestamps.add`, `rs.getString`, `timestamps.get`, `COUNT`, `rs.getLong`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsExactlyInOrder`, `row`. Current assertion helpers: `assertThat`, `hasSize`, `isTrue`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, format, s, VALUES, Thread.sleep, CAST, IN, rows, rows.get, get, COUNT] vs current [env.createTopic, env.createTrinoConnection, conn.createStatement, stmt.executeUpdate, VALUES, Thread.sleep, stmt.executeQuery, CAST, IN, rs.next, timestamps.add, rs.getString, timestamps.get, COUNT, rs.getLong]; assertion helpers differ: legacy [assertThat, containsExactlyInOrder, row] vs current [assertThat, hasSize, isTrue, isEqualTo]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, format, s, VALUES, Thread.sleep, CAST, IN, rows, rows.get, get, COUNT], verbs [INSERT, SELECT]. Current flow summary -> helpers [env.createTopic, env.createTrinoConnection, conn.createStatement, stmt.executeUpdate, VALUES, Thread.sleep, stmt.executeQuery, CAST, IN, rs.next, timestamps.add, rs.getString, timestamps.get, COUNT, rs.getLong], verbs [INSERT, SELECT].
- Audit status: `verified`

### `TestKafkaReadsSmokeTest`


- Owning migration commit: `Migrate TestKafkaReadsSmokeTest to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaReadsSmokeTest.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaReadsSmokeTest.java`
- Class-level environment requirement: `KafkaBasicEnvironment`.
- Class-level tags: `Kafka`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `4`. Current methods: `4`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: None identified at class scope.
- Method statuses present: `verified`.

#### Methods

##### `testSelectSimpleKeyAndValue`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaReadsSmokeTest.java` ->
  `testSelectSimpleKeyAndValue`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaReadsSmokeTest.java` ->
  `TestKafkaReadsSmokeTest.testSelectSimpleKeyAndValue`
- Mapping type: `direct`
- Environment parity: Current class requires `KafkaBasicEnvironment`. Routed by source review into `SuiteKafka` run 1.
- Tag parity: Current tags: `Kafka`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: `env.createTopic`, `env.createTrinoConnection`, `conn.createStatement`.
- Action parity: Legacy action shape: `SELECT`, `onTrino`, `executeQuery`, `format`. Current action shape: `SELECT`, `env.sendMessages`, `getBytes`, `Thread.sleep`, `stmt.executeQuery`, `collectRows`, `containsExactlyInAnyOrder`, `List.of`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, format] vs current [env.createTopic, env.sendMessages, getBytes, Thread.sleep, env.createTrinoConnection, conn.createStatement, stmt.executeQuery, collectRows, containsExactlyInAnyOrder, List.of]; assertion helpers differ: legacy [assertThat, containsOnly, row] vs current [assertThat]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, format], verbs [SELECT]. Current flow summary -> helpers [env.createTopic, env.sendMessages, getBytes, Thread.sleep, env.createTrinoConnection, conn.createStatement, stmt.executeQuery, collectRows, containsExactlyInAnyOrder, List.of], verbs [SELECT].
- Audit status: `verified`

##### `testSelectAllRawTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaReadsSmokeTest.java` ->
  `testSelectAllRawTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaReadsSmokeTest.java` ->
  `TestKafkaReadsSmokeTest.testSelectAllRawTable`
- Mapping type: `direct`
- Environment parity: Current class requires `KafkaBasicEnvironment`. Routed by source review into `SuiteKafka` run 1.
- Tag parity: Current tags: `Kafka`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: `env.createTopic`, `env.createTrinoConnection`, `conn.createStatement`.
- Action parity: Legacy action shape: `SELECT`, `onTrino`, `executeQuery`, `format`. Current action shape: `SELECT`, `allocate`, `order`, `put`, `getBytes`, `c_varchar`, `c_byte_bigint`, `putShort`, `c_short_bigint`, `putInt`, `c_int_bigint`, `putLong`, `c_long_bigint`, `c_byte_integer`, `c_short_integer`, `c_int_integer`, `c_byte_smallint`, `c_short_smallint`, `c_byte_tinyint`, `floatToIntBits`, `c_float_double`, `doubleToRawLongBits`, `c_double_double`, `c_byte_boolean`, `c_short_boolean`, `c_int_boolean`, `c_long_boolean`, `array`, `env.sendMessages`, `Thread.sleep`, `stmt.executeQuery`, `collectRows`, `containsExactlyInAnyOrder`, `List.of`, `rs.next`, `rs.getString`, `rs.getLong`, `rs.getInt`, `rs.getShort`, `rs.getByte`, `rs.getFloat`, `rs.getDouble`, `rs.getBoolean`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `isTrue`, `isEqualTo`, `isFalse`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, format] vs current [env.createTopic, allocate, order, put, getBytes, c_varchar, c_byte_bigint, putShort, c_short_bigint, putInt, c_int_bigint, putLong, c_long_bigint, c_byte_integer, c_short_integer, c_int_integer, c_byte_smallint, c_short_smallint, c_byte_tinyint, floatToIntBits, c_float_double, doubleToRawLongBits, c_double_double, c_byte_boolean, c_short_boolean, c_int_boolean, c_long_boolean, array, env.sendMessages, Thread.sleep, env.createTrinoConnection, conn.createStatement, stmt.executeQuery, collectRows, containsExactlyInAnyOrder, List.of, rs.next, rs.getString, rs.getLong, rs.getInt, rs.getShort, rs.getByte, rs.getFloat, rs.getDouble, rs.getBoolean]; assertion helpers differ: legacy [assertThat, containsOnly, row] vs current [assertThat, isTrue, isEqualTo, isFalse]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, format], verbs [SELECT]. Current flow summary -> helpers [env.createTopic, allocate, order, put, getBytes, c_varchar, c_byte_bigint, putShort, c_short_bigint, putInt, c_int_bigint, putLong, c_long_bigint, c_byte_integer, c_short_integer, c_int_integer, c_byte_smallint, c_short_smallint, c_byte_tinyint, floatToIntBits, c_float_double, doubleToRawLongBits, c_double_double, c_byte_boolean, c_short_boolean, c_int_boolean, c_long_boolean, array, env.sendMessages, Thread.sleep, env.createTrinoConnection, conn.createStatement, stmt.executeQuery, collectRows, containsExactlyInAnyOrder, List.of, rs.next, rs.getString, rs.getLong, rs.getInt, rs.getShort, rs.getByte, rs.getFloat, rs.getDouble, rs.getBoolean], verbs [SELECT].
- Audit status: `verified`

##### `testSelectAllCsvTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaReadsSmokeTest.java` ->
  `testSelectAllCsvTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaReadsSmokeTest.java` ->
  `TestKafkaReadsSmokeTest.testSelectAllCsvTable`
- Mapping type: `direct`
- Environment parity: Current class requires `KafkaBasicEnvironment`. Routed by source review into `SuiteKafka` run 1.
- Tag parity: Current tags: `Kafka`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: `env.createTopic`, `env.createTrinoConnection`, `conn.createStatement`.
- Action parity: Legacy action shape: `SELECT`, `onTrino`, `executeQuery`, `format`. Current action shape: `SELECT`, `env.sendMessages`, `getBytes`, `Thread.sleep`, `stmt.executeQuery`, `collectRows`, `containsExactlyInAnyOrder`, `List.of`, `collectRowsNullable`, `nullList`, `listWithNulls`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, format] vs current [env.createTopic, env.sendMessages, getBytes, Thread.sleep, env.createTrinoConnection, conn.createStatement, stmt.executeQuery, collectRows, containsExactlyInAnyOrder, List.of, collectRowsNullable, nullList, listWithNulls]; assertion helpers differ: legacy [assertThat, containsOnly, row] vs current [assertThat]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, format], verbs [SELECT]. Current flow summary -> helpers [env.createTopic, env.sendMessages, getBytes, Thread.sleep, env.createTrinoConnection, conn.createStatement, stmt.executeQuery, collectRows, containsExactlyInAnyOrder, List.of, collectRowsNullable, nullList, listWithNulls], verbs [SELECT].
- Audit status: `verified`

##### `testSelectAllJsonTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaReadsSmokeTest.java` ->
  `testSelectAllJsonTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaReadsSmokeTest.java` ->
  `TestKafkaReadsSmokeTest.testSelectAllJsonTable`
- Mapping type: `direct`
- Environment parity: Current class requires `KafkaBasicEnvironment`. Routed by source review into `SuiteKafka` run 1.
- Tag parity: Current tags: `Kafka`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: `env.createTopic`, `env.createTrinoConnection`, `conn.createStatement`.
- Action parity: Legacy action shape: `SELECT`, `onTrino`, `executeQuery`, `format`, `timestamp`, `time`, `Timestamp.valueOf`, `LocalDateTime.of`, `Date.valueOf`, `LocalDate.of`, `Time.valueOf`, `LocalTime.of`. Current action shape: `SELECT`, `env.sendMessages`, `jsonMessage.getBytes`, `Thread.sleep`, `stmt.executeQuery`, `collectRows`, `containsExactlyInAnyOrder`, `List.of`, `timestamp`, `time`, `rs.next`, `rs.getString`, `rs.getLong`, `rs.getInt`, `rs.getShort`, `rs.getByte`, `rs.getDouble`, `rs.getBoolean`, `rs.getTimestamp`, `Timestamp.valueOf`, `LocalDateTime.of`, `rs.getDate`, `Date.valueOf`, `LocalDate.of`, `rs.getTime`, `Time.valueOf`, `LocalTime.of`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `isTrue`, `isEqualTo`, `isFalse`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, format, timestamp, time, Timestamp.valueOf, LocalDateTime.of, Date.valueOf, LocalDate.of, Time.valueOf, LocalTime.of] vs current [env.createTopic, env.sendMessages, jsonMessage.getBytes, Thread.sleep, env.createTrinoConnection, conn.createStatement, stmt.executeQuery, collectRows, containsExactlyInAnyOrder, List.of, timestamp, time, rs.next, rs.getString, rs.getLong, rs.getInt, rs.getShort, rs.getByte, rs.getDouble, rs.getBoolean, rs.getTimestamp, Timestamp.valueOf, LocalDateTime.of, rs.getDate, Date.valueOf, LocalDate.of, rs.getTime, Time.valueOf, LocalTime.of]; assertion helpers differ: legacy [assertThat, containsOnly, row] vs current [assertThat, isTrue, isEqualTo, isFalse]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, format, timestamp, time, Timestamp.valueOf, LocalDateTime.of, Date.valueOf, LocalDate.of, Time.valueOf, LocalTime.of], verbs [SELECT]. Current flow summary -> helpers [env.createTopic, env.sendMessages, jsonMessage.getBytes, Thread.sleep, env.createTrinoConnection, conn.createStatement, stmt.executeQuery, collectRows, containsExactlyInAnyOrder, List.of, timestamp, time, rs.next, rs.getString, rs.getLong, rs.getInt, rs.getShort, rs.getByte, rs.getDouble, rs.getBoolean, rs.getTimestamp, Timestamp.valueOf, LocalDateTime.of, rs.getDate, Date.valueOf, LocalDate.of, rs.getTime, Time.valueOf, LocalTime.of], verbs [SELECT].
- Audit status: `verified`

### `TestKafkaWritesSmokeTest`


- Owning migration commit: `Migrate TestKafkaWritesSmokeTest to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaWritesSmokeTest.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaWritesSmokeTest.java`
- Class-level environment requirement: `KafkaBasicEnvironment`.
- Class-level tags: `Kafka`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `4`. Current methods: `4`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: None identified at class scope.
- Method statuses present: `verified`.

#### Methods

##### `testInsertSimpleKeyAndValue`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaWritesSmokeTest.java` ->
  `testInsertSimpleKeyAndValue`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaWritesSmokeTest.java` ->
  `TestKafkaWritesSmokeTest.testInsertSimpleKeyAndValue`
- Mapping type: `direct`
- Environment parity: Current class requires `KafkaBasicEnvironment`. Routed by source review into `SuiteKafka` run 1.
- Tag parity: Current tags: `Kafka`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `INSERT`. Current setup shape: `INSERT`, `env.createTopic`, `env.createTrinoConnection`, `conn.createStatement`.
- Action parity: Legacy action shape: `SELECT`, `onTrino`, `executeQuery`, `format`, `updatedRowsCountIsEqualTo`. Current action shape: `SELECT`, `stmt.executeUpdate`, `Thread.sleep`, `stmt.executeQuery`, `collectRows`, `containsExactlyInAnyOrder`, `List.of`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, format, updatedRowsCountIsEqualTo] vs current [env.createTopic, env.createTrinoConnection, conn.createStatement, stmt.executeUpdate, Thread.sleep, stmt.executeQuery, collectRows, containsExactlyInAnyOrder, List.of]; assertion helpers differ: legacy [assertThat, containsOnly, row] vs current [assertThat, isEqualTo]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, format, updatedRowsCountIsEqualTo], verbs [INSERT, SELECT]. Current flow summary -> helpers [env.createTopic, env.createTrinoConnection, conn.createStatement, stmt.executeUpdate, Thread.sleep, stmt.executeQuery, collectRows, containsExactlyInAnyOrder, List.of], verbs [INSERT, SELECT].
- Audit status: `verified`

##### `testInsertRawTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaWritesSmokeTest.java` ->
  `testInsertRawTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaWritesSmokeTest.java` ->
  `TestKafkaWritesSmokeTest.testInsertRawTable`
- Mapping type: `direct`
- Environment parity: Current class requires `KafkaBasicEnvironment`. Routed by source review into `SuiteKafka` run 1.
- Tag parity: Current tags: `Kafka`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `INSERT`. Current setup shape: `INSERT`, `env.createTopic`, `env.createTrinoConnection`, `conn.createStatement`.
- Action parity: Legacy action shape: `SELECT`, `write`, `onTrino`, `executeQuery`, `format`, `updatedRowsCountIsEqualTo`. Current action shape: `SELECT`, `stmt.executeUpdate`, `Thread.sleep`, `stmt.executeQuery`, `collectRows`, `containsExactlyInAnyOrder`, `List.of`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [write, onTrino, executeQuery, format, updatedRowsCountIsEqualTo] vs current [env.createTopic, env.createTrinoConnection, conn.createStatement, stmt.executeUpdate, Thread.sleep, stmt.executeQuery, collectRows, containsExactlyInAnyOrder, List.of]; assertion helpers differ: legacy [assertThat, containsOnly, row] vs current [assertThat, isEqualTo]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [write, onTrino, executeQuery, format, updatedRowsCountIsEqualTo], verbs [INSERT, SELECT]. Current flow summary -> helpers [env.createTopic, env.createTrinoConnection, conn.createStatement, stmt.executeUpdate, Thread.sleep, stmt.executeQuery, collectRows, containsExactlyInAnyOrder, List.of], verbs [INSERT, SELECT].
- Audit status: `verified`

##### `testInsertCsvTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaWritesSmokeTest.java` ->
  `testInsertCsvTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaWritesSmokeTest.java` ->
  `TestKafkaWritesSmokeTest.testInsertCsvTable`
- Mapping type: `direct`
- Environment parity: Current class requires `KafkaBasicEnvironment`. Routed by source review into `SuiteKafka` run 1.
- Tag parity: Current tags: `Kafka`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `INSERT`. Current setup shape: `INSERT`, `env.createTopic`, `env.createTrinoConnection`, `conn.createStatement`.
- Action parity: Legacy action shape: `SELECT`, `onTrino`, `executeQuery`, `format`, `updatedRowsCountIsEqualTo`. Current action shape: `SELECT`, `stmt.executeUpdate`, `Thread.sleep`, `stmt.executeQuery`, `collectRowsNullable`, `containsExactlyInAnyOrder`, `List.of`, `nullList`, `listWithNulls`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, format, updatedRowsCountIsEqualTo] vs current [env.createTopic, env.createTrinoConnection, conn.createStatement, stmt.executeUpdate, Thread.sleep, stmt.executeQuery, collectRowsNullable, containsExactlyInAnyOrder, List.of, nullList, listWithNulls]; assertion helpers differ: legacy [assertThat, containsOnly, row] vs current [assertThat, isEqualTo]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, format, updatedRowsCountIsEqualTo], verbs [INSERT, SELECT]. Current flow summary -> helpers [env.createTopic, env.createTrinoConnection, conn.createStatement, stmt.executeUpdate, Thread.sleep, stmt.executeQuery, collectRowsNullable, containsExactlyInAnyOrder, List.of, nullList, listWithNulls], verbs [INSERT, SELECT].
- Audit status: `verified`

##### `testInsertJsonTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/kafka/TestKafkaWritesSmokeTest.java` ->
  `testInsertJsonTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/kafka/TestKafkaWritesSmokeTest.java` ->
  `TestKafkaWritesSmokeTest.testInsertJsonTable`
- Mapping type: `direct`
- Environment parity: Current class requires `KafkaBasicEnvironment`. Routed by source review into `SuiteKafka` run 1.
- Tag parity: Current tags: `Kafka`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `INSERT`. Current setup shape: `INSERT`, `env.createTopic`, `env.createTrinoConnection`, `conn.createStatement`.
- Action parity: Legacy action shape: `SELECT`, `onTrino`, `executeQuery`, `format`, `VALUES`, `updatedRowsCountIsEqualTo`, `CAST`, `Timestamp.valueOf`, `LocalDateTime.of`, `Date.valueOf`, `LocalDate.of`, `Time.valueOf`, `LocalTime.of`. Current action shape: `SELECT`, `stmt.executeUpdate`, `VALUES`, `Thread.sleep`, `stmt.executeQuery`, `CAST`, `rs.next`, `rs.getString`, `rs.getLong`, `rs.getInt`, `rs.getShort`, `rs.getByte`, `rs.getDouble`, `rs.getBoolean`, `rs.getTimestamp`, `Timestamp.valueOf`, `LocalDateTime.of`, `rs.getDate`, `Date.valueOf`, `LocalDate.of`, `rs.getTime`, `Time.valueOf`, `LocalTime.of`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `isEqualTo`, `isTrue`, `isFalse`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, format, VALUES, updatedRowsCountIsEqualTo, CAST, Timestamp.valueOf, LocalDateTime.of, Date.valueOf, LocalDate.of, Time.valueOf, LocalTime.of] vs current [env.createTopic, env.createTrinoConnection, conn.createStatement, stmt.executeUpdate, VALUES, Thread.sleep, stmt.executeQuery, CAST, rs.next, rs.getString, rs.getLong, rs.getInt, rs.getShort, rs.getByte, rs.getDouble, rs.getBoolean, rs.getTimestamp, Timestamp.valueOf, LocalDateTime.of, rs.getDate, Date.valueOf, LocalDate.of, rs.getTime, Time.valueOf, LocalTime.of]; assertion helpers differ: legacy [assertThat, containsOnly, row] vs current [assertThat, isEqualTo, isTrue, isFalse]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, format, VALUES, updatedRowsCountIsEqualTo, CAST, Timestamp.valueOf, LocalDateTime.of, Date.valueOf, LocalDate.of, Time.valueOf, LocalTime.of], verbs [INSERT, SELECT]. Current flow summary -> helpers [env.createTopic, env.createTrinoConnection, conn.createStatement, stmt.executeUpdate, VALUES, Thread.sleep, stmt.executeQuery, CAST, rs.next, rs.getString, rs.getLong, rs.getInt, rs.getShort, rs.getByte, rs.getDouble, rs.getBoolean, rs.getTimestamp, Timestamp.valueOf, LocalDateTime.of, rs.getDate, Date.valueOf, LocalDate.of, rs.getTime, Time.valueOf, LocalTime.of], verbs [INSERT, SELECT].
- Audit status: `verified`
