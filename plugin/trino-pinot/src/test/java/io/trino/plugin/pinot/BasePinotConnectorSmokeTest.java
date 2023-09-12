/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.pinot;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.trino.Session;
import io.trino.plugin.pinot.client.PinotHostMapper;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.kafka.TestingKafka;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.segment.local.recordtransformer.CompositeTransformer;
import org.apache.pinot.segment.local.recordtransformer.RecordTransformer;
import org.apache.pinot.segment.local.segment.creator.RecordReaderSegmentCreationDataSource;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentCreationDataSource;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.name.NormalizedDateSegmentNameGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.Test;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.trino.plugin.pinot.PinotQueryRunner.createPinotQueryRunner;
import static io.trino.plugin.pinot.TestingPinotCluster.PINOT_PREVIOUS_IMAGE_NAME;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.joining;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.pinot.spi.utils.JsonUtils.inputStreamToObject;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.testng.Assert.assertEquals;

public abstract class BasePinotConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    private static final int MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES = 11;
    private static final int MAX_ROWS_PER_SPLIT_FOR_BROKER_QUERIES = 12;
    // If a broker query does not supply a limit, pinot defaults to 10 rows
    private static final int DEFAULT_PINOT_LIMIT_FOR_BROKER_QUERIES = 10;
    private static final String ALL_TYPES_TABLE = "alltypes";
    private static final String DATE_TIME_FIELDS_TABLE = "date_time_fields";
    private static final String MIXED_CASE_COLUMN_NAMES_TABLE = "mixed_case";
    private static final String MIXED_CASE_DISTINCT_TABLE = "mixed_case_distinct";
    private static final String TOO_MANY_ROWS_TABLE = "too_many_rows";
    private static final String TOO_MANY_BROKER_ROWS_TABLE = "too_many_broker_rows";
    private static final String MIXED_CASE_TABLE_NAME = "mixedCase";
    private static final String HYBRID_TABLE_NAME = "hybrid";
    private static final String DUPLICATE_TABLE_LOWERCASE = "dup_table";
    private static final String DUPLICATE_TABLE_MIXED_CASE = "dup_Table";
    private static final String JSON_TABLE = "my_table";
    private static final String JSON_TYPE_TABLE = "json_type_table";
    private static final String RESERVED_KEYWORD_TABLE = "reserved_keyword";
    private static final String QUOTES_IN_COLUMN_NAME_TABLE = "quotes_in_column_name";
    private static final String DUPLICATE_VALUES_IN_COLUMNS_TABLE = "duplicate_values_in_columns";
    // Use a recent value for updated_at to ensure Pinot doesn't clean up records older than retentionTimeValue as defined in the table specs
    private static final Instant initialUpdatedAt = Instant.now().minus(Duration.ofDays(1)).truncatedTo(SECONDS);
    // Use a fixed instant for testing date time functions
    private static final Instant CREATED_AT_INSTANT = Instant.parse("2021-05-10T00:00:00.00Z");

    private static final DateTimeFormatter MILLIS_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    protected abstract boolean isSecured();

    protected boolean isGrpcEnabled()
    {
        return true;
    }

    protected String getPinotImageName()
    {
        return PINOT_PREVIOUS_IMAGE_NAME;
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingKafka kafka = closeAfterClass(TestingKafka.createWithSchemaRegistry());
        kafka.start();
        TestingPinotCluster pinot = closeAfterClass(new TestingPinotCluster(kafka.getNetwork(), isSecured(), getPinotImageName()));
        pinot.start();

        createAndPopulateAllTypesTopic(kafka, pinot);
        createAndPopulateMixedCaseTableAndTopic(kafka, pinot);
        createAndPopulateMixedCaseDistinctTableAndTopic(kafka, pinot);
        createAndPopulateTooManyRowsTable(kafka, pinot);
        createAndPopulateTooManyBrokerRowsTableAndTopic(kafka, pinot);
        createTheDuplicateTablesAndTopics(kafka, pinot);
        createAndPopulateDateTimeFieldsTableAndTopic(kafka, pinot);
        createAndPopulateJsonTypeTable(kafka, pinot);
        createAndPopulateJsonTable(kafka, pinot);
        createAndPopulateMixedCaseHybridTablesAndTopic(kafka, pinot);
        createAndPopulateTableHavingReservedKeywordColumnNames(kafka, pinot);
        createAndPopulateHavingQuotesInColumnNames(kafka, pinot);
        createAndPopulateHavingMultipleColumnsWithDuplicateValues(kafka, pinot);

        DistributedQueryRunner queryRunner = createPinotQueryRunner(
                ImmutableMap.of(),
                pinotProperties(pinot),
                Optional.of(binder -> newOptionalBinder(binder, PinotHostMapper.class).setBinding()
                        .toInstance(new TestingPinotHostMapper(pinot.getBrokerHostAndPort(), pinot.getServerHostAndPort(), pinot.getServerGrpcHostAndPort()))));

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        // We need the query runner to populate nation and region data from tpch schema
        createAndPopulateNationAndRegionData(kafka, pinot, queryRunner);

        return queryRunner;
    }

    private void createAndPopulateAllTypesTopic(TestingKafka kafka, TestingPinotCluster pinot)
            throws Exception
    {
        // Create and populate the all_types topic and table
        kafka.createTopic(ALL_TYPES_TABLE);

        ImmutableList.Builder<ProducerRecord<String, GenericRecord>> allTypesRecordsBuilder = ImmutableList.builder();
        for (int i = 0, step = 1200; i < MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES - 2; i++) {
            int offset = i * step;
            allTypesRecordsBuilder.add(new ProducerRecord<>(ALL_TYPES_TABLE, "key" + i * step,
                    createTestRecord(
                            Arrays.asList("string_" + (offset), "string1_" + (offset + 1), "string2_" + (offset + 2)),
                            true,
                            Arrays.asList(54 + i / 3, -10001, 1000),
                            Arrays.asList(-7.33F + i, Float.POSITIVE_INFINITY, 17.034F + i),
                            Arrays.asList(-17.33D + i, Double.POSITIVE_INFINITY, 10596.034D + i),
                            Arrays.asList(-3147483647L + i, 12L - i, 4147483647L + i),
                            initialUpdatedAt.minusMillis(offset).toEpochMilli(),
                            initialUpdatedAt.plusMillis(offset).toEpochMilli())));
        }

        allTypesRecordsBuilder.add(new ProducerRecord<>(ALL_TYPES_TABLE, null, createNullRecord()));
        allTypesRecordsBuilder.add(new ProducerRecord<>(ALL_TYPES_TABLE, null, createArrayNullRecord()));
        kafka.sendMessages(allTypesRecordsBuilder.build().stream(), schemaRegistryAwareProducer(kafka));

        pinot.createSchema(getClass().getClassLoader().getResourceAsStream("alltypes_schema.json"), ALL_TYPES_TABLE);
        pinot.addRealTimeTable(getClass().getClassLoader().getResourceAsStream("alltypes_realtimeSpec.json"), ALL_TYPES_TABLE);
    }

    private void createAndPopulateMixedCaseTableAndTopic(TestingKafka kafka, TestingPinotCluster pinot)
            throws Exception
    {
        // Create and populate mixed case table and topic
        kafka.createTopic(MIXED_CASE_COLUMN_NAMES_TABLE);
        Schema mixedCaseAvroSchema = SchemaBuilder.record(MIXED_CASE_COLUMN_NAMES_TABLE).fields()
                .name("stringCol").type().stringType().noDefault()
                .name("longCol").type().optional().longType()
                .name("updatedAt").type().longType().noDefault()
                .endRecord();

        List<ProducerRecord<String, GenericRecord>> mixedCaseProducerRecords = ImmutableList.<ProducerRecord<String, GenericRecord>>builder()
                .add(new ProducerRecord<>(MIXED_CASE_COLUMN_NAMES_TABLE, "key0", new GenericRecordBuilder(mixedCaseAvroSchema)
                        .set("stringCol", "string_0")
                        .set("longCol", 0L)
                        .set("updatedAt", initialUpdatedAt.toEpochMilli())
                        .build()))
                .add(new ProducerRecord<>(MIXED_CASE_COLUMN_NAMES_TABLE, "key1", new GenericRecordBuilder(mixedCaseAvroSchema)
                        .set("stringCol", "string_1")
                        .set("longCol", 1L)
                        .set("updatedAt", initialUpdatedAt.plusMillis(1000).toEpochMilli())
                        .build()))
                .add(new ProducerRecord<>(MIXED_CASE_COLUMN_NAMES_TABLE, "key2", new GenericRecordBuilder(mixedCaseAvroSchema)
                        .set("stringCol", "string_2")
                        .set("longCol", 2L)
                        .set("updatedAt", initialUpdatedAt.plusMillis(2000).toEpochMilli())
                        .build()))
                .add(new ProducerRecord<>(MIXED_CASE_COLUMN_NAMES_TABLE, "key3", new GenericRecordBuilder(mixedCaseAvroSchema)
                        .set("stringCol", "string_3")
                        .set("longCol", 3L)
                        .set("updatedAt", initialUpdatedAt.plusMillis(3000).toEpochMilli())
                        .build()))
                .build();

        kafka.sendMessages(mixedCaseProducerRecords.stream(), schemaRegistryAwareProducer(kafka));
        pinot.createSchema(getClass().getClassLoader().getResourceAsStream("mixed_case_schema.json"), MIXED_CASE_COLUMN_NAMES_TABLE);
        pinot.addRealTimeTable(getClass().getClassLoader().getResourceAsStream("mixed_case_realtimeSpec.json"), MIXED_CASE_COLUMN_NAMES_TABLE);
    }

    private void createAndPopulateMixedCaseDistinctTableAndTopic(TestingKafka kafka, TestingPinotCluster pinot)
            throws Exception
    {
        // Create and populate mixed case distinct table and topic
        kafka.createTopic(MIXED_CASE_DISTINCT_TABLE);
        Schema mixedCaseDistinctAvroSchema = SchemaBuilder.record(MIXED_CASE_DISTINCT_TABLE).fields()
                .name("string_col").type().stringType().noDefault()
                .name("updated_at").type().longType().noDefault()
                .endRecord();

        List<ProducerRecord<String, GenericRecord>> mixedCaseDistinctProducerRecords = ImmutableList.<ProducerRecord<String, GenericRecord>>builder()
                .add(new ProducerRecord<>(MIXED_CASE_DISTINCT_TABLE, "key0", new GenericRecordBuilder(mixedCaseDistinctAvroSchema)
                        .set("string_col", "A")
                        .set("updated_at", initialUpdatedAt.toEpochMilli())
                        .build()))
                .add(new ProducerRecord<>(MIXED_CASE_DISTINCT_TABLE, "key1", new GenericRecordBuilder(mixedCaseDistinctAvroSchema)
                        .set("string_col", "a")
                        .set("updated_at", initialUpdatedAt.plusMillis(1000).toEpochMilli())
                        .build()))
                .add(new ProducerRecord<>(MIXED_CASE_DISTINCT_TABLE, "key2", new GenericRecordBuilder(mixedCaseDistinctAvroSchema)
                        .set("string_col", "B")
                        .set("updated_at", initialUpdatedAt.plusMillis(2000).toEpochMilli())
                        .build()))
                .add(new ProducerRecord<>(MIXED_CASE_DISTINCT_TABLE, "key3", new GenericRecordBuilder(mixedCaseDistinctAvroSchema)
                        .set("string_col", "b")
                        .set("updated_at", initialUpdatedAt.plusMillis(3000).toEpochMilli())
                        .build()))
                .build();

        kafka.sendMessages(mixedCaseDistinctProducerRecords.stream(), schemaRegistryAwareProducer(kafka));
        pinot.createSchema(getClass().getClassLoader().getResourceAsStream("mixed_case_distinct_schema.json"), MIXED_CASE_DISTINCT_TABLE);
        pinot.addRealTimeTable(getClass().getClassLoader().getResourceAsStream("mixed_case_distinct_realtimeSpec.json"), MIXED_CASE_DISTINCT_TABLE);

        // Create mixed case table name, populated from the mixed case topic
        pinot.createSchema(getClass().getClassLoader().getResourceAsStream("mixed_case_table_name_schema.json"), MIXED_CASE_TABLE_NAME);
        pinot.addRealTimeTable(getClass().getClassLoader().getResourceAsStream("mixed_case_table_name_realtimeSpec.json"), MIXED_CASE_TABLE_NAME);
    }

    private void createAndPopulateTooManyRowsTable(TestingKafka kafka, TestingPinotCluster pinot)
            throws Exception
    {
        // Create and populate too many rows table and topic
        kafka.createTopic(TOO_MANY_ROWS_TABLE);
        Schema tooManyRowsAvroSchema = SchemaBuilder.record(TOO_MANY_ROWS_TABLE).fields()
                .name("string_col").type().optional().stringType()
                .name("updatedAt").type().optional().longType()
                .endRecord();

        ImmutableList.Builder<ProducerRecord<String, GenericRecord>> tooManyRowsRecordsBuilder = ImmutableList.builder();
        for (int i = 0; i < MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES + 1; i++) {
            tooManyRowsRecordsBuilder.add(new ProducerRecord<>(TOO_MANY_ROWS_TABLE, "key" + i, new GenericRecordBuilder(tooManyRowsAvroSchema)
                    .set("string_col", "string_" + i)
                    .set("updatedAt", initialUpdatedAt.plusMillis(i * 1000L).toEpochMilli())
                    .build()));
        }
        kafka.sendMessages(tooManyRowsRecordsBuilder.build().stream(), schemaRegistryAwareProducer(kafka));
        pinot.createSchema(getClass().getClassLoader().getResourceAsStream("too_many_rows_schema.json"), TOO_MANY_ROWS_TABLE);
        pinot.addRealTimeTable(getClass().getClassLoader().getResourceAsStream("too_many_rows_realtimeSpec.json"), TOO_MANY_ROWS_TABLE);
    }

    private void createAndPopulateTooManyBrokerRowsTableAndTopic(TestingKafka kafka, TestingPinotCluster pinot)
            throws Exception
    {
        // Create and populate too many broker rows table and topic
        kafka.createTopic(TOO_MANY_BROKER_ROWS_TABLE);
        Schema tooManyBrokerRowsAvroSchema = SchemaBuilder.record(TOO_MANY_BROKER_ROWS_TABLE).fields()
                .name("string_col").type().optional().stringType()
                .name("updatedAt").type().optional().longType()
                .endRecord();

        ImmutableList.Builder<ProducerRecord<String, GenericRecord>> tooManyBrokerRowsRecordsBuilder = ImmutableList.builder();
        for (int i = 0; i < MAX_ROWS_PER_SPLIT_FOR_BROKER_QUERIES + 1; i++) {
            tooManyBrokerRowsRecordsBuilder.add(new ProducerRecord<>(TOO_MANY_BROKER_ROWS_TABLE, "key" + i, new GenericRecordBuilder(tooManyBrokerRowsAvroSchema)
                    .set("string_col", "string_" + i)
                    .set("updatedAt", initialUpdatedAt.plusMillis(i * 1000L).toEpochMilli())
                    .build()));
        }
        kafka.sendMessages(tooManyBrokerRowsRecordsBuilder.build().stream(), schemaRegistryAwareProducer(kafka));
        pinot.createSchema(getClass().getClassLoader().getResourceAsStream("too_many_broker_rows_schema.json"), TOO_MANY_BROKER_ROWS_TABLE);
        pinot.addRealTimeTable(getClass().getClassLoader().getResourceAsStream("too_many_broker_rows_realtimeSpec.json"), TOO_MANY_BROKER_ROWS_TABLE);
    }

    private void createTheDuplicateTablesAndTopics(TestingKafka kafka, TestingPinotCluster pinot)
            throws Exception
    {
        // Create the duplicate tables and topics
        kafka.createTopic(DUPLICATE_TABLE_LOWERCASE);
        pinot.createSchema(getClass().getClassLoader().getResourceAsStream("dup_table_lower_case_schema.json"), DUPLICATE_TABLE_LOWERCASE);
        pinot.addRealTimeTable(getClass().getClassLoader().getResourceAsStream("dup_table_lower_case_realtimeSpec.json"), DUPLICATE_TABLE_LOWERCASE);

        kafka.createTopic(DUPLICATE_TABLE_MIXED_CASE);
        pinot.createSchema(getClass().getClassLoader().getResourceAsStream("dup_table_mixed_case_schema.json"), DUPLICATE_TABLE_MIXED_CASE);
        pinot.addRealTimeTable(getClass().getClassLoader().getResourceAsStream("dup_table_mixed_case_realtimeSpec.json"), DUPLICATE_TABLE_MIXED_CASE);
    }

    private void createAndPopulateDateTimeFieldsTableAndTopic(TestingKafka kafka, TestingPinotCluster pinot)
            throws Exception
    {
        // Create and populate date time fields table and topic
        kafka.createTopic(DATE_TIME_FIELDS_TABLE);
        Schema dateTimeFieldsAvroSchema = SchemaBuilder.record(DATE_TIME_FIELDS_TABLE).fields()
                .name("string_col").type().stringType().noDefault()
                .name("created_at").type().longType().noDefault()
                .name("updated_at").type().longType().noDefault()
                .endRecord();
        List<ProducerRecord<String, GenericRecord>> dateTimeFieldsProducerRecords = ImmutableList.<ProducerRecord<String, GenericRecord>>builder()
                .add(new ProducerRecord<>(DATE_TIME_FIELDS_TABLE, "string_0", new GenericRecordBuilder(dateTimeFieldsAvroSchema)
                        .set("string_col", "string_0")
                        .set("created_at", CREATED_AT_INSTANT.toEpochMilli())
                        .set("updated_at", initialUpdatedAt.toEpochMilli())
                        .build()))
                .add(new ProducerRecord<>(DATE_TIME_FIELDS_TABLE, "string_1", new GenericRecordBuilder(dateTimeFieldsAvroSchema)
                        .set("string_col", "string_1")
                        .set("created_at", CREATED_AT_INSTANT.plusMillis(1000).toEpochMilli())
                        .set("updated_at", initialUpdatedAt.plusMillis(1000).toEpochMilli())
                        .build()))
                .add(new ProducerRecord<>(DATE_TIME_FIELDS_TABLE, "string_2", new GenericRecordBuilder(dateTimeFieldsAvroSchema)
                        .set("string_col", "string_2")
                        .set("created_at", CREATED_AT_INSTANT.plusMillis(2000).toEpochMilli())
                        .set("updated_at", initialUpdatedAt.plusMillis(2000).toEpochMilli())
                        .build()))
                .build();
        kafka.sendMessages(dateTimeFieldsProducerRecords.stream(), schemaRegistryAwareProducer(kafka));
        pinot.createSchema(getClass().getClassLoader().getResourceAsStream("date_time_fields_schema.json"), DATE_TIME_FIELDS_TABLE);
        pinot.addRealTimeTable(getClass().getClassLoader().getResourceAsStream("date_time_fields_realtimeSpec.json"), DATE_TIME_FIELDS_TABLE);
    }

    private void createAndPopulateJsonTypeTable(TestingKafka kafka, TestingPinotCluster pinot)
            throws Exception
    {
        // Create json type table
        kafka.createTopic(JSON_TYPE_TABLE);

        Schema jsonTableAvroSchema = SchemaBuilder.record(JSON_TYPE_TABLE).fields()
                .name("string_col").type().optional().stringType()
                .name("json_col").type().optional().stringType()
                .name("updatedAt").type().optional().longType()
                .endRecord();

        ImmutableList.Builder<ProducerRecord<String, GenericRecord>> jsonTableRecordsBuilder = ImmutableList.builder();
        for (int i = 0; i < 3; i++) {
            jsonTableRecordsBuilder.add(new ProducerRecord<>(JSON_TYPE_TABLE, "key" + i, new GenericRecordBuilder(jsonTableAvroSchema)
                    .set("string_col", "string_" + i)
                    .set("json_col", "{ \"name\": \"user_" + i + "\", \"id\": " + i + "}")
                    .set("updatedAt", initialUpdatedAt.plusMillis(i * 1000L).toEpochMilli())
                    .build()));
        }
        kafka.sendMessages(jsonTableRecordsBuilder.build().stream(), schemaRegistryAwareProducer(kafka));
        pinot.createSchema(getClass().getClassLoader().getResourceAsStream("json_schema.json"), JSON_TYPE_TABLE);
        pinot.addRealTimeTable(getClass().getClassLoader().getResourceAsStream("json_realtimeSpec.json"), JSON_TYPE_TABLE);
        pinot.addOfflineTable(getClass().getClassLoader().getResourceAsStream("json_offlineSpec.json"), JSON_TYPE_TABLE);
    }

    private void createAndPopulateJsonTable(TestingKafka kafka, TestingPinotCluster pinot)
            throws Exception
    {
        // Create json table
        kafka.createTopic(JSON_TABLE);
        long key = 0L;
        kafka.sendMessages(Stream.of(
                new ProducerRecord<>(JSON_TABLE, key++, TestingJsonRecord.of("vendor1", "Los Angeles", Arrays.asList("foo1", "bar1", "baz1"), Arrays.asList(5, 6, 7), Arrays.asList(3.5F, 5.5F), Arrays.asList(10_000.5D, 20_000.335D, -3.7D), Arrays.asList(10_000L, 20_000_000L, -37L), 4)),
                new ProducerRecord<>(JSON_TABLE, key++, TestingJsonRecord.of("vendor2", "New York", Arrays.asList("foo2", "bar1", "baz1"), Arrays.asList(6, 7, 8), Arrays.asList(4.5F, 6.5F), Arrays.asList(10_000.5D, 20_000.335D, -3.7D), Arrays.asList(10_000L, 20_000_000L, -37L), 6)),
                new ProducerRecord<>(JSON_TABLE, key++, TestingJsonRecord.of("vendor3", "Los Angeles", Arrays.asList("foo3", "bar2", "baz1"), Arrays.asList(7, 8, 9), Arrays.asList(5.5F, 7.5F), Arrays.asList(10_000.5D, 20_000.335D, -3.7D), Arrays.asList(10_000L, 20_000_000L, -37L), 8)),
                new ProducerRecord<>(JSON_TABLE, key++, TestingJsonRecord.of("vendor4", "New York", Arrays.asList("foo4", "bar2", "baz2"), Arrays.asList(8, 9, 10), Arrays.asList(6.5F, 8.5F), Arrays.asList(10_000.5D, 20_000.335D, -3.7D), Arrays.asList(10_000L, 20_000_000L, -37L), 10)),
                new ProducerRecord<>(JSON_TABLE, key++, TestingJsonRecord.of("vendor5", "Los Angeles", Arrays.asList("foo5", "bar3", "baz2"), Arrays.asList(9, 10, 11), Arrays.asList(7.5F, 9.5F), Arrays.asList(10_000.5D, 20_000.335D, -3.7D), Arrays.asList(10_000L, 20_000_000L, -37L), 12)),
                new ProducerRecord<>(JSON_TABLE, key++, TestingJsonRecord.of("vendor6", "Los Angeles", Arrays.asList("foo6", "bar3", "baz2"), Arrays.asList(10, 11, 12), Arrays.asList(8.5F, 10.5F), Arrays.asList(10_000.5D, 20_000.335D, -3.7D), Arrays.asList(10_000L, 20_000_000L, -37L), 12)),
                new ProducerRecord<>(JSON_TABLE, key, TestingJsonRecord.of("vendor7", "Los Angeles", Arrays.asList("foo6", "bar3", "baz2"), Arrays.asList(10, 11, 12), Arrays.asList(9.5F, 10.5F), Arrays.asList(10_000.5D, 20_000.335D, -3.7D), Arrays.asList(10_000L, 20_000_000L, -37L), 12))));

        pinot.createSchema(getClass().getClassLoader().getResourceAsStream("schema.json"), JSON_TABLE);
        pinot.addRealTimeTable(getClass().getClassLoader().getResourceAsStream("realtimeSpec.json"), JSON_TABLE);
    }

    private void createAndPopulateMixedCaseHybridTablesAndTopic(TestingKafka kafka, TestingPinotCluster pinot)
            throws Exception
    {
        // Create and populate mixed case table and topic
        kafka.createTopic(HYBRID_TABLE_NAME);
        Schema hybridAvroSchema = SchemaBuilder.record(HYBRID_TABLE_NAME).fields()
                .name("stringCol").type().stringType().noDefault()
                .name("longCol").type().optional().longType()
                .name("updatedAt").type().longType().noDefault()
                .endRecord();

        pinot.createSchema(getClass().getClassLoader().getResourceAsStream("hybrid_schema.json"), HYBRID_TABLE_NAME);
        pinot.addRealTimeTable(getClass().getClassLoader().getResourceAsStream("hybrid_realtimeSpec.json"), HYBRID_TABLE_NAME);
        pinot.addOfflineTable(getClass().getClassLoader().getResourceAsStream("hybrid_offlineSpec.json"), HYBRID_TABLE_NAME);

        Instant startInstant = initialUpdatedAt.truncatedTo(DAYS);
        List<ProducerRecord<String, GenericRecord>> hybridProducerRecords = ImmutableList.<ProducerRecord<String, GenericRecord>>builder()
                .add(new ProducerRecord<>(HYBRID_TABLE_NAME, "key0", new GenericRecordBuilder(hybridAvroSchema)
                        .set("stringCol", "string_0")
                        .set("longCol", 0L)
                        .set("updatedAt", startInstant.toEpochMilli())
                        .build()))
                .add(new ProducerRecord<>(HYBRID_TABLE_NAME, "key1", new GenericRecordBuilder(hybridAvroSchema)
                        .set("stringCol", "string_1")
                        .set("longCol", 1L)
                        .set("updatedAt", startInstant.plusMillis(1000).toEpochMilli())
                        .build()))
                .add(new ProducerRecord<>(HYBRID_TABLE_NAME, "key2", new GenericRecordBuilder(hybridAvroSchema)
                        .set("stringCol", "string_2")
                        .set("longCol", 2L)
                        .set("updatedAt", startInstant.plusMillis(2000).toEpochMilli())
                        .build()))
                .add(new ProducerRecord<>(HYBRID_TABLE_NAME, "key3", new GenericRecordBuilder(hybridAvroSchema)
                        .set("stringCol", "string_3")
                        .set("longCol", 3L)
                        .set("updatedAt", startInstant.plusMillis(3000).toEpochMilli())
                        .build()))
                .build();

        Path temporaryDirectory = Paths.get("/tmp/segments-" + randomUUID());
        try {
            Files.createDirectory(temporaryDirectory);
            ImmutableList.Builder<GenericRow> offlineRowsBuilder = ImmutableList.builder();
            for (int i = 4; i < 8; i++) {
                GenericRow row = new GenericRow();
                row.putValue("stringCol", "string_" + i);
                row.putValue("longCol", (long) i);
                row.putValue("updatedAt", startInstant.plus(1, DAYS).plusMillis(1000L * (i - 4)).toEpochMilli());
                offlineRowsBuilder.add(row);
            }
            Path segmentPath = createSegment(getClass().getClassLoader().getResourceAsStream("hybrid_offlineSpec.json"), getClass().getClassLoader().getResourceAsStream("hybrid_schema.json"), new GenericRowRecordReader(offlineRowsBuilder.build()), temporaryDirectory.toString(), 0);
            pinot.publishOfflineSegment("hybrid", segmentPath);

            offlineRowsBuilder = ImmutableList.builder();
            // These rows will be visible as they are older than the Pinot time boundary
            // In Pinot the time boundary is the most recent time column value for an offline row - 24 hours
            for (int i = 8; i < 12; i++) {
                GenericRow row = new GenericRow();
                row.putValue("stringCol", "string_" + i);
                row.putValue("longCol", (long) i);
                row.putValue("updatedAt", startInstant.minus(1, DAYS).plusMillis(1000L * (i - 7)).toEpochMilli());
                offlineRowsBuilder.add(row);
            }
            segmentPath = createSegment(getClass().getClassLoader().getResourceAsStream("hybrid_offlineSpec.json"), getClass().getClassLoader().getResourceAsStream("hybrid_schema.json"), new GenericRowRecordReader(offlineRowsBuilder.build()), temporaryDirectory.toString(), 1);
            pinot.publishOfflineSegment("hybrid", segmentPath);
        }
        finally {
            deleteRecursively(temporaryDirectory, ALLOW_INSECURE);
        }

        kafka.sendMessages(hybridProducerRecords.stream(), schemaRegistryAwareProducer(kafka));
    }

    private void createAndPopulateTableHavingReservedKeywordColumnNames(TestingKafka kafka, TestingPinotCluster pinot)
            throws Exception
    {
        // Create a table having reserved keyword column names
        kafka.createTopic(RESERVED_KEYWORD_TABLE);
        Schema reservedKeywordAvroSchema = SchemaBuilder.record(RESERVED_KEYWORD_TABLE).fields()
                .name("date").type().optional().stringType()
                .name("as").type().optional().stringType()
                .name("updatedAt").type().optional().longType()
                .endRecord();
        ImmutableList.Builder<ProducerRecord<String, GenericRecord>> reservedKeywordRecordsBuilder = ImmutableList.builder();
        reservedKeywordRecordsBuilder.add(new ProducerRecord<>(RESERVED_KEYWORD_TABLE, "key0", new GenericRecordBuilder(reservedKeywordAvroSchema).set("date", "2021-09-30").set("as", "foo").set("updatedAt", initialUpdatedAt.plusMillis(1000).toEpochMilli()).build()));
        reservedKeywordRecordsBuilder.add(new ProducerRecord<>(RESERVED_KEYWORD_TABLE, "key1", new GenericRecordBuilder(reservedKeywordAvroSchema).set("date", "2021-10-01").set("as", "bar").set("updatedAt", initialUpdatedAt.plusMillis(2000).toEpochMilli()).build()));
        kafka.sendMessages(reservedKeywordRecordsBuilder.build().stream(), schemaRegistryAwareProducer(kafka));
        pinot.createSchema(getClass().getClassLoader().getResourceAsStream("reserved_keyword_schema.json"), RESERVED_KEYWORD_TABLE);
        pinot.addRealTimeTable(getClass().getClassLoader().getResourceAsStream("reserved_keyword_realtimeSpec.json"), RESERVED_KEYWORD_TABLE);
    }

    private void createAndPopulateHavingQuotesInColumnNames(TestingKafka kafka, TestingPinotCluster pinot)
            throws Exception
    {
        // Create a table having quotes in column names
        kafka.createTopic(QUOTES_IN_COLUMN_NAME_TABLE);
        Schema quotesInColumnNameAvroSchema = SchemaBuilder.record(QUOTES_IN_COLUMN_NAME_TABLE).fields()
                .name("non_quoted").type().optional().stringType()
                .name("updatedAt").type().optional().longType()
                .endRecord();
        ImmutableList.Builder<ProducerRecord<String, GenericRecord>> quotesInColumnNameRecordsBuilder = ImmutableList.builder();
        quotesInColumnNameRecordsBuilder.add(new ProducerRecord<>(QUOTES_IN_COLUMN_NAME_TABLE, "key0", new GenericRecordBuilder(quotesInColumnNameAvroSchema).set("non_quoted", "Foo").set("updatedAt", initialUpdatedAt.plusMillis(1000).toEpochMilli()).build()));
        quotesInColumnNameRecordsBuilder.add(new ProducerRecord<>(QUOTES_IN_COLUMN_NAME_TABLE, "key1", new GenericRecordBuilder(quotesInColumnNameAvroSchema).set("non_quoted", "Bar").set("updatedAt", initialUpdatedAt.plusMillis(2000).toEpochMilli()).build()));
        kafka.sendMessages(quotesInColumnNameRecordsBuilder.build().stream(), schemaRegistryAwareProducer(kafka));
        pinot.createSchema(getClass().getClassLoader().getResourceAsStream("quotes_in_column_name_schema.json"), QUOTES_IN_COLUMN_NAME_TABLE);
        pinot.addRealTimeTable(getClass().getClassLoader().getResourceAsStream("quotes_in_column_name_realtimeSpec.json"), QUOTES_IN_COLUMN_NAME_TABLE);
    }

    private void createAndPopulateHavingMultipleColumnsWithDuplicateValues(TestingKafka kafka, TestingPinotCluster pinot)
            throws Exception
    {
        // Create a table having multiple columns with duplicate values
        kafka.createTopic(DUPLICATE_VALUES_IN_COLUMNS_TABLE);
        Schema duplicateValuesInColumnsAvroSchema = SchemaBuilder.record(DUPLICATE_VALUES_IN_COLUMNS_TABLE).fields()
                .name("dim_col").type().optional().longType()
                .name("another_dim_col").type().optional().longType()
                .name("string_col").type().optional().stringType()
                .name("another_string_col").type().optional().stringType()
                .name("metric_col1").type().optional().longType()
                .name("metric_col2").type().optional().longType()
                .name("updated_at").type().longType().noDefault()
                .endRecord();

        ImmutableList.Builder<ProducerRecord<String, GenericRecord>> duplicateValuesInColumnsRecordsBuilder = ImmutableList.builder();
        duplicateValuesInColumnsRecordsBuilder.add(new ProducerRecord<>(DUPLICATE_VALUES_IN_COLUMNS_TABLE, "key0", new GenericRecordBuilder(duplicateValuesInColumnsAvroSchema)
                .set("dim_col", 1000L)
                .set("another_dim_col", 1000L)
                .set("string_col", "string1")
                .set("another_string_col", "string1")
                .set("metric_col1", 10L)
                .set("metric_col2", 20L)
                .set("updated_at", initialUpdatedAt.plusMillis(1000).toEpochMilli())
                .build()));
        duplicateValuesInColumnsRecordsBuilder.add(new ProducerRecord<>(DUPLICATE_VALUES_IN_COLUMNS_TABLE, "key1", new GenericRecordBuilder(duplicateValuesInColumnsAvroSchema)
                .set("dim_col", 2000L)
                .set("another_dim_col", 2000L)
                .set("string_col", "string1")
                .set("another_string_col", "string1")
                .set("metric_col1", 100L)
                .set("metric_col2", 200L)
                .set("updated_at", initialUpdatedAt.plusMillis(2000).toEpochMilli())
                .build()));
        duplicateValuesInColumnsRecordsBuilder.add(new ProducerRecord<>(DUPLICATE_VALUES_IN_COLUMNS_TABLE, "key2", new GenericRecordBuilder(duplicateValuesInColumnsAvroSchema)
                .set("dim_col", 3000L)
                .set("another_dim_col", 3000L)
                .set("string_col", "string1")
                .set("another_string_col", "another_string1")
                .set("metric_col1", 1000L)
                .set("metric_col2", 2000L)
                .set("updated_at", initialUpdatedAt.plusMillis(3000).toEpochMilli())
                .build()));
        duplicateValuesInColumnsRecordsBuilder.add(new ProducerRecord<>(DUPLICATE_VALUES_IN_COLUMNS_TABLE, "key1", new GenericRecordBuilder(duplicateValuesInColumnsAvroSchema)
                .set("dim_col", 4000L)
                .set("another_dim_col", 4000L)
                .set("string_col", "string2")
                .set("another_string_col", "another_string2")
                .set("metric_col1", 100L)
                .set("metric_col2", 200L)
                .set("updated_at", initialUpdatedAt.plusMillis(4000).toEpochMilli())
                .build()));
        duplicateValuesInColumnsRecordsBuilder.add(new ProducerRecord<>(DUPLICATE_VALUES_IN_COLUMNS_TABLE, "key2", new GenericRecordBuilder(duplicateValuesInColumnsAvroSchema)
                .set("dim_col", 4000L)
                .set("another_dim_col", 4001L)
                .set("string_col", "string2")
                .set("another_string_col", "string2")
                .set("metric_col1", 1000L)
                .set("metric_col2", 2000L)
                .set("updated_at", initialUpdatedAt.plusMillis(5000).toEpochMilli())
                .build()));

        kafka.sendMessages(duplicateValuesInColumnsRecordsBuilder.build().stream(), schemaRegistryAwareProducer(kafka));
        pinot.createSchema(getClass().getClassLoader().getResourceAsStream("duplicate_values_in_columns_schema.json"), DUPLICATE_VALUES_IN_COLUMNS_TABLE);
        pinot.addRealTimeTable(getClass().getClassLoader().getResourceAsStream("duplicate_values_in_columns_realtimeSpec.json"), DUPLICATE_VALUES_IN_COLUMNS_TABLE);
    }

    private void createAndPopulateNationAndRegionData(TestingKafka kafka, TestingPinotCluster pinot, DistributedQueryRunner queryRunner)
            throws Exception
    {
        // Create and populate table and topic data
        String regionTableName = "region";
        kafka.createTopicWithConfig(2, 1, regionTableName, false);
        Schema regionSchema = SchemaBuilder.record(regionTableName).fields()
                // regionkey bigint, name varchar, comment varchar
                .name("regionkey").type().longType().noDefault()
                .name("name").type().stringType().noDefault()
                .name("comment").type().stringType().noDefault()
                .name("updated_at_seconds").type().longType().noDefault()
                .endRecord();
        ImmutableList.Builder<ProducerRecord<String, GenericRecord>> regionRowsBuilder = ImmutableList.builder();
        MaterializedResult regionRows = queryRunner.execute("SELECT * FROM tpch.tiny.region");
        for (MaterializedRow row : regionRows.getMaterializedRows()) {
            regionRowsBuilder.add(new ProducerRecord<>(regionTableName, "key" + row.getField(0), new GenericRecordBuilder(regionSchema)
                    .set("regionkey", row.getField(0))
                    .set("name", row.getField(1))
                    .set("comment", row.getField(2))
                    .set("updated_at_seconds", initialUpdatedAt.plusMillis(1000).toEpochMilli())
                    .build()));
        }
        kafka.sendMessages(regionRowsBuilder.build().stream(), schemaRegistryAwareProducer(kafka));
        pinot.createSchema(getClass().getClassLoader().getResourceAsStream("region_schema.json"), regionTableName);
        pinot.addRealTimeTable(getClass().getClassLoader().getResourceAsStream("region_realtimeSpec.json"), regionTableName);

        String nationTableName = "nation";
        kafka.createTopicWithConfig(2, 1, nationTableName, false);
        Schema nationSchema = SchemaBuilder.record(nationTableName).fields()
                // nationkey BIGINT, name VARCHAR,  VARCHAR, regionkey BIGINT
                .name("nationkey").type().longType().noDefault()
                .name("name").type().stringType().noDefault()
                .name("comment").type().stringType().noDefault()
                .name("regionkey").type().longType().noDefault()
                .name("updated_at_seconds").type().longType().noDefault()
                .endRecord();
        ImmutableList.Builder<ProducerRecord<String, GenericRecord>> nationRowsBuilder = ImmutableList.builder();
        MaterializedResult nationRows = queryRunner.execute("SELECT * FROM tpch.tiny.nation");
        for (MaterializedRow row : nationRows.getMaterializedRows()) {
            nationRowsBuilder.add(new ProducerRecord<>(nationTableName, "key" + row.getField(0), new GenericRecordBuilder(nationSchema)
                    .set("nationkey", row.getField(0))
                    .set("name", row.getField(1))
                    .set("comment", row.getField(3))
                    .set("regionkey", row.getField(2))
                    .set("updated_at_seconds", initialUpdatedAt.plusMillis(1000).toEpochMilli())
                    .build()));
        }
        kafka.sendMessages(nationRowsBuilder.build().stream(), schemaRegistryAwareProducer(kafka));
        pinot.createSchema(getClass().getClassLoader().getResourceAsStream("nation_schema.json"), nationTableName);
        pinot.addRealTimeTable(getClass().getClassLoader().getResourceAsStream("nation_realtimeSpec.json"), nationTableName);
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_MATERIALIZED_VIEW,
                    SUPPORTS_CREATE_SCHEMA,
                    SUPPORTS_CREATE_TABLE,
                    SUPPORTS_CREATE_VIEW,
                    SUPPORTS_DELETE,
                    SUPPORTS_INSERT,
                    SUPPORTS_MERGE,
                    SUPPORTS_RENAME_TABLE,
                    SUPPORTS_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    private Map<String, String> pinotProperties(TestingPinotCluster pinot)
    {
        return ImmutableMap.<String, String>builder()
                .put("pinot.controller-urls", pinot.getControllerConnectString())
                .put("pinot.max-rows-per-split-for-segment-queries", String.valueOf(MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES))
                .put("pinot.max-rows-for-broker-queries", String.valueOf(MAX_ROWS_PER_SPLIT_FOR_BROKER_QUERIES))
                .putAll(additionalPinotProperties())
                .buildOrThrow();
    }

    protected Map<String, String> additionalPinotProperties()
    {
        if (isGrpcEnabled()) {
            return ImmutableMap.of("pinot.grpc.enabled", "true");
        }
        return ImmutableMap.of();
    }

    private static Path createSegment(InputStream tableConfigInputStream, InputStream pinotSchemaInputStream, RecordReader recordReader, String outputDirectory, int sequenceId)
    {
        try {
            org.apache.pinot.spi.data.Schema pinotSchema = org.apache.pinot.spi.data.Schema.fromInputStream(pinotSchemaInputStream);
            TableConfig tableConfig = inputStreamToObject(tableConfigInputStream, TableConfig.class);
            String tableName = TableNameBuilder.extractRawTableName(tableConfig.getTableName());
            String timeColumnName = tableConfig.getValidationConfig().getTimeColumnName();
            String segmentTempLocation = String.join(File.separator, outputDirectory, tableName, "segments");
            Files.createDirectories(Paths.get(outputDirectory));
            SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, pinotSchema);
            segmentGeneratorConfig.setTableName(tableName);
            segmentGeneratorConfig.setOutDir(segmentTempLocation);
            if (timeColumnName != null) {
                DateTimeFormatSpec formatSpec = new DateTimeFormatSpec(pinotSchema.getDateTimeSpec(timeColumnName).getFormat());
                segmentGeneratorConfig.setSegmentNameGenerator(new NormalizedDateSegmentNameGenerator(
                        tableName,
                        null,
                        false,
                        tableConfig.getValidationConfig().getSegmentPushType(),
                        tableConfig.getValidationConfig().getSegmentPushFrequency(),
                        formatSpec,
                        null));
            }
            else {
                checkState(tableConfig.isDimTable(), "Null time column only allowed for dimension tables");
            }
            segmentGeneratorConfig.setSequenceId(sequenceId);
            SegmentCreationDataSource dataSource = new RecordReaderSegmentCreationDataSource(recordReader);
            RecordTransformer recordTransformer = genericRow -> {
                GenericRow record = null;
                try {
                    record = CompositeTransformer.getDefaultTransformer(tableConfig, pinotSchema).transform(genericRow);
                }
                catch (Exception e) {
                    // ignored
                    record = null;
                }
                return record;
            };
            SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
            driver.init(segmentGeneratorConfig, dataSource, recordTransformer, null);
            driver.build();
            File segmentOutputDirectory = driver.getOutputDirectory();
            File tgzPath = new File(String.join(File.separator, outputDirectory, segmentOutputDirectory.getName() + ".tar.gz"));
            TarGzCompressionUtils.createTarGzFile(segmentOutputDirectory, tgzPath);
            return Paths.get(tgzPath.getAbsolutePath());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<String, String> schemaRegistryAwareProducer(TestingKafka testingKafka)
    {
        return ImmutableMap.<String, String>builder()
                .put(SCHEMA_REGISTRY_URL_CONFIG, testingKafka.getSchemaRegistryConnectString())
                .put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
                .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                .buildOrThrow();
    }

    private static GenericRecord createTestRecord(
            List<String> stringArrayColumn,
            Boolean booleanColumn,
            List<Integer> intArrayColumn,
            List<Float> floatArrayColumn,
            List<Double> doubleArrayColumn,
            List<Long> longArrayColumn,
            long timestampColumn,
            long updatedAtMillis)
    {
        Schema schema = getAllTypesAvroSchema();

        return new GenericRecordBuilder(schema)
                .set("string_col", stringArrayColumn.get(0))
                .set("bool_col", booleanColumn)
                .set("bytes_col", HexFormat.of().formatHex(stringArrayColumn.get(0).getBytes(StandardCharsets.UTF_8)))
                .set("string_array_col", stringArrayColumn)
                .set("int_array_col", intArrayColumn)
                .set("int_array_col_with_pinot_default", intArrayColumn)
                .set("float_array_col", floatArrayColumn)
                .set("double_array_col", doubleArrayColumn)
                .set("long_array_col", longArrayColumn)
                .set("timestamp_col", timestampColumn)
                .set("int_col", intArrayColumn.get(0))
                .set("float_col", floatArrayColumn.get(0))
                .set("double_col", doubleArrayColumn.get(0))
                .set("long_col", longArrayColumn.get(0))
                .set("updated_at", updatedAtMillis)
                .set("ts", updatedAtMillis)
                .build();
    }

    private static GenericRecord createNullRecord()
    {
        Schema schema = getAllTypesAvroSchema();
        // Pinot does not transform the time column value to default null value
        return new GenericRecordBuilder(schema)
                .set("updated_at", initialUpdatedAt.toEpochMilli())
                .build();
    }

    private static GenericRecord createArrayNullRecord()
    {
        Schema schema = getAllTypesAvroSchema();
        List<String> stringList = Arrays.asList("string_0", null, "string_2", null, "string_4");
        List<Integer> integerList = new ArrayList<>();
        integerList.addAll(Arrays.asList(null, null, null, null, null));
        List<Integer> integerWithDefaultList = Arrays.asList(-1112, null, 753, null, -9238);
        List<Float> floatList = new ArrayList<>();
        floatList.add(null);
        List<Integer> doubleList = new ArrayList<>();
        doubleList.add(null);

        return new GenericRecordBuilder(schema)
                .set("string_col", "array_null")
                .set("string_array_col", stringList)
                .set("int_array_col", integerList)
                .set("int_array_col_with_pinot_default", integerWithDefaultList)
                .set("float_array_col", floatList)
                .set("double_array_col", doubleList)
                .set("long_array_col", new ArrayList<>())
                .set("updated_at", initialUpdatedAt.toEpochMilli())
                .build();
    }

    private static Schema getAllTypesAvroSchema()
    {
        // Note:
        // The reason optional() is used is because the avro record can omit those fields.
        // Fields with nullable type are required to be included or have a default value.
        //
        // For example:
        // If "string_col" is set to type().nullable().stringType().noDefault()
        // the following error is returned: Field string_col type:UNION pos:0 not set and has no default value

        return SchemaBuilder.record("alltypes")
                .fields()
                .name("string_col").type().optional().stringType()
                .name("bool_col").type().optional().booleanType()
                .name("bytes_col").type().optional().stringType()
                .name("string_array_col").type().optional().array().items().nullable().stringType()
                .name("int_array_col").type().optional().array().items().nullable().intType()
                .name("int_array_col_with_pinot_default").type().optional().array().items().nullable().intType()
                .name("float_array_col").type().optional().array().items().nullable().floatType()
                .name("double_array_col").type().optional().array().items().nullable().doubleType()
                .name("long_array_col").type().optional().array().items().nullable().longType()
                .name("timestamp_col").type().optional().longType()
                .name("int_col").type().optional().intType()
                .name("float_col").type().optional().floatType()
                .name("double_col").type().optional().doubleType()
                .name("long_col").type().optional().longType()
                .name("updated_at").type().optional().longType()
                .name("ts").type().optional().longType()
                .endRecord();
    }

    private static class TestingJsonRecord
    {
        private final String vendor;
        private final String city;
        private final List<String> neighbors;
        private final List<Integer> luckyNumbers;
        private final List<Float> prices;
        private final List<Double> unluckyNumbers;
        private final List<Long> longNumbers;
        private final Integer luckyNumber;
        private final Float price;
        private final Double unluckyNumber;
        private final Long longNumber;
        private final long updatedAt;

        @JsonCreator
        public TestingJsonRecord(
                @JsonProperty("vendor") String vendor,
                @JsonProperty("city") String city,
                @JsonProperty("neighbors") List<String> neighbors,
                @JsonProperty("lucky_numbers") List<Integer> luckyNumbers,
                @JsonProperty("prices") List<Float> prices,
                @JsonProperty("unlucky_numbers") List<Double> unluckyNumbers,
                @JsonProperty("long_numbers") List<Long> longNumbers,
                @JsonProperty("lucky_number") Integer luckyNumber,
                @JsonProperty("price") Float price,
                @JsonProperty("unlucky_number") Double unluckyNumber,
                @JsonProperty("long_number") Long longNumber,
                @JsonProperty("updatedAt") long updatedAt)
        {
            this.vendor = requireNonNull(vendor, "vendor is null");
            this.city = requireNonNull(city, "city is null");
            this.neighbors = requireNonNull(neighbors, "neighbors is null");
            this.luckyNumbers = requireNonNull(luckyNumbers, "luckyNumbers is null");
            this.prices = requireNonNull(prices, "prices is null");
            this.unluckyNumbers = requireNonNull(unluckyNumbers, "unluckyNumbers is null");
            this.longNumbers = requireNonNull(longNumbers, "longNumbers is null");
            this.price = requireNonNull(price, "price is null");
            this.luckyNumber = requireNonNull(luckyNumber, "luckyNumber is null");
            this.unluckyNumber = requireNonNull(unluckyNumber, "unluckyNumber is null");
            this.longNumber = requireNonNull(longNumber, "longNumber is null");
            this.updatedAt = updatedAt;
        }

        @JsonProperty
        public String getVendor()
        {
            return vendor;
        }

        @JsonProperty
        public String getCity()
        {
            return city;
        }

        @JsonProperty
        public List<String> getNeighbors()
        {
            return neighbors;
        }

        @JsonProperty("lucky_numbers")
        public List<Integer> getLuckyNumbers()
        {
            return luckyNumbers;
        }

        @JsonProperty
        public List<Float> getPrices()
        {
            return prices;
        }

        @JsonProperty("unlucky_numbers")
        public List<Double> getUnluckyNumbers()
        {
            return unluckyNumbers;
        }

        @JsonProperty("long_numbers")
        public List<Long> getLongNumbers()
        {
            return longNumbers;
        }

        @JsonProperty("lucky_number")
        public Integer getLuckyNumber()
        {
            return luckyNumber;
        }

        @JsonProperty
        public Float getPrice()
        {
            return price;
        }

        @JsonProperty("unlucky_number")
        public Double getUnluckyNumber()
        {
            return unluckyNumber;
        }

        @JsonProperty("long_number")
        public Long getLongNumber()
        {
            return longNumber;
        }

        @JsonProperty
        public long getUpdatedAt()
        {
            return updatedAt;
        }

        public static Object of(
                String vendor,
                String city,
                List<String> neighbors,
                List<Integer> luckyNumbers,
                List<Float> prices,
                List<Double> unluckyNumbers,
                List<Long> longNumbers,
                long offset)
        {
            return new TestingJsonRecord(vendor, city, neighbors, luckyNumbers, prices, unluckyNumbers, longNumbers, luckyNumbers.get(0), prices.get(0), unluckyNumbers.get(0), longNumbers.get(0), Instant.now().plusMillis(offset).getEpochSecond());
        }
    }

    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .isEqualTo(
                        "CREATE TABLE %s.%s.region (\n" +
                                "   regionkey bigint,\n" +
                                "   updated_at_seconds bigint,\n" +
                                "   name varchar,\n" +
                                "   comment varchar\n" +
                                ")",
                        getSession().getCatalog().orElseThrow(),
                        getSession().getSchema().orElseThrow());
    }

    @Override
    public void testSelectInformationSchemaColumns()
    {
        // Override because there's updated_at_seconds column
        assertThat(query("SELECT column_name FROM information_schema.columns WHERE table_schema = 'default' AND table_name = 'region'"))
                .skippingTypesCheck()
                .matches("VALUES 'regionkey', 'name', 'comment', 'updated_at_seconds'");
    }

    @Override
    public void testTopN()
    {
        // TODO https://github.com/trinodb/trino/issues/14045 Fix ORDER BY ... LIMIT query
        assertQueryFails("SELECT regionkey FROM nation ORDER BY name LIMIT 3",
                format("Segment query returned '%2$s' rows per split, maximum allowed is '%1$s' rows. with query \"SELECT \"regionkey\", \"name\" FROM nation_REALTIME  LIMIT 12\"", MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES, MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES + 1));
    }

    @Override
    public void testJoin()
    {
        // TODO https://github.com/trinodb/trino/issues/14046 Fix JOIN query
        assertQueryFails("SELECT n.name, r.name FROM nation n JOIN region r on n.regionkey = r.regionkey",
                format("Segment query returned '%2$s' rows per split, maximum allowed is '%1$s' rows. with query \"SELECT \"regionkey\", \"name\" FROM nation_REALTIME  LIMIT 12\"", MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES, MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES + 1));
    }

    @Test
    public void testRealType()
    {
        MaterializedResult result = computeActual("SELECT price FROM " + JSON_TABLE + " WHERE vendor = 'vendor1'");
        assertEquals(getOnlyElement(result.getTypes()), REAL);
        assertEquals(result.getOnlyValue(), 3.5F);
    }

    @Test
    public void testIntegerType()
    {
        assertThat(query("SELECT lucky_number FROM " + JSON_TABLE + " WHERE vendor = 'vendor1'"))
                .matches("VALUES (INTEGER '5')")
                .isFullyPushedDown();
    }

    @Test
    public void testBrokerColumnMappingForSelectQueries()
    {
        String expected = "VALUES" +
                "  ('3.5', 'vendor1')," +
                "  ('4.5', 'vendor2')," +
                "  ('5.5', 'vendor3')," +
                "  ('6.5', 'vendor4')," +
                "  ('7.5', 'vendor5')," +
                "  ('8.5', 'vendor6')";
        assertQuery("SELECT price, vendor FROM \"SELECT price, vendor FROM " + JSON_TABLE + " WHERE vendor != 'vendor7'\"", expected);
        assertQuery("SELECT price, vendor FROM \"SELECT * FROM " + JSON_TABLE + " WHERE vendor != 'vendor7'\"", expected);
        assertQuery("SELECT price, vendor FROM \"SELECT vendor, lucky_numbers, price FROM " + JSON_TABLE + " WHERE vendor != 'vendor7'\"", expected);
    }

    @Test
    public void testBrokerColumnMappingsForQueriesWithAggregates()
    {
        String passthroughQuery = "\"SELECT city, COUNT(*), MAX(price), SUM(lucky_number) " +
                "  FROM " + JSON_TABLE +
                "  WHERE vendor != 'vendor7'" +
                "  GROUP BY city\"";
        assertQuery("SELECT * FROM " + passthroughQuery, "VALUES" +
                "  ('New York', 2, 6.5, 14)," +
                "  ('Los Angeles', 4, 8.5, 31)");
        assertQuery("SELECT \"max(price)\", city, \"sum(lucky_number)\", \"count(*)\" FROM " + passthroughQuery, "VALUES" +
                "  (6.5, 'New York', 14, 2)," +
                "  (8.5, 'Los Angeles', 31, 4)");
        assertQuery("SELECT \"max(price)\", city, \"count(*)\" FROM " + passthroughQuery, "VALUES" +
                "  (6.5, 'New York', 2)," +
                "  (8.5, 'Los Angeles', 4)");
    }

    @Test
    public void testBrokerColumnMappingsForArrays()
    {
        assertQuery("SELECT ARRAY_MIN(unlucky_numbers), ARRAY_MAX(long_numbers), ELEMENT_AT(neighbors, 2), ARRAY_MIN(lucky_numbers), ARRAY_MAX(prices)" +
                        "  FROM \"SELECT unlucky_numbers, long_numbers, neighbors, lucky_numbers, prices" +
                        "  FROM " + JSON_TABLE +
                        "  WHERE vendor = 'vendor1'\"",
                "VALUES (-3.7, 20000000, 'bar1', 5, 5.5)");
        assertQuery("SELECT CARDINALITY(unlucky_numbers), CARDINALITY(long_numbers), CARDINALITY(neighbors), CARDINALITY(lucky_numbers), CARDINALITY(prices)" +
                        "  FROM \"SELECT unlucky_numbers, long_numbers, neighbors, lucky_numbers, prices" +
                        "  FROM " + JSON_TABLE +
                        "  WHERE vendor = 'vendor1'\"",
                "VALUES (3, 3, 3, 3, 2)");
    }

    @Test
    public void testCountStarQueries()
    {
        assertQuery("SELECT COUNT(*) FROM \"SELECT * FROM " + JSON_TABLE + " WHERE vendor != 'vendor7'\"", "VALUES(6)");
        assertQuery("SELECT COUNT(*) FROM " + JSON_TABLE + " WHERE vendor != 'vendor7'", "VALUES(6)");
        assertQuery("SELECT \"count(*)\" FROM \"SELECT COUNT(*) FROM " + JSON_TABLE + " WHERE vendor != 'vendor7'\"", "VALUES(6)");
    }

    @Test
    public void testBrokerQueriesWithAvg()
    {
        assertQuery("SELECT city, \"avg(lucky_number)\", \"avg(price)\", \"avg(long_number)\"" +
                "  FROM \"SELECT city, AVG(price), AVG(lucky_number), AVG(long_number) FROM " + JSON_TABLE + " WHERE vendor != 'vendor7' GROUP BY city\"", "VALUES" +
                "  ('New York', 7.0, 5.5, 10000.0)," +
                "  ('Los Angeles', 7.75, 6.25, 10000.0)");
        MaterializedResult result = computeActual("SELECT \"avg(lucky_number)\"" +
                "  FROM \"SELECT AVG(lucky_number) FROM my_table WHERE vendor in ('vendor2', 'vendor4')\"");
        assertEquals(getOnlyElement(result.getTypes()), DOUBLE);
        assertEquals(result.getOnlyValue(), 7.0);
    }

    @Test
    public void testNonLowerCaseColumnNames()
    {
        long rowCount = (long) computeScalar("SELECT COUNT(*) FROM " + MIXED_CASE_COLUMN_NAMES_TABLE);
        List<String> rows = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            rows.add(format("('string_%s', '%s', '%s')", i, i, initialUpdatedAt.plusMillis(i * 1000L).getEpochSecond()));
        }
        String mixedCaseColumnNamesTableValues = rows.stream().collect(joining(",", "VALUES ", ""));

        // Test segment query all rows
        assertQuery("SELECT stringcol, longcol, updatedatseconds" +
                        "  FROM " + MIXED_CASE_COLUMN_NAMES_TABLE,
                mixedCaseColumnNamesTableValues);

        // Test broker query all rows
        assertQuery("SELECT stringcol, longcol, updatedatseconds" +
                        "  FROM  \"SELECT updatedatseconds, longcol, stringcol FROM " + MIXED_CASE_COLUMN_NAMES_TABLE + "\"",
                mixedCaseColumnNamesTableValues);

        String singleRowValues = "VALUES (VARCHAR 'string_3', BIGINT '3', BIGINT '" + initialUpdatedAt.plusMillis(3 * 1000).getEpochSecond() + "')";

        // Test segment query single row
        assertThat(query("SELECT stringcol, longcol, updatedatseconds" +
                        "  FROM " + MIXED_CASE_COLUMN_NAMES_TABLE +
                        "  WHERE longcol = 3"))
                .matches(singleRowValues)
                .isFullyPushedDown();

        // Test broker query single row
        assertThat(query("SELECT stringcol, longcol, updatedatseconds" +
                        "  FROM  \"SELECT updatedatseconds, longcol, stringcol FROM " + MIXED_CASE_COLUMN_NAMES_TABLE +
                        "\" WHERE longcol = 3"))
                .matches(singleRowValues)
                .isFullyPushedDown();

        assertThat(query("SELECT AVG(longcol), MIN(longcol), MAX(longcol), APPROX_DISTINCT(longcol), SUM(longcol)" +
                "  FROM " + MIXED_CASE_COLUMN_NAMES_TABLE))
                .matches("VALUES (DOUBLE '1.5', BIGINT '0', BIGINT '3', BIGINT '4', BIGINT '6')")
                .isFullyPushedDown();

        assertThat(query("SELECT stringcol, AVG(longcol), MIN(longcol), MAX(longcol), APPROX_DISTINCT(longcol), SUM(longcol)" +
                "  FROM " + MIXED_CASE_COLUMN_NAMES_TABLE +
                "  GROUP BY stringcol"))
                .matches("VALUES (VARCHAR 'string_0', DOUBLE '0.0', BIGINT '0', BIGINT '0', BIGINT '1', BIGINT '0')," +
                        "  (VARCHAR 'string_1', DOUBLE '1.0', BIGINT '1', BIGINT '1', BIGINT '1', BIGINT '1')," +
                        "  (VARCHAR 'string_2', DOUBLE '2.0', BIGINT '2', BIGINT '2', BIGINT '1', BIGINT '2')," +
                        "  (VARCHAR 'string_3', DOUBLE '3.0', BIGINT '3', BIGINT '3', BIGINT '1', BIGINT '3')")
                .isFullyPushedDown();
    }

    @Test
    public void testNonLowerTable()
    {
        long rowCount = (long) computeScalar("SELECT COUNT(*) FROM " + MIXED_CASE_TABLE_NAME);
        List<String> rows = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            rows.add(format("('string_%s', '%s', '%s')", i, i, initialUpdatedAt.plusMillis(i * 1000L).getEpochSecond()));
        }

        String mixedCaseColumnNamesTableValues = rows.stream().collect(joining(",", "VALUES ", ""));

        // Test segment query all rows
        assertQuery("SELECT stringcol, longcol, updatedatseconds" +
                        "  FROM " + MIXED_CASE_TABLE_NAME,
                mixedCaseColumnNamesTableValues);

        // Test broker query all rows
        assertQuery("SELECT stringcol, longcol, updatedatseconds" +
                        "  FROM  \"SELECT updatedatseconds, longcol, stringcol FROM " + MIXED_CASE_TABLE_NAME + "\"",
                mixedCaseColumnNamesTableValues);

        String singleRowValues = "VALUES (VARCHAR 'string_3', BIGINT '3', BIGINT '" + initialUpdatedAt.plusMillis(3 * 1000).getEpochSecond() + "')";

        // Test segment query single row
        assertThat(query("SELECT stringcol, longcol, updatedatseconds" +
                "  FROM " + MIXED_CASE_TABLE_NAME +
                "  WHERE longcol = 3"))
                .matches(singleRowValues)
                .isFullyPushedDown();

        // Test broker query single row
        assertThat(query("SELECT stringcol, longcol, updatedatseconds" +
                "  FROM  \"SELECT updatedatseconds, longcol, stringcol FROM " + MIXED_CASE_TABLE_NAME +
                "\" WHERE longcol = 3"))
                .matches(singleRowValues)
                .isFullyPushedDown();

        // Test information schema
        assertQuery(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = 'default' AND table_name = 'mixedcase'",
                "VALUES 'stringcol', 'updatedatseconds', 'longcol'");
        assertQuery(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'mixedcase'",
                "VALUES 'stringcol', 'updatedatseconds', 'longcol'");
        assertEquals(
                computeActual("SHOW COLUMNS FROM default.mixedcase").getMaterializedRows().stream()
                        .map(row -> row.getField(0))
                        .collect(toImmutableSet()),
                ImmutableSet.of("stringcol", "updatedatseconds", "longcol"));
    }

    @Test
    public void testAmbiguousTables()
    {
        assertQueryFails("SELECT * FROM " + DUPLICATE_TABLE_LOWERCASE, "Ambiguous table names: (" + DUPLICATE_TABLE_LOWERCASE + ", " + DUPLICATE_TABLE_MIXED_CASE + "|" + DUPLICATE_TABLE_MIXED_CASE + ", " + DUPLICATE_TABLE_LOWERCASE + ")");
        assertQueryFails("SELECT * FROM " + DUPLICATE_TABLE_MIXED_CASE, "Ambiguous table names: (" + DUPLICATE_TABLE_LOWERCASE + ", " + DUPLICATE_TABLE_MIXED_CASE + "|" + DUPLICATE_TABLE_MIXED_CASE + ", " + DUPLICATE_TABLE_LOWERCASE + ")");
        assertQueryFails("SELECT * FROM \"SELECT * FROM " + DUPLICATE_TABLE_LOWERCASE + "\"", "Ambiguous table names: (" + DUPLICATE_TABLE_LOWERCASE + ", " + DUPLICATE_TABLE_MIXED_CASE + "|" + DUPLICATE_TABLE_MIXED_CASE + ", " + DUPLICATE_TABLE_LOWERCASE + ")");
        assertQueryFails("SELECT * FROM \"SELECT * FROM " + DUPLICATE_TABLE_MIXED_CASE + "\"", "Ambiguous table names: (" + DUPLICATE_TABLE_LOWERCASE + ", " + DUPLICATE_TABLE_MIXED_CASE + "|" + DUPLICATE_TABLE_MIXED_CASE + ", " + DUPLICATE_TABLE_LOWERCASE + ")");
        assertQueryFails("SELECT * FROM information_schema.columns", "Error listing table columns for catalog pinot: Ambiguous table names: (" + DUPLICATE_TABLE_LOWERCASE + ", " + DUPLICATE_TABLE_MIXED_CASE + "|" + DUPLICATE_TABLE_MIXED_CASE + ", " + DUPLICATE_TABLE_LOWERCASE + ")");
    }

    @Test
    public void testReservedKeywordColumnNames()
    {
        assertQuery("SELECT date FROM " + RESERVED_KEYWORD_TABLE + " WHERE date = '2021-09-30'", "VALUES '2021-09-30'");
        assertQuery("SELECT date FROM " + RESERVED_KEYWORD_TABLE + " WHERE date IN ('2021-09-30', '2021-10-01')", "VALUES '2021-09-30', '2021-10-01'");

        assertThat(query("SELECT date FROM  \"SELECT \"\"date\"\" FROM " + RESERVED_KEYWORD_TABLE + "\""))
                .matches("VALUES VARCHAR '2021-09-30', VARCHAR '2021-10-01'")
                .isFullyPushedDown();

        assertThat(query("SELECT date FROM  \"SELECT \"\"date\"\" FROM " + RESERVED_KEYWORD_TABLE + " WHERE \"\"date\"\" = '2021-09-30'\""))
                .matches("VALUES VARCHAR '2021-09-30'")
                .isFullyPushedDown();

        assertThat(query("SELECT date FROM  \"SELECT \"\"date\"\" FROM " + RESERVED_KEYWORD_TABLE + " WHERE \"\"date\"\" IN ('2021-09-30', '2021-10-01')\""))
                .matches("VALUES VARCHAR '2021-09-30', VARCHAR '2021-10-01'")
                .isFullyPushedDown();

        assertThat(query("SELECT date FROM  \"SELECT \"\"date\"\" FROM " + RESERVED_KEYWORD_TABLE + " ORDER BY \"\"date\"\"\""))
                .matches("VALUES VARCHAR '2021-09-30', VARCHAR '2021-10-01'")
                .isFullyPushedDown();

        assertThat(query("SELECT date, \"count(*)\" FROM  \"SELECT \"\"date\"\", COUNT(*) FROM " + RESERVED_KEYWORD_TABLE + " GROUP BY \"\"date\"\"\""))
                .matches("VALUES (VARCHAR '2021-09-30', BIGINT '1'), (VARCHAR '2021-10-01', BIGINT '1')")
                .isFullyPushedDown();

        assertThat(query("SELECT \"count(*)\" FROM  \"SELECT COUNT(*) FROM " + RESERVED_KEYWORD_TABLE + " ORDER BY COUNT(*)\""))
                .matches("VALUES BIGINT '2'")
                .isFullyPushedDown();

        assertQuery("SELECT \"as\" FROM " + RESERVED_KEYWORD_TABLE + " WHERE \"as\" = 'foo'", "VALUES 'foo'");
        assertQuery("SELECT \"as\" FROM " + RESERVED_KEYWORD_TABLE + " WHERE \"as\" IN ('foo', 'bar')", "VALUES 'foo', 'bar'");

        assertThat(query("SELECT \"as\" FROM  \"SELECT \"\"as\"\" FROM " + RESERVED_KEYWORD_TABLE + "\""))
                .matches("VALUES VARCHAR 'foo', VARCHAR 'bar'")
                .isFullyPushedDown();

        assertThat(query("SELECT \"as\" FROM  \"SELECT \"\"as\"\" FROM " + RESERVED_KEYWORD_TABLE + " WHERE \"\"as\"\" = 'foo'\""))
                .matches("VALUES VARCHAR 'foo'")
                .isFullyPushedDown();

        assertThat(query("SELECT \"as\" FROM  \"SELECT \"\"as\"\" FROM " + RESERVED_KEYWORD_TABLE + " WHERE \"\"as\"\" IN ('foo', 'bar')\""))
                .matches("VALUES VARCHAR 'foo', VARCHAR 'bar'")
                .isFullyPushedDown();

        assertThat(query("SELECT \"as\" FROM  \"SELECT \"\"as\"\" FROM " + RESERVED_KEYWORD_TABLE + " ORDER BY \"\"as\"\"\""))
                .matches("VALUES VARCHAR 'foo', VARCHAR 'bar'")
                .isFullyPushedDown();

        assertThat(query("SELECT \"as\", \"count(*)\" FROM  \"SELECT \"\"as\"\", COUNT(*) FROM " + RESERVED_KEYWORD_TABLE + " GROUP BY \"\"as\"\"\""))
                .matches("VALUES (VARCHAR 'foo', BIGINT '1'), (VARCHAR 'bar', BIGINT '1')")
                .isFullyPushedDown();
    }

    @Test
    public void testLimitForSegmentQueries()
    {
        // The connector will not allow segment queries to return more than MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES.
        // This is not a pinot error, it is enforced by the connector to avoid stressing pinot servers.
        assertQueryFails("SELECT string_col, updated_at_seconds FROM " + TOO_MANY_ROWS_TABLE,
                format("Segment query returned '%2$s' rows per split, maximum allowed is '%1$s' rows. with query \"SELECT \"string_col\", \"updated_at_seconds\" FROM too_many_rows_REALTIME  LIMIT %2$s\"", MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES, MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES + 1));

        // Verify the row count is greater than the max rows per segment limit
        assertQuery("SELECT \"count(*)\" FROM \"SELECT COUNT(*) FROM " + TOO_MANY_ROWS_TABLE + "\"", format("VALUES(%s)", MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES + 1));
    }

    @Test
    public void testBrokerQueryWithTooManyRowsForSegmentQuery()
    {
        // Note:
        // This data does not include the null row inserted in createQueryRunner().
        // This verifies that if the time column has a null value, pinot does not
        // ingest the row from kafka.
        List<String> tooManyRowsTableValues = new ArrayList<>();
        for (int i = 0; i < MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES + 1; i++) {
            tooManyRowsTableValues.add(format("('string_%s', '%s')", i, initialUpdatedAt.plusMillis(i * 1000L).getEpochSecond()));
        }

        // Explicit limit is necessary otherwise pinot returns 10 rows.
        // The limit is greater than the result size returned.
        assertQuery("SELECT string_col, updated_at_seconds" +
                        "  FROM  \"SELECT updated_at_seconds, string_col FROM " + TOO_MANY_ROWS_TABLE +
                        "  LIMIT " + (MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES + 2) + "\"",
                tooManyRowsTableValues.stream().collect(joining(",", "VALUES ", "")));
    }

    @Test
    public void testMaxLimitForPassthroughQueries()
    {
        assertQueryFails("SELECT string_col, updated_at_seconds" +
                        "  FROM  \"SELECT updated_at_seconds, string_col FROM " + TOO_MANY_BROKER_ROWS_TABLE +
                        "  LIMIT " + (MAX_ROWS_PER_SPLIT_FOR_BROKER_QUERIES + 1) + "\"",
                "Broker query returned '13' rows, maximum allowed is '12' rows. with query \"SELECT \"updated_at_seconds\", \"string_col\" FROM too_many_broker_rows LIMIT 13\"");

        // Pinot issue preventing Integer.MAX_VALUE from being a limit: https://github.com/apache/incubator-pinot/issues/7110
        // This is now resolved in pinot 0.8.0
        assertQuerySucceeds("SELECT * FROM \"SELECT string_col, long_col FROM " + ALL_TYPES_TABLE + " LIMIT " + Integer.MAX_VALUE + "\"");

        // Pinot broker requests do not handle limits greater than Integer.MAX_VALUE
        // Note that -2147483648 is due to an integer overflow in Pinot: https://github.com/apache/pinot/issues/7242
        assertQueryFails("SELECT * FROM \"SELECT string_col, long_col FROM " + ALL_TYPES_TABLE + " LIMIT " + ((long) Integer.MAX_VALUE + 1) + "\"",
                "(?s)Query SELECT \"string_col\", \"long_col\" FROM alltypes LIMIT -2147483648 encountered exception .* with query \"SELECT \"string_col\", \"long_col\" FROM alltypes LIMIT -2147483648\"");

        List<String> tooManyBrokerRowsTableValues = new ArrayList<>();
        for (int i = 0; i < MAX_ROWS_PER_SPLIT_FOR_BROKER_QUERIES; i++) {
            tooManyBrokerRowsTableValues.add(format("('string_%s', '%s')", i, initialUpdatedAt.plusMillis(i * 1000L).getEpochSecond()));
        }

        // Explicit limit is necessary otherwise pinot returns 10 rows.
        assertQuery("SELECT string_col, updated_at_seconds" +
                        "  FROM  \"SELECT updated_at_seconds, string_col FROM " + TOO_MANY_BROKER_ROWS_TABLE +
                        "  WHERE string_col != 'string_12'" +
                        "  LIMIT " + MAX_ROWS_PER_SPLIT_FOR_BROKER_QUERIES + "\"",
                tooManyBrokerRowsTableValues.stream().collect(joining(",", "VALUES ", "")));
    }

    @Test
    public void testCount()
    {
        assertQuery("SELECT \"count(*)\" FROM \"SELECT COUNT(*) FROM " + ALL_TYPES_TABLE + "\"", "VALUES " + MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES);
        // If no limit is supplied to a broker query, 10 arbitrary rows will be returned. Verify this behavior:
        MaterializedResult result = computeActual("SELECT * FROM \"SELECT bool_col FROM " + ALL_TYPES_TABLE + "\"");
        assertEquals(result.getRowCount(), DEFAULT_PINOT_LIMIT_FOR_BROKER_QUERIES);
    }

    @Test
    public void testNullBehavior()
    {
        // Verify the null behavior of pinot:

        // Default null value for timestamp single value columns is 0
        assertThat(query("SELECT timestamp_col" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE string_col = 'null'"))
                .matches("VALUES(TIMESTAMP '1970-01-01 00:00:00.000')")
                .isFullyPushedDown();

        // Default null value for long single value columns is 0
        assertThat(query("SELECT long_col" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'array_null'"))
                .matches("VALUES(BIGINT '0')")
                .isFullyPushedDown();

        // Default null value for long array values is Long.MIN_VALUE,
        assertThat(query("SELECT element_at(long_array_col, 1)" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'array_null'"))
                .matches("VALUES(BIGINT '" + Long.MIN_VALUE + "')")
                .isNotFullyPushedDown(ProjectNode.class);

        // Default null value for int single value columns is 0
        assertThat(query("SELECT int_col" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'null'"))
                .matches("VALUES(INTEGER '0')")
                .isFullyPushedDown();

        // Default null value for int array values is Integer.MIN_VALUE,
        assertThat(query("SELECT element_at(int_array_col, 1)" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'null'"))
                .matches("VALUES(INTEGER '" + Integer.MIN_VALUE + "')")
                .isNotFullyPushedDown(ProjectNode.class);

        // Verify a null value for an array with all null values is a single element.
        // The original value inserted from kafka is 5 null elements.
        assertThat(query("SELECT element_at(int_array_col, 1)" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'array_null'"))
                .matches("VALUES(INTEGER '" + Integer.MIN_VALUE + "')")
                .isNotFullyPushedDown(ProjectNode.class);

        // Verify default null value for array matches expected result
        assertThat(query("SELECT element_at(int_array_col_with_pinot_default, 1)" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'null'"))
                .matches("VALUES(INTEGER '7')")
                .isNotFullyPushedDown(ProjectNode.class);

        // Verify an array with null and non-null values omits the null values
        assertThat(query("SELECT int_array_col_with_pinot_default" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'array_null'"))
                .matches("VALUES(CAST(ARRAY[-1112, 753, -9238] AS ARRAY(INTEGER)))")
                .isFullyPushedDown();

        // Default null value for strings is the string 'null'
        assertThat(query("SELECT string_col" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE bytes_col = X'' AND element_at(string_array_col, 1) = 'null'"))
                .matches("VALUES (VARCHAR 'null')")
                .isNotFullyPushedDown(FilterNode.class);

        // Default array null value for strings is the string 'null'
        assertThat(query("SELECT element_at(string_array_col, 1)" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE bytes_col = X'' AND string_col = 'null'"))
                .matches("VALUES (VARCHAR 'null')")
                .isNotFullyPushedDown(ProjectNode.class);

        // Default null value for booleans is the string 'null'
        // Booleans are treated as a string
        assertThat(query("SELECT bool_col" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'null'"))
                .matches("VALUES (false)")
                .isFullyPushedDown();

        // Default null value for pinot BYTES type (varbinary) is the string 'null'
        // BYTES values are treated as a strings
        // BYTES arrays are not supported
        assertThat(query("SELECT bytes_col" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'null'"))
                .matches("VALUES (VARBINARY '')")
                .isFullyPushedDown();

        // Default null value for float single value columns is 0.0F
        assertThat(query("SELECT float_col" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'array_null'"))
                .matches("VALUES(REAL '0.0')")
                .isFullyPushedDown();

        // Default null value for float array values is -INFINITY,
        assertThat(query("SELECT element_at(float_array_col, 1)" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'array_null'"))
                .matches("VALUES(CAST(-POWER(0, -1) AS REAL))")
                .isNotFullyPushedDown(ProjectNode.class);

        // Default null value for double single value columns is 0.0D
        assertThat(query("SELECT double_col" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'array_null'"))
                .matches("VALUES(DOUBLE '0.0')")
                .isFullyPushedDown();

        // Default null value for double array values is -INFINITY,
        assertThat(query("SELECT element_at(double_array_col, 1)" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'array_null'"))
                .matches("VALUES(-POWER(0, -1))")
                .isNotFullyPushedDown(ProjectNode.class);

        // Null behavior for arrays:
        // Default value for a "null" array is 1 element with default null array value,
        // Values are tested above, this test is to verify pinot returns an array with 1 element.
        assertThat(query("SELECT CARDINALITY(string_array_col)," +
                        "  CARDINALITY(int_array_col_with_pinot_default)," +
                        "  CARDINALITY(int_array_col)," +
                        "  CARDINALITY(float_array_col)," +
                        "  CARDINALITY(long_array_col)" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'null'"))
                .matches("VALUES (BIGINT '1', BIGINT '1', BIGINT '1', BIGINT '1', BIGINT '1')")
                .isNotFullyPushedDown(ProjectNode.class);

        // If an array contains both null and non-null values, the null values are omitted:
        // There are 5 values in the avro records, but only the 3 non-null values are in pinot
        assertThat(query("SELECT CARDINALITY(string_array_col)," +
                        "  CARDINALITY(int_array_col_with_pinot_default)," +
                        "  CARDINALITY(int_array_col)," +
                        "  CARDINALITY(float_array_col)," +
                        "  CARDINALITY(long_array_col)" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'array_null'"))
                .matches("VALUES (BIGINT '3', BIGINT '3', BIGINT '1', BIGINT '1', BIGINT '1')")
                .isNotFullyPushedDown(ProjectNode.class);

        // IS NULL and IS NOT NULL is not pushed down in Pinot due to inconsistent results.
        // see https://docs.pinot.apache.org/developers/advanced/null-value-support
        assertThat(query("""
                SELECT COUNT(*)
                FROM alltypes
                WHERE string_col IS NULL"""))
                .matches("VALUES (BIGINT '0')")
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query("""
                SELECT COUNT(*)
                FROM alltypes
                WHERE string_col IS NOT NULL"""))
                .matches("VALUES (BIGINT '11')")
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query("""
                SELECT COUNT(*)
                FROM alltypes
                WHERE string_col = 'string_0' OR string_col IS NULL"""))
                .matches("VALUES (BIGINT '1')")
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query("""
                SELECT COUNT(*)
                FROM alltypes
                WHERE string_col = 'string_0'"""))
                .matches("VALUES (BIGINT '1')")
                .isFullyPushedDown();

        assertThat(query("""
                SELECT COUNT(*)
                FROM alltypes
                WHERE string_col != 'string_0' OR string_col IS NULL"""))
                .matches("VALUES (BIGINT '10')")
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query("""
                SELECT COUNT(*)
                FROM alltypes
                WHERE string_col != 'string_0'"""))
                .matches("VALUES (BIGINT '10')")
                .isFullyPushedDown();

        assertThat(query("""
                SELECT COUNT(*)
                FROM alltypes
                WHERE string_col NOT IN ('null', 'array_null') OR string_col IS NULL"""))
                .matches("VALUES (BIGINT '9')")
                .isNotFullyPushedDown(FilterNode.class);

        // VARCHAR NOT IN is pushed down
        assertThat(query("""
                SELECT COUNT(*)
                FROM alltypes
                WHERE string_col NOT IN ('null', 'array_null')"""))
                .matches("VALUES (BIGINT '9')")
                .isFullyPushedDown();

        assertThat(query("""
                SELECT COUNT(*)
                FROM alltypes
                WHERE string_col IN ('null', 'array_null') OR string_col IS NULL"""))
                .matches("VALUES (BIGINT '2')")
                .isNotFullyPushedDown(FilterNode.class);

        // VARCHAR IN is pushed down
        assertThat(query("""
                SELECT COUNT(*)
                FROM alltypes
                WHERE string_col IN ('null', 'array_null')"""))
                .matches("VALUES (BIGINT '2')")
                .isFullyPushedDown();

        assertThat(query("""
                SELECT COUNT(*)
                FROM alltypes
                WHERE long_col IS NULL"""))
                .matches("VALUES (BIGINT '0')")
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query("""
                SELECT COUNT(*)
                FROM alltypes
                WHERE long_col IS NOT NULL"""))
                .matches("VALUES (BIGINT '11')")
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query("""
                SELECT COUNT(*)
                FROM alltypes
                WHERE long_col = -3147483645 OR long_col IS NULL"""))
                .matches("VALUES (BIGINT '1')")
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query("""
                SELECT COUNT(*)
                FROM alltypes
                WHERE long_col = -3147483645"""))
                .matches("VALUES (BIGINT '1')")
                .isFullyPushedDown();

        assertThat(query("""
                SELECT COUNT(*)
                FROM alltypes
                WHERE long_col != -3147483645 OR long_col IS NULL"""))
                .matches("VALUES (BIGINT '10')")
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query("""
                SELECT COUNT(*)
                FROM alltypes
                WHERE long_col != -3147483645"""))
                .matches("VALUES (BIGINT '10')")
                .isFullyPushedDown();

        assertThat(query("""
                SELECT long_col
                FROM alltypes
                WHERE long_col NOT IN (-3147483645, -3147483646, -3147483647) OR long_col IS NULL"""))
                .matches("""
                        VALUES (BIGINT '-3147483644'),
                        (BIGINT '-3147483643'),
                        (BIGINT '-3147483642'),
                        (BIGINT '-3147483641'),
                        (BIGINT '-3147483640'),
                        (BIGINT '-3147483639'),
                        (BIGINT '0'),
                        (BIGINT '0')""")
                .isNotFullyPushedDown(FilterNode.class);

        // BIGINT NOT IN is pushed down
        assertThat(query("""
                SELECT long_col
                FROM alltypes
                WHERE long_col NOT IN (-3147483645, -3147483646, -3147483647)"""))
                .matches("""
                        VALUES (BIGINT '-3147483644'),
                        (BIGINT '-3147483643'),
                        (BIGINT '-3147483642'),
                        (BIGINT '-3147483641'),
                        (BIGINT '-3147483640'),
                        (BIGINT '-3147483639'),
                        (BIGINT '0'),
                        (BIGINT '0')""")
                .isFullyPushedDown();

        assertThat(query("""
                SELECT COUNT(*)
                FROM alltypes
                WHERE long_col IN (-3147483645, -3147483646, -3147483647) OR long_col IS NULL"""))
                .matches("VALUES (BIGINT '3')")
                .isNotFullyPushedDown(FilterNode.class);

        // BIGINT IN is pushed down
        assertThat(query("""
                SELECT COUNT(*)
                FROM alltypes
                WHERE long_col IN (-3147483645, -3147483646, -3147483647)"""))
                .matches("VALUES (BIGINT '3')")
                .isFullyPushedDown();

        assertThat(query("""
                SELECT int_col
                FROM alltypes
                WHERE int_col NOT IN (0, 54, 56) OR int_col IS NULL"""))
                .matches("VALUES (55), (55), (55)")
                .isNotFullyPushedDown(FilterNode.class);

        // INTEGER NOT IN is pushed down
        assertThat(query("""
                SELECT int_col
                FROM alltypes
                WHERE int_col NOT IN (0, 54, 56)"""))
                .matches("VALUES (55), (55), (55)")
                .isFullyPushedDown();

        assertThat(query("""
                SELECT COUNT(*)
                FROM alltypes
                WHERE int_col IN (0, 54, 56) OR int_col IS NULL"""))
                .matches("VALUES (BIGINT '8')")
                .isNotFullyPushedDown(FilterNode.class);

        // INTEGER IN is pushed down
        assertThat(query("""
                SELECT COUNT(*)
                FROM alltypes
                WHERE int_col IN (0, 54, 56)"""))
                .matches("VALUES (BIGINT '8')")
                .isFullyPushedDown();

        assertThat(query("""
                SELECT COUNT(*)
                FROM alltypes
                WHERE bool_col OR bool_col IS NULL"""))
                .matches("VALUES (BIGINT '9')")
                .isNotFullyPushedDown(FilterNode.class);

        // BOOLEAN values are pushed down
        assertThat(query("""
                SELECT COUNT(*)
                FROM alltypes
                WHERE bool_col"""))
                .matches("VALUES (BIGINT '9')")
                .isFullyPushedDown();

        assertThat(query("""
                SELECT COUNT(*)
                FROM alltypes
                WHERE NOT bool_col OR bool_col IS NULL"""))
                .matches("VALUES (BIGINT '2')")
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query("""
                SELECT COUNT(*)
                FROM alltypes
                WHERE NOT bool_col"""))
                .matches("VALUES (BIGINT '2')")
                .isFullyPushedDown();

        assertThat(query("""
                SELECT COUNT(*)
                FROM alltypes
                WHERE float_col NOT IN (-2.33, -3.33, -4.33, -5.33, -6.33, -7.33) OR float_col IS NULL"""))
                .matches("VALUES (BIGINT '5')")
                .isNotFullyPushedDown(FilterNode.class);

        // REAL values are not pushed down, applyFilter is not called
        assertThat(query("""
                SELECT COUNT(*)
                FROM alltypes
                WHERE float_col NOT IN (-2.33, -3.33, -4.33, -5.33, -6.33, -7.33)"""))
                .matches("VALUES (BIGINT '5')")
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query("""
                SELECT COUNT(*)
                FROM alltypes
                WHERE double_col NOT IN (0.0, -16.33, -17.33) OR double_col IS NULL"""))
                .matches("VALUES (BIGINT '7')")
                .isNotFullyPushedDown(FilterNode.class);

        // DOUBLE values are not pushed down, applyFilter is not called
        assertThat(query("""
                SELECT COUNT(*)
                FROM alltypes
                WHERE double_col NOT IN (0.0, -16.33, -17.33)"""))
                .matches("VALUES (BIGINT '7')")
                .isNotFullyPushedDown(FilterNode.class);
    }

    @Test
    public void testBrokerQueriesWithCaseStatementsInFilter()
    {
        // Need to invoke the UPPER function since identifiers are lower case
        assertQuery("SELECT city, \"avg(lucky_number)\", \"avg(price)\", \"avg(long_number)\"" +
                "  FROM \"SELECT city, AVG(price), AVG(lucky_number), AVG(long_number) FROM my_table WHERE " +
                "  CASE WHEN city = CONCAT(CONCAT(UPPER('N'), 'ew ', ''), CONCAT(UPPER('Y'), 'ork', ''), '') THEN city WHEN city = CONCAT(CONCAT(UPPER('L'), 'os ', ''), CONCAT(UPPER('A'), 'ngeles', ''), '') THEN city ELSE 'gotham' END != 'gotham'" +
                "  AND CASE WHEN vendor = 'vendor1' THEN 'vendor1' WHEN vendor = 'vendor2' THEN 'vendor2' ELSE vendor END != 'vendor7' GROUP BY city\"", "VALUES" +
                "  ('New York', 7.0, 5.5, 10000.0)," +
                "  ('Los Angeles', 7.75, 6.25, 10000.0)");
    }

    @Test
    public void testFilterWithRealLiteral()
    {
        String expectedSingleValue = "VALUES (REAL '3.5', VARCHAR 'vendor1')";
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE price = 3.5")).matches(expectedSingleValue).isFullyPushedDown();
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE price <= 3.5")).matches(expectedSingleValue).isFullyPushedDown();
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE price BETWEEN 3 AND 4")).matches(expectedSingleValue).isFullyPushedDown();
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE price > 3 AND price < 4")).matches(expectedSingleValue).isFullyPushedDown();
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE price >= 3.5 AND price <= 4")).matches(expectedSingleValue).isFullyPushedDown();
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE price < 3.6")).matches(expectedSingleValue).isFullyPushedDown();
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE price IN (3.5)")).matches(expectedSingleValue).isFullyPushedDown();
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE price IN (3.5, 4)")).matches(expectedSingleValue).isFullyPushedDown();
        // NOT IN is not pushed down for real type
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE price NOT IN (4.5, 5.5, 6.5, 7.5, 8.5, 9.5)")).isNotFullyPushedDown(FilterNode.class);

        String expectedMultipleValues = "VALUES" +
                "  (REAL '3.5', VARCHAR 'vendor1')," +
                "  (REAL '4.5', VARCHAR 'vendor2')";
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE price < 4.6")).matches(expectedMultipleValues).isFullyPushedDown();
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE price BETWEEN 3.5 AND 4.5")).matches(expectedMultipleValues).isFullyPushedDown();

        String expectedMaxValue = "VALUES (REAL '9.5', VARCHAR 'vendor7')";
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE price > 9")).matches(expectedMaxValue).isFullyPushedDown();
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE price >= 9")).matches(expectedMaxValue).isFullyPushedDown();
    }

    @Test
    public void testArrayFilter()
    {
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE vendor != 'vendor7' AND prices = ARRAY[3.5, 5.5]"))
                .matches("VALUES (REAL '3.5', VARCHAR 'vendor1')")
                .isNotFullyPushedDown(FilterNode.class);

        // Array filters are not pushed down, as there are no array literals in pinot
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE prices = ARRAY[3.5, 5.5]")).isNotFullyPushedDown(FilterNode.class);
    }

    @Test
    public void testLimitPushdown()
    {
        assertThat(query("SELECT string_col, long_col FROM " + "\"SELECT string_col, long_col, bool_col FROM " + ALL_TYPES_TABLE + " WHERE int_col > 0\" " +
                "  WHERE bool_col = false LIMIT " + MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES))
                .isFullyPushedDown();
        assertThat(query("SELECT string_col, long_col FROM " + ALL_TYPES_TABLE + "  WHERE int_col >0 AND bool_col = false LIMIT " + MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES))
                .isNotFullyPushedDown(LimitNode.class);
    }

    /**
     * https://github.com/trinodb/trino/issues/8307
     */
    @Test
    public void testInformationSchemaColumnsTableNotExist()
    {
        assertThat(query("SELECT * FROM pinot.information_schema.columns WHERE table_name = 'table_not_exist'"))
                .returnsEmptyResult();
    }

    @Test
    public void testAggregationPushdown()
    {
        // Without the limit inside the passthrough query, pinot will only return 10 rows
        assertThat(query("SELECT COUNT(*) FROM \"SELECT * FROM " + ALL_TYPES_TABLE + " LIMIT " + MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES + "\""))
                .isFullyPushedDown();

        // Test aggregates with no grouping columns
        assertThat(query("SELECT COUNT(*)," +
                "  MIN(int_col), MAX(int_col)," +
                "  MIN(long_col), MAX(long_col), AVG(long_col), SUM(long_col)," +
                "  MIN(float_col), MAX(float_col), AVG(float_col), SUM(float_col)," +
                "  MIN(double_col), MAX(double_col), AVG(double_col), SUM(double_col)," +
                "  MIN(timestamp_col), MAX(timestamp_col)" +
                "  FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();

        // Test aggregates with no grouping columns with a limit
        assertThat(query("SELECT COUNT(*)," +
                "  MIN(int_col), MAX(int_col)," +
                "  MIN(long_col), MAX(long_col), AVG(long_col), SUM(long_col)," +
                "  MIN(float_col), MAX(float_col), AVG(float_col), SUM(float_col)," +
                "  MIN(double_col), MAX(double_col), AVG(double_col), SUM(double_col)," +
                "  MIN(timestamp_col), MAX(timestamp_col)" +
                "  FROM " + ALL_TYPES_TABLE +
                "  LIMIT " + MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES))
                .isFullyPushedDown();

        // Test aggregates with no grouping columns with a filter
        assertThat(query("SELECT COUNT(*)," +
                "  MIN(int_col), MAX(int_col)," +
                "  MIN(long_col), MAX(long_col), AVG(long_col), SUM(long_col)," +
                "  MIN(float_col), MAX(float_col), AVG(float_col), SUM(float_col)," +
                "  MIN(double_col), MAX(double_col), AVG(double_col), SUM(double_col)," +
                "  MIN(timestamp_col), MAX(timestamp_col)" +
                "  FROM " + ALL_TYPES_TABLE + " WHERE long_col < 4147483649"))
                .isFullyPushedDown();

        // Test aggregates with no grouping columns with a filter and limit
        assertThat(query("SELECT COUNT(*)," +
                "  MIN(int_col), MAX(int_col)," +
                "  MIN(long_col), MAX(long_col), AVG(long_col), SUM(long_col)," +
                "  MIN(float_col), MAX(float_col), AVG(float_col), SUM(float_col)," +
                "  MIN(double_col), MAX(double_col), AVG(double_col), SUM(double_col)," +
                "  MIN(timestamp_col), MAX(timestamp_col)" +
                "  FROM " + ALL_TYPES_TABLE + " WHERE long_col < 4147483649" +
                "  LIMIT " + MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES))
                .isFullyPushedDown();

        // Test aggregates with one grouping column
        assertThat(query("SELECT bool_col, COUNT(*)," +
                "  MIN(int_col), MAX(int_col)," +
                "  MIN(long_col), MAX(long_col), AVG(long_col), SUM(long_col)," +
                "  MIN(float_col), MAX(float_col), AVG(float_col), SUM(float_col)," +
                "  MIN(double_col), MAX(double_col), AVG(double_col), SUM(double_col)," +
                "  MIN(timestamp_col), MAX(timestamp_col)" +
                "  FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isFullyPushedDown();

        // Test aggregates with one grouping column and a limit
        assertThat(query("SELECT string_col, COUNT(*)," +
                "  MIN(int_col), MAX(int_col)," +
                "  MIN(long_col), MAX(long_col), AVG(long_col), SUM(long_col)," +
                "  MIN(float_col), MAX(float_col), AVG(float_col), SUM(float_col)," +
                "  MIN(double_col), MAX(double_col), AVG(double_col), SUM(double_col)," +
                "  MIN(timestamp_col), MAX(timestamp_col)" +
                "  FROM " + ALL_TYPES_TABLE + " GROUP BY string_col" +
                "  LIMIT " + MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES))
                .isFullyPushedDown();

        // Test aggregates with one grouping column and a filter
        assertThat(query("SELECT bool_col, COUNT(*)," +
                "  MIN(int_col), MAX(int_col)," +
                "  MIN(long_col), MAX(long_col), AVG(long_col), SUM(long_col)," +
                "  MIN(float_col), MAX(float_col), AVG(float_col), SUM(float_col)," +
                "  MIN(double_col), MAX(double_col), AVG(double_col), SUM(double_col)," +
                "  MIN(timestamp_col), MAX(timestamp_col)" +
                "  FROM " + ALL_TYPES_TABLE + " WHERE long_col < 4147483649 GROUP BY bool_col"))
                .isFullyPushedDown();

        // Test aggregates with one grouping column, a filter and a limit
        assertThat(query("SELECT string_col, COUNT(*)," +
                "  MIN(int_col), MAX(int_col)," +
                "  MIN(long_col), MAX(long_col), AVG(long_col), SUM(long_col)," +
                "  MIN(float_col), MAX(float_col), AVG(float_col), SUM(float_col)," +
                "  MIN(double_col), MAX(double_col), AVG(double_col), SUM(double_col)," +
                "  MIN(timestamp_col), MAX(timestamp_col)" +
                "  FROM " + ALL_TYPES_TABLE + " WHERE long_col < 4147483649 GROUP BY string_col" +
                "  LIMIT " + MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES))
                .isFullyPushedDown();

        // Test single row from pinot where filter results in an empty result set.
        // A direct pinot query would return 1 row with default values, not null values.
        assertThat(query("SELECT COUNT(*)," +
                "  MIN(int_col), MAX(int_col)," +
                "  MIN(long_col), MAX(long_col), AVG(long_col), SUM(long_col)," +
                "  MIN(float_col), MAX(float_col), AVG(float_col), SUM(float_col)," +
                "  MIN(double_col), MAX(double_col), AVG(double_col), SUM(double_col)," +
                "  MIN(timestamp_col), MAX(timestamp_col)" +
                "  FROM " + ALL_TYPES_TABLE + " WHERE long_col > 4147483649"))
                .isFullyPushedDown();

        // Ensure that isNullOnEmptyGroup is handled correctly for passthrough queries as well
        assertThat(query("SELECT \"count(*)\", \"distinctcounthll(string_col)\", \"distinctcount(string_col)\", \"sum(created_at_seconds)\", \"max(created_at_seconds)\"" +
                "  FROM \"SELECT count(*), distinctcounthll(string_col), distinctcount(string_col), sum(created_at_seconds), max(created_at_seconds) FROM " + DATE_TIME_FIELDS_TABLE + " WHERE created_at_seconds = 0\""))
                .matches("VALUES (BIGINT '0', BIGINT '0', INTEGER '0', CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE))")
                .isFullyPushedDown();

        // Test passthrough queries with no aggregates
        assertThat(query("SELECT string_col, COUNT(*)," +
                "  MIN(int_col), MAX(int_col)," +
                "  MIN(long_col), MAX(long_col), AVG(long_col), SUM(long_col)," +
                "  MIN(float_col), MAX(float_col), AVG(float_col), SUM(float_col)," +
                "  MIN(double_col), MAX(double_col), AVG(double_col), SUM(double_col)," +
                "  MIN(timestamp_col), MAX(timestamp_col)" +
                "  FROM \"SELECT * FROM " + ALL_TYPES_TABLE + " WHERE long_col > 4147483649" +
                "  LIMIT " + MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES + "\"  GROUP BY string_col"))
                .isFullyPushedDown();

        // Passthrough queries with aggregates will not push down more aggregations.
        assertThat(query("SELECT bool_col, \"count(*)\", COUNT(*) FROM \"SELECT bool_col, count(*) FROM " +
                ALL_TYPES_TABLE + " GROUP BY bool_col\" GROUP BY bool_col, \"count(*)\""))
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, ProjectNode.class);

        assertThat(query("SELECT bool_col, \"max(long_col)\", COUNT(*) FROM \"SELECT bool_col, max(long_col) FROM " +
                ALL_TYPES_TABLE + " GROUP BY bool_col\" GROUP BY bool_col, \"max(long_col)\""))
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, ProjectNode.class);

        assertThat(query("SELECT int_col, COUNT(*) FROM " + ALL_TYPES_TABLE + " GROUP BY int_col LIMIT " + MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES))
                .isFullyPushedDown();

        // count(<column>) should not be pushed down, as pinot currently only implements count(*)
        assertThat(query("SELECT bool_col, COUNT(long_col)" +
                "  FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, ProjectNode.class);

        // AVG on INTEGER columns is not pushed down
        assertThat(query("SELECT string_col, AVG(int_col) FROM " + ALL_TYPES_TABLE + " GROUP BY string_col"))
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, ProjectNode.class);

        // SUM on INTEGER columns is not pushed down
        assertThat(query("SELECT string_col, SUM(int_col) FROM " + ALL_TYPES_TABLE + " GROUP BY string_col"))
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, ProjectNode.class);

        // MIN on VARCHAR columns is not pushed down
        assertThat(query("SELECT bool_col, MIN(string_col)" +
                "  FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, ProjectNode.class);

        // MAX on VARCHAR columns is not pushed down
        assertThat(query("SELECT bool_col, MAX(string_col)" +
                "  FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, ProjectNode.class);

        // COUNT on VARCHAR columns is not pushed down
        assertThat(query("SELECT bool_col, COUNT(string_col)" +
                "  FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, ProjectNode.class);

        // Distinct on varchar is pushed down
        assertThat(query("SELECT DISTINCT string_col FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        // Distinct on bool is pushed down
        assertThat(query("SELECT DISTINCT bool_col FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        // Distinct on double is pushed down
        assertThat(query("SELECT DISTINCT double_col FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        // Distinct on float is pushed down
        assertThat(query("SELECT DISTINCT float_col FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        // Distinct on long is pushed down
        assertThat(query("SELECT DISTINCT long_col FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        // Distinct on timestamp is pushed down
        assertThat(query("SELECT DISTINCT timestamp_col FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        // Distinct on int is partially pushed down
        assertThat(query("SELECT DISTINCT int_col FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();

        // Distinct on 2 columns for supported types:
        assertThat(query("SELECT DISTINCT bool_col, string_col FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        assertThat(query("SELECT DISTINCT bool_col, double_col FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        assertThat(query("SELECT DISTINCT bool_col, float_col FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        assertThat(query("SELECT DISTINCT bool_col, long_col FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        assertThat(query("SELECT DISTINCT bool_col, timestamp_col FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        assertThat(query("SELECT DISTINCT bool_col, int_col FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();

        // Test distinct for mixed case values
        assertThat(query("SELECT DISTINCT string_col FROM " + MIXED_CASE_DISTINCT_TABLE))
                .isFullyPushedDown();

        // Test count distinct for mixed case values
        assertThat(query("SELECT COUNT(DISTINCT string_col) FROM " + MIXED_CASE_DISTINCT_TABLE))
                .isFullyPushedDown();

        // Approx distinct for mixed case values
        assertThat(query("SELECT approx_distinct(string_col) FROM " + MIXED_CASE_DISTINCT_TABLE))
                .isFullyPushedDown();

        // Approx distinct on varchar is pushed down
        assertThat(query("SELECT approx_distinct(string_col) FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        // Approx distinct on bool is pushed down
        assertThat(query("SELECT approx_distinct(bool_col) FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        // Approx distinct on double is pushed down
        assertThat(query("SELECT approx_distinct(double_col) FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        // Approx distinct on float is pushed down
        assertThat(query("SELECT approx_distinct(float_col) FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        // Approx distinct on long is pushed down
        assertThat(query("SELECT approx_distinct(long_col) FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        // Approx distinct on int is partially pushed down
        assertThat(query("SELECT approx_distinct(int_col) FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();

        // Approx distinct on 2 columns for supported types:
        assertThat(query("SELECT bool_col, approx_distinct(string_col) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isFullyPushedDown();
        assertThat(query("SELECT bool_col, approx_distinct(double_col) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isFullyPushedDown();
        assertThat(query("SELECT bool_col, approx_distinct(float_col) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isFullyPushedDown();
        assertThat(query("SELECT bool_col, approx_distinct(long_col) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isFullyPushedDown();
        assertThat(query("SELECT bool_col, approx_distinct(int_col) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isFullyPushedDown();

        // Distinct count is fully pushed down by default
        assertThat(query("SELECT bool_col, COUNT(DISTINCT string_col) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isFullyPushedDown();
        assertThat(query("SELECT bool_col, COUNT(DISTINCT double_col) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isFullyPushedDown();
        assertThat(query("SELECT bool_col, COUNT(DISTINCT float_col) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isFullyPushedDown();
        assertThat(query("SELECT bool_col, COUNT(DISTINCT long_col) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isFullyPushedDown();
        assertThat(query("SELECT bool_col, COUNT(DISTINCT int_col) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isFullyPushedDown();
        // Test queries with no grouping columns
        assertThat(query("SELECT COUNT(DISTINCT string_col) FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        assertThat(query("SELECT COUNT(DISTINCT bool_col) FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        assertThat(query("SELECT COUNT(DISTINCT double_col) FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        assertThat(query("SELECT COUNT(DISTINCT float_col) FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        assertThat(query("SELECT COUNT(DISTINCT long_col) FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        assertThat(query("SELECT COUNT(DISTINCT int_col) FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();

        // Aggregation is not pushed down for queries with count distinct and other aggregations
        assertThat(query("SELECT bool_col, MAX(long_col), COUNT(DISTINCT long_col) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, MarkDistinctNode.class, ExchangeNode.class, ExchangeNode.class, ProjectNode.class);
        assertThat(query("SELECT bool_col, COUNT(DISTINCT long_col), MAX(long_col) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, MarkDistinctNode.class, ExchangeNode.class, ExchangeNode.class, ProjectNode.class);
        assertThat(query("SELECT bool_col, COUNT(*), COUNT(DISTINCT long_col) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, MarkDistinctNode.class, ExchangeNode.class, ExchangeNode.class, ProjectNode.class);
        assertThat(query("SELECT bool_col, COUNT(DISTINCT long_col), COUNT(*) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, MarkDistinctNode.class, ExchangeNode.class, ExchangeNode.class, ProjectNode.class);
        // Test queries with no grouping columns
        assertThat(query("SELECT MAX(long_col), COUNT(DISTINCT long_col) FROM " + ALL_TYPES_TABLE))
                .isNotFullyPushedDown(AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, MarkDistinctNode.class, ExchangeNode.class, ExchangeNode.class, ProjectNode.class);
        assertThat(query("SELECT COUNT(DISTINCT long_col), MAX(long_col) FROM " + ALL_TYPES_TABLE))
                .isNotFullyPushedDown(AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, MarkDistinctNode.class, ExchangeNode.class, ExchangeNode.class, ProjectNode.class);
        assertThat(query("SELECT COUNT(*), COUNT(DISTINCT long_col) FROM " + ALL_TYPES_TABLE))
                .isNotFullyPushedDown(AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, MarkDistinctNode.class, ExchangeNode.class, ExchangeNode.class, ProjectNode.class);
        assertThat(query("SELECT COUNT(DISTINCT long_col), COUNT(*) FROM " + ALL_TYPES_TABLE))
                .isNotFullyPushedDown(AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, MarkDistinctNode.class, ExchangeNode.class, ExchangeNode.class, ProjectNode.class);

        Session countDistinctPushdownDisabledSession = Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty("pinot", "count_distinct_pushdown_enabled", "false")
                .build();

        // Distinct count is partially pushed down when the distinct_count_pushdown_enabled session property is disabled
        assertThat(query(countDistinctPushdownDisabledSession, "SELECT bool_col, COUNT(DISTINCT long_col) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, ProjectNode.class);
        // Test query with no grouping columns
        assertThat(query(countDistinctPushdownDisabledSession, "SELECT COUNT(DISTINCT long_col) FROM " + ALL_TYPES_TABLE))
                .isNotFullyPushedDown(AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class);

        // Ensure that count(<column name>) is not pushed down even when a broker query is present
        // This is also done as the second step of count distinct but should not be pushed down in this case.
        assertThat(query("SELECT COUNT(long_col) FROM \"SELECT long_col FROM " + ALL_TYPES_TABLE + "\""))
                .isNotFullyPushedDown(AggregationNode.class);

        // Ensure that count(<column name>) is not pushed down even when a broker query is present and has grouping columns
        // This is also done as the second step of count distinct but should not be pushed down in this case.
        assertThat(query("SELECT bool_col, COUNT(long_col) FROM \"SELECT bool_col, long_col FROM " + ALL_TYPES_TABLE + "\" GROUP BY bool_col"))
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, ProjectNode.class);

        // Ensure that count(<column name>) is not pushed down even if the query contains a matching grouping column
        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(() -> query("SELECT COUNT(long_col) FROM \"SELECT long_col FROM " + ALL_TYPES_TABLE + " GROUP BY long_col\""))
                .withRootCauseInstanceOf(RuntimeException.class)
                .withMessage("Operation not supported for DISTINCT aggregation function");

        // Ensure that count(<column name>) with grouping columns is not pushed down even if the query contains a matching grouping column
        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(() -> query("SELECT bool_col, COUNT(long_col) FROM \"SELECT bool_col, long_col FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col, long_col\""))
                .withRootCauseInstanceOf(RuntimeException.class)
                .withMessage("Operation not supported for DISTINCT aggregation function");

        // Verify that count(<column name>) is pushed down only when it matches a COUNT(DISTINCT <column name>) query
        assertThat(query("""
                SELECT COUNT(bool_col) FROM
                (SELECT bool_col FROM alltypes GROUP BY bool_col)
                """))
                .matches("VALUES (BIGINT '2')")
                .isFullyPushedDown();
        assertThat(query("""
                SELECT bool_col, COUNT(long_col) FROM
                (SELECT bool_col, long_col FROM alltypes GROUP BY bool_col, long_col)
                GROUP BY bool_col
                """))
                .matches("""
                    VALUES (FALSE, BIGINT '1'),
                    (TRUE, BIGINT '9')
                """)
                .isFullyPushedDown();
        // Verify that count(1) is not pushed down when the subquery selects distinct values for a single column
        assertThat(query("""
                SELECT COUNT(1) FROM
                (SELECT bool_col FROM alltypes GROUP BY bool_col)
                """))
                .matches("VALUES (BIGINT '2')")
                .isNotFullyPushedDown(AggregationNode.class);
        // Verify that count(*) is not pushed down when the subquery selects distinct values for a single column
        assertThat(query("""
                SELECT COUNT(*) FROM
                (SELECT bool_col FROM alltypes GROUP BY bool_col)
                """))
                .matches("VALUES (BIGINT '2')")
                .isNotFullyPushedDown(AggregationNode.class);
        // Verify that other aggregation types are not pushed down when the subquery selects distinct values for a single column
        assertThat(query("""
                SELECT SUM(long_col) FROM
                (SELECT long_col FROM alltypes GROUP BY long_col)
                """))
                .matches("VALUES (BIGINT '-28327352787')")
                .isNotFullyPushedDown(AggregationNode.class);
        assertThat(query("""
                SELECT bool_col, SUM(long_col) FROM
                (SELECT bool_col, long_col FROM alltypes GROUP BY bool_col, long_col)
                GROUP BY bool_col
                """))
                .matches("VALUES (TRUE, BIGINT '-28327352787'), (FALSE, BIGINT '0')")
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, ProjectNode.class);
        assertThat(query("""
                SELECT AVG(long_col) FROM
                (SELECT long_col FROM alltypes GROUP BY long_col)
                """))
                .matches("VALUES (DOUBLE '-2.8327352787E9')")
                .isNotFullyPushedDown(AggregationNode.class);
        assertThat(query("""
                SELECT bool_col, AVG(long_col) FROM
                (SELECT bool_col, long_col FROM alltypes GROUP BY bool_col, long_col)
                GROUP BY bool_col
                """))
                .matches("VALUES (TRUE, DOUBLE '-3.147483643E9'), (FALSE, DOUBLE '0.0')")
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, ProjectNode.class);
        assertThat(query("""
                SELECT MIN(long_col) FROM
                (SELECT long_col FROM alltypes GROUP BY long_col)
                """))
                .matches("VALUES (BIGINT '-3147483647')")
                .isNotFullyPushedDown(AggregationNode.class);
        assertThat(query("""
                SELECT bool_col, MIN(long_col) FROM
                (SELECT bool_col, long_col FROM alltypes GROUP BY bool_col, long_col)
                GROUP BY bool_col
                """))
                .matches("VALUES (TRUE, BIGINT '-3147483647'), (FALSE, BIGINT '0')")
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, ProjectNode.class);
        assertThat(query("""
                SELECT MAX(long_col) FROM
                (SELECT long_col FROM alltypes GROUP BY long_col)
                """))
                .matches("VALUES (BIGINT '0')")
                .isNotFullyPushedDown(AggregationNode.class);
        assertThat(query("""
                SELECT bool_col, MAX(long_col) FROM
                (SELECT bool_col, long_col FROM alltypes GROUP BY bool_col, long_col)
                GROUP BY bool_col
                """))
                .matches("VALUES (TRUE, BIGINT '-3147483639'), (FALSE, BIGINT '0')")
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, ProjectNode.class);
    }

    @Test
    public void testInClause()
    {
        assertThat(query("SELECT string_col, sum(long_col)" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE string_col IN ('string_1200','string_2400','string_3600')" +
                "  GROUP BY string_col"))
                .isFullyPushedDown();

        assertThat(query("SELECT string_col, sum(long_col)" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE string_col NOT IN ('string_1200','string_2400','string_3600')" +
                "  GROUP BY string_col"))
                .isFullyPushedDown();

        assertThat(query("SELECT int_col, sum(long_col)" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE int_col IN (54, 56)" +
                "  GROUP BY int_col"))
                .isFullyPushedDown();

        assertThat(query("SELECT int_col, sum(long_col)" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE int_col NOT IN (54, 56)" +
                "  GROUP BY int_col"))
                .isFullyPushedDown();
    }

    @Test
    public void testVarbinaryFilters()
    {
        assertThat(query("SELECT string_col" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE bytes_col = X''"))
                .matches("VALUES (VARCHAR 'null'), (VARCHAR 'array_null')")
                .isFullyPushedDown();

        assertThat(query("SELECT string_col" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE bytes_col != X''"))
                .matches("VALUES (VARCHAR 'string_0')," +
                        "  (VARCHAR 'string_1200')," +
                        "  (VARCHAR 'string_2400')," +
                        "  (VARCHAR 'string_3600')," +
                        "  (VARCHAR 'string_4800')," +
                        "  (VARCHAR 'string_6000')," +
                        "  (VARCHAR 'string_7200')," +
                        "  (VARCHAR 'string_8400')," +
                        "  (VARCHAR 'string_9600')")
                .isFullyPushedDown();

        assertThat(query("SELECT string_col" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE bytes_col = X'73 74 72 69 6e 67 5f 30'"))
                .matches("VALUES (VARCHAR 'string_0')")
                .isFullyPushedDown();

        assertThat(query("SELECT string_col" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE bytes_col != X'73 74 72 69 6e 67 5f 30'"))
                .matches("VALUES (VARCHAR 'null')," +
                        "  (VARCHAR 'array_null')," +
                        "  (VARCHAR 'string_1200')," +
                        "  (VARCHAR 'string_2400')," +
                        "  (VARCHAR 'string_3600')," +
                        "  (VARCHAR 'string_4800')," +
                        "  (VARCHAR 'string_6000')," +
                        "  (VARCHAR 'string_7200')," +
                        "  (VARCHAR 'string_8400')," +
                        "  (VARCHAR 'string_9600')")
                .isFullyPushedDown();
    }

    @Test
    public void testRealWithInfinity()
    {
        assertThat(query("SELECT element_at(float_array_col, 1)" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE bytes_col = X''"))
                .matches("VALUES  (CAST(-POWER(0, -1) AS REAL))," +
                        "  (CAST(-POWER(0, -1) AS REAL))");

        assertThat(query("SELECT element_at(float_array_col, 1) FROM \"SELECT float_array_col" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE bytes_col = '' \""))
                .matches("VALUES  (CAST(-POWER(0, -1) AS REAL))," +
                        "  (CAST(-POWER(0, -1) AS REAL))");

        assertThat(query("SELECT element_at(float_array_col, 2)" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE string_col = 'string_0'"))
                .matches("VALUES (CAST(POWER(0, -1) AS REAL))");

        assertThat(query("SELECT element_at(float_array_col, 2) FROM \"SELECT float_array_col" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE string_col = 'string_0'\""))
                .matches("VALUES (CAST(POWER(0, -1) AS REAL))");
    }

    @Test
    public void testDoubleWithInfinity()
    {
        assertThat(query("SELECT element_at(double_array_col, 1)" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE bytes_col = X''"))
                .matches("VALUES  (-POWER(0, -1))," +
                        "  (-POWER(0, -1))");

        assertThat(query("SELECT element_at(double_array_col, 1) FROM \"SELECT double_array_col" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE bytes_col = '' \""))
                .matches("VALUES  (-POWER(0, -1))," +
                        "  (-POWER(0, -1))");

        assertThat(query("SELECT element_at(double_array_col, 2)" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE string_col = 'string_0'"))
                .matches("VALUES (POWER(0, -1))");

        assertThat(query("SELECT element_at(double_array_col, 2) FROM \"SELECT double_array_col" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE string_col = 'string_0'\""))
                .matches("VALUES (POWER(0, -1))");
    }

    @Test
    public void testTransformFunctions()
    {
        // Test that time units and formats are correctly uppercased.
        // The dynamic table, i.e. the query between the quotes, will be lowercased since it is passed as a SchemaTableName.
        assertThat(query("SELECT hours_col, hours_col2 FROM \"SELECT timeconvert(created_at_seconds, 'SECONDS', 'HOURS') as hours_col," +
                "  CAST(FLOOR(created_at_seconds / 3600) as long) as hours_col2 from " + DATE_TIME_FIELDS_TABLE + "\""))
                .matches("VALUES (BIGINT '450168', BIGINT '450168')," +
                        "  (BIGINT '450168', BIGINT '450168')," +
                        "  (BIGINT '450168', BIGINT '450168')");
        assertThat(query("SELECT \"datetimeconvert(created_at_seconds,'1:seconds:epoch','1:days:epoch','1:days')\" FROM \"SELECT datetimeconvert(created_at_seconds, '1:SECONDS:EPOCH', '1:DAYS:EPOCH', '1:DAYS')" +
                " FROM " + DATE_TIME_FIELDS_TABLE + "\""))
                .matches("VALUES (BIGINT '18757'), (BIGINT '18757'), (BIGINT '18757')");
        // Multiple forms of datetrunc from 2-5 arguments
        assertThat(query("SELECT \"datetrunc('hour',created_at)\" FROM \"SELECT datetrunc('hour', created_at)" +
                " FROM " + DATE_TIME_FIELDS_TABLE + "\""))
                .matches("VALUES (BIGINT '1620604800000'), (BIGINT '1620604800000'), (BIGINT '1620604800000')");
        assertThat(query("SELECT \"datetrunc('hour',created_at_seconds,'seconds')\" FROM \"SELECT datetrunc('hour', created_at_seconds, 'SECONDS')" +
                " FROM " + DATE_TIME_FIELDS_TABLE + "\""))
                .matches("VALUES (BIGINT '1620604800'), (BIGINT '1620604800'), (BIGINT '1620604800')");
        assertThat(query("SELECT \"datetrunc('hour',created_at_seconds,'seconds','utc')\" FROM \"SELECT datetrunc('hour', created_at_seconds, 'SECONDS', 'UTC')" +
                " FROM " + DATE_TIME_FIELDS_TABLE + "\""))
                .matches("VALUES (BIGINT '1620604800'), (BIGINT '1620604800'), (BIGINT '1620604800')");

        assertThat(query("SELECT \"datetrunc('quarter',created_at_seconds,'seconds','america/los_angeles','hours')\" FROM \"SELECT datetrunc('quarter', created_at_seconds, 'SECONDS', 'America/Los_Angeles', 'HOURS')" +
                " FROM " + DATE_TIME_FIELDS_TABLE + "\""))
                .matches("VALUES (BIGINT '449239'), (BIGINT '449239'), (BIGINT '449239')");
        assertThat(query("SELECT \"arraylength(double_array_col)\" FROM " +
                "\"SELECT arraylength(double_array_col)" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE string_col in ('string_0', 'array_null')\""))
                .matches("VALUES (3), (1)");

        assertThat(query("SELECT \"cast(floor(arrayaverage(long_array_col)),'long')\" FROM " +
                "\"SELECT cast(floor(arrayaverage(long_array_col)) as long)" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE double_array_col is not null and double_col != -17.33\""))
                .matches("VALUES (BIGINT '333333337')," +
                        "  (BIGINT '333333338')," +
                        "  (BIGINT '333333338')," +
                        "  (BIGINT '333333338')," +
                        "  (BIGINT '333333339')," +
                        "  (BIGINT '333333339')," +
                        "  (BIGINT '333333339')," +
                        "  (BIGINT '333333340')");

        assertThat(query("SELECT \"arraymax(long_array_col)\" FROM " +
                "\"SELECT arraymax(long_array_col)" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE string_col is not null and string_col != 'array_null'\""))
                .matches("VALUES (BIGINT '4147483647')," +
                        "  (BIGINT '4147483648')," +
                        "  (BIGINT '4147483649')," +
                        "  (BIGINT '4147483650')," +
                        "  (BIGINT '4147483651')," +
                        "  (BIGINT '4147483652')," +
                        "  (BIGINT '4147483653')," +
                        "  (BIGINT '4147483654')," +
                        "  (BIGINT '4147483655')");

        assertThat(query("SELECT \"arraymin(long_array_col)\" FROM " +
                "\"SELECT arraymin(long_array_col)" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE string_col is not null and string_col != 'array_null'\""))
                .matches("VALUES (BIGINT '-3147483647')," +
                        "  (BIGINT '-3147483646')," +
                        "  (BIGINT '-3147483645')," +
                        "  (BIGINT '-3147483644')," +
                        "  (BIGINT '-3147483643')," +
                        "  (BIGINT '-3147483642')," +
                        "  (BIGINT '-3147483641')," +
                        "  (BIGINT '-3147483640')," +
                        "  (BIGINT '-3147483639')");
    }

    @Test
    public void testPassthroughQueriesWithAliases()
    {
        assertThat(query("SELECT hours_col, hours_col2 FROM " +
                "\"SELECT timeconvert(created_at_seconds, 'SECONDS', 'HOURS') AS hours_col," +
                "  CAST(FLOOR(created_at_seconds / 3600) as long) as hours_col2" +
                "  FROM " + DATE_TIME_FIELDS_TABLE + "\""))
                .matches("VALUES (BIGINT '450168', BIGINT '450168'), (BIGINT '450168', BIGINT '450168'), (BIGINT '450168', BIGINT '450168')");

        // Test without aliases to verify fieldName is correctly handled
        assertThat(query("SELECT \"timeconvert(created_at_seconds,'seconds','hours')\"," +
                " \"cast(floor(divide(created_at_seconds,'3600')),'long')\" FROM " +
                "\"SELECT timeconvert(created_at_seconds, 'SECONDS', 'HOURS')," +
                "  CAST(FLOOR(created_at_seconds / 3600) as long)" +
                "  FROM " + DATE_TIME_FIELDS_TABLE + "\""))
                .matches("VALUES (BIGINT '450168', BIGINT '450168'), (BIGINT '450168', BIGINT '450168'), (BIGINT '450168', BIGINT '450168')");

        assertThat(query("SELECT int_col2, long_col2 FROM " +
                "\"SELECT int_col AS int_col2, long_col AS long_col2" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE string_col IS NOT null AND string_col != 'array_null'\""))
                .matches("VALUES (54, BIGINT '-3147483647')," +
                        "  (54, BIGINT '-3147483646')," +
                        "  (54, BIGINT '-3147483645')," +
                        "  (55, BIGINT '-3147483644')," +
                        "  (55, BIGINT '-3147483643')," +
                        "  (55, BIGINT '-3147483642')," +
                        "  (56, BIGINT '-3147483641')," +
                        "  (56, BIGINT '-3147483640')," +
                        "  (56, BIGINT '-3147483639')");

        assertThat(query("SELECT int_col2, long_col2 FROM " +
                "\"SELECT int_col AS int_col2, long_col AS long_col2 " +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE string_col IS NOT null AND string_col != 'array_null'\""))
                .matches("VALUES (54, BIGINT '-3147483647')," +
                        "  (54, BIGINT '-3147483646')," +
                        "  (54, BIGINT '-3147483645')," +
                        "  (55, BIGINT '-3147483644')," +
                        "  (55, BIGINT '-3147483643')," +
                        "  (55, BIGINT '-3147483642')," +
                        "  (56, BIGINT '-3147483641')," +
                        "  (56, BIGINT '-3147483640')," +
                        "  (56, BIGINT '-3147483639')");

        assertQuerySucceeds("SELECT int_col FROM " +
                "\"SELECT floor(int_col / 3) AS int_col" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE string_col IS NOT null AND string_col != 'array_null'\"");
    }

    @Test
    public void testPassthroughQueriesWithPushdowns()
    {
        assertThat(query("SELECT DISTINCT \"timeconvert(created_at_seconds,'seconds','hours')\"," +
                "  \"cast(floor(divide(created_at_seconds,'3600')),'long')\" FROM " +
                "\"SELECT timeconvert(created_at_seconds, 'SECONDS', 'HOURS')," +
                "  CAST(FLOOR(created_at_seconds / 3600) AS long)" +
                "  FROM " + DATE_TIME_FIELDS_TABLE + "\""))
                .matches("VALUES (BIGINT '450168', BIGINT '450168')");

        assertThat(query("SELECT DISTINCT \"timeconvert(created_at_seconds,'seconds','milliseconds')\"," +
                "  \"cast(floor(divide(created_at_seco" +
                "nds,'3600')),'long')\" FROM " +
                "\"SELECT timeconvert(created_at_seconds, 'SECONDS', 'MILLISECONDS')," +
                "  CAST(FLOOR(created_at_seconds / 3600) as long)" +
                "  FROM " + DATE_TIME_FIELDS_TABLE + "\""))
                .matches("VALUES (BIGINT '1620604802000', BIGINT '450168')," +
                        "  (BIGINT '1620604801000', BIGINT '450168')," +
                        "  (BIGINT '1620604800000', BIGINT '450168')");

        assertThat(query("SELECT int_col, sum(long_col) FROM " +
                "\"SELECT int_col, long_col" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE string_col IS NOT null AND string_col != 'array_null'\"" +
                "  GROUP BY int_col"))
                .isFullyPushedDown();

        assertThat(query("SELECT DISTINCT int_col, long_col FROM " +
                "\"SELECT int_col, long_col FROM " + ALL_TYPES_TABLE +
                "  WHERE string_col IS NOT null AND string_col != 'array_null'\""))
                .isFullyPushedDown();

        assertThat(query("SELECT int_col2, long_col2, count(*) FROM " +
                "\"SELECT int_col AS int_col2, long_col AS long_col2" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE string_col IS NOT null AND string_col != 'array_null'\"" +
                "  GROUP BY int_col2, long_col2"))
                .isFullyPushedDown();

        assertQuerySucceeds("SELECT DISTINCT int_col2, long_col2 FROM " +
                "\"SELECT int_col AS int_col2, long_col AS long_col2" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE string_col IS NOT null AND string_col != 'array_null'\"");
        assertThat(query("SELECT int_col2, count(*) FROM " +
                "\"SELECT int_col AS int_col2, long_col AS long_col2" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE string_col IS NOT null AND string_col != 'array_null'\"" +
                "  GROUP BY int_col2"))
                .isFullyPushedDown();
    }

    @Test
    public void testColumnNamesWithDoubleQuotes()
    {
        assertThat(query("select \"double\"\"\"\"qu\"\"ot\"\"ed\"\"\" from quotes_in_column_name"))
                .matches("VALUES (VARCHAR 'foo'), (VARCHAR 'bar')")
                .isFullyPushedDown();

        assertThat(query("select \"qu\"\"ot\"\"ed\" from quotes_in_column_name"))
                .matches("VALUES (VARCHAR 'FOO'), (VARCHAR 'BAR')")
                .isFullyPushedDown();

        assertThat(query("select non_quoted from \"select \"\"qu\"\"\"\"ot\"\"\"\"ed\"\" as non_quoted from quotes_in_column_name\""))
                .matches("VALUES (VARCHAR 'FOO'), (VARCHAR 'BAR')")
                .isFullyPushedDown();

        assertThat(query("select \"qu\"\"ot\"\"ed\" from \"select non_quoted as \"\"qu\"\"\"\"ot\"\"\"\"ed\"\" from quotes_in_column_name\""))
                .matches("VALUES (VARCHAR 'Foo'), (VARCHAR 'Bar')")
                .isFullyPushedDown();

        assertThat(query("select \"double\"\"\"\"qu\"\"ot\"\"ed\"\"\" from \"select \"\"double\"\"\"\"\"\"\"\"qu\"\"\"\"ot\"\"\"\"ed\"\"\"\"\"\" from quotes_in_column_name\""))
                .matches("VALUES (VARCHAR 'foo'), (VARCHAR 'bar')")
                .isFullyPushedDown();

        assertThat(query("select \"qu\"\"oted\" from \"select \"\"double\"\"\"\"\"\"\"\"qu\"\"\"\"ot\"\"\"\"ed\"\"\"\"\"\" as \"\"qu\"\"\"\"oted\"\" from quotes_in_column_name\""))
                .matches("VALUES (VARCHAR 'foo'), (VARCHAR 'bar')")
                .isFullyPushedDown();

        assertThat(query("select \"date\" from \"select \"\"qu\"\"\"\"ot\"\"\"\"ed\"\" as \"\"date\"\" from quotes_in_column_name\""))
                .matches("VALUES (VARCHAR 'FOO'), (VARCHAR 'BAR')")
                .isFullyPushedDown();

        assertThat(query("select \"date\" from \"select non_quoted as \"\"date\"\" from quotes_in_column_name\""))
                .matches("VALUES (VARCHAR 'Foo'), (VARCHAR 'Bar')")
                .isFullyPushedDown();

        /// Test aggregations with double quoted columns
        assertThat(query("select non_quoted, COUNT(DISTINCT \"date\") from \"select non_quoted, non_quoted as \"\"date\"\" from quotes_in_column_name\" GROUP BY non_quoted"))
                .isFullyPushedDown();

        assertThat(query("select non_quoted, COUNT(DISTINCT \"double\"\"\"\"qu\"\"ot\"\"ed\"\"\") from \"select non_quoted, \"\"double\"\"\"\"\"\"\"\"qu\"\"\"\"ot\"\"\"\"ed\"\"\"\"\"\" from quotes_in_column_name\" GROUP BY non_quoted"))
                .isFullyPushedDown();

        assertThat(query("select non_quoted, COUNT(DISTINCT  \"qu\"\"ot\"\"ed\") from \"select non_quoted, \"\"qu\"\"\"\"ot\"\"\"\"ed\"\" from quotes_in_column_name\" GROUP BY non_quoted"))
                .isFullyPushedDown();

        assertThat(query("select non_quoted, COUNT(DISTINCT  \"qu\"\"ot\"\"ed\") from \"select non_quoted, non_quoted as \"\"qu\"\"\"\"ot\"\"\"\"ed\"\" from quotes_in_column_name\" GROUP BY non_quoted"))
                .isFullyPushedDown();

        assertThat(query("select \"qu\"\"ot\"\"ed\", COUNT(DISTINCT \"date\") from \"select \"\"qu\"\"\"\"ot\"\"\"\"ed\"\", non_quoted as \"\"date\"\" from quotes_in_column_name\" GROUP BY \"qu\"\"ot\"\"ed\""))
                .isFullyPushedDown();

        assertThat(query("select \"qu\"\"ot\"\"ed\", COUNT(DISTINCT \"double\"\"\"\"qu\"\"ot\"\"ed\"\"\") from \"select \"\"qu\"\"\"\"ot\"\"\"\"ed\"\", \"\"double\"\"\"\"\"\"\"\"qu\"\"\"\"ot\"\"\"\"ed\"\"\"\"\"\" from quotes_in_column_name\" GROUP BY \"qu\"\"ot\"\"ed\""))
                .isFullyPushedDown();

        // Test with grouping column that has double quotes aliased to a name without double quotes
        assertThat(query("select non_quoted, COUNT(DISTINCT  \"qu\"\"ot\"\"ed\") from \"select \"\"double\"\"\"\"\"\"\"\"qu\"\"\"\"ot\"\"\"\"ed\"\"\"\"\"\" as non_quoted, \"\"qu\"\"\"\"ot\"\"\"\"ed\"\" from quotes_in_column_name\" GROUP BY non_quoted"))
                .isFullyPushedDown();

        // Test with grouping column that has no double quotes aliased to a name with double quotes
        assertThat(query("select \"qu\"\"oted\", COUNT(DISTINCT  \"qu\"\"ot\"\"ed\") from \"select non_quoted as \"\"qu\"\"\"\"oted\"\", \"\"qu\"\"\"\"ot\"\"\"\"ed\"\" from quotes_in_column_name\" GROUP BY \"qu\"\"oted\""))
                .isFullyPushedDown();

        assertThat(query("select \"qu\"\"oted\", COUNT(DISTINCT  \"qu\"\"oted\") from \"select \"\"qu\"\"\"\"ot\"\"\"\"ed\"\", non_quoted as \"\"qu\"\"\"\"oted\"\" from quotes_in_column_name\" GROUP BY \"qu\"\"oted\""))
                .isFullyPushedDown();

        /// Test aggregations with double quoted columns and no grouping sets
        assertThat(query("select COUNT(DISTINCT \"date\") from \"select non_quoted as \"\"date\"\" from quotes_in_column_name\""))
                .isFullyPushedDown();

        assertThat(query("select COUNT(DISTINCT \"double\"\"\"\"qu\"\"ot\"\"ed\"\"\") from \"select \"\"double\"\"\"\"\"\"\"\"qu\"\"\"\"ot\"\"\"\"ed\"\"\"\"\"\" from quotes_in_column_name\""))
                .isFullyPushedDown();

        assertThat(query("select COUNT(DISTINCT  \"qu\"\"ot\"\"ed\") from \"select \"\"qu\"\"\"\"ot\"\"\"\"ed\"\" from quotes_in_column_name\""))
                .isFullyPushedDown();

        assertThat(query("select COUNT(DISTINCT  \"qu\"\"ot\"\"ed\") from \"select non_quoted as \"\"qu\"\"\"\"ot\"\"\"\"ed\"\" from quotes_in_column_name\""))
                .isFullyPushedDown();
    }

    @Test
    public void testLimitAndOffsetWithPushedDownAggregates()
    {
        // Aggregation pushdown must be disabled when there is an offset as the results will not be correct
        assertThat(query("SELECT COUNT(*), MAX(long_col)" +
                "  FROM \"SELECT long_col FROM " + ALL_TYPES_TABLE +
                "  WHERE long_col < 0" +
                "  ORDER BY long_col " +
                "  LIMIT 5, 6\""))
                .matches("VALUES (BIGINT '4', BIGINT '-3147483639')")
                .isNotFullyPushedDown(AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class);

        assertThat(query("SELECT long_col, COUNT(*), MAX(long_col)" +
                "  FROM \"SELECT long_col FROM " + ALL_TYPES_TABLE +
                "  WHERE long_col < 0" +
                "  ORDER BY long_col " +
                "  LIMIT 5, 6\" GROUP BY long_col"))
                .matches("VALUES (BIGINT '-3147483642', BIGINT '1', BIGINT '-3147483642')," +
                        "  (BIGINT '-3147483640', BIGINT '1', BIGINT '-3147483640')," +
                        "  (BIGINT '-3147483641', BIGINT '1', BIGINT '-3147483641')," +
                        "  (BIGINT '-3147483639', BIGINT '1', BIGINT '-3147483639')")
                .isNotFullyPushedDown(AggregationNode.class, ExchangeNode.class, ExchangeNode.class, ProjectNode.class, AggregationNode.class);

        assertThat(query("SELECT long_col, string_col, COUNT(*), MAX(long_col)" +
                "  FROM \"SELECT * FROM " + ALL_TYPES_TABLE +
                "  WHERE long_col < 0" +
                "  ORDER BY long_col, string_col" +
                "  LIMIT 5, 6\" GROUP BY long_col, string_col"))
                .matches("VALUES (BIGINT '-3147483641', VARCHAR 'string_7200', BIGINT '1', BIGINT '-3147483641')," +
                        "  (BIGINT '-3147483640', VARCHAR 'string_8400', BIGINT '1', BIGINT '-3147483640')," +
                        "  (BIGINT '-3147483642', VARCHAR 'string_6000', BIGINT '1', BIGINT '-3147483642')," +
                        "  (BIGINT '-3147483639', VARCHAR 'string_9600', BIGINT '1', BIGINT '-3147483639')")
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, ProjectNode.class);

        // Note that the offset is the first parameter
        assertThat(query("SELECT long_col" +
                "  FROM \"SELECT long_col FROM " + ALL_TYPES_TABLE +
                "  WHERE long_col < 0" +
                "  ORDER BY long_col " +
                "  LIMIT 2, 6\""))
                .matches("VALUES (BIGINT '-3147483645')," +
                        "  (BIGINT '-3147483644')," +
                        "  (BIGINT '-3147483643')," +
                        "  (BIGINT '-3147483642')," +
                        "  (BIGINT '-3147483641')," +
                        "  (BIGINT '-3147483640')")
                .isFullyPushedDown();

        // Note that the offset is the first parameter
        assertThat(query("SELECT long_col, string_col" +
                "  FROM \"SELECT long_col, string_col FROM " + ALL_TYPES_TABLE +
                "  WHERE long_col < 0" +
                "  ORDER BY long_col " +
                "  LIMIT 2, 6\""))
                .matches("VALUES (BIGINT '-3147483645', VARCHAR 'string_2400')," +
                        "  (BIGINT '-3147483644', VARCHAR 'string_3600')," +
                        "  (BIGINT '-3147483643', VARCHAR 'string_4800')," +
                        "  (BIGINT '-3147483642', VARCHAR 'string_6000')," +
                        "  (BIGINT '-3147483641', VARCHAR 'string_7200')," +
                        "  (BIGINT '-3147483640', VARCHAR 'string_8400')")
                .isFullyPushedDown();
    }

    @Test
    public void testAggregatePassthroughQueriesWithExpressions()
    {
        assertThat(query("SELECT string_col, sum_metric_col1, count_dup_string_col, ratio_metric_col" +
                "  FROM \"SELECT string_col, SUM(metric_col1) AS sum_metric_col1, COUNT(DISTINCT another_string_col) AS count_dup_string_col," +
                "  (SUM(metric_col1) - SUM(metric_col2)) / SUM(metric_col1) AS ratio_metric_col" +
                "  FROM duplicate_values_in_columns WHERE dim_col = another_dim_col" +
                "  GROUP BY string_col" +
                "  ORDER BY string_col\""))
                .matches("VALUES (VARCHAR 'string1', DOUBLE '1110.0', 2, DOUBLE '-1.0')," +
                        "  (VARCHAR 'string2', DOUBLE '100.0', 1, DOUBLE '-1.0')");

        assertThat(query("SELECT string_col, sum_metric_col1, count_dup_string_col, ratio_metric_col" +
                "  FROM \"SELECT string_col, SUM(metric_col1) AS sum_metric_col1," +
                "  COUNT(DISTINCT another_string_col) AS count_dup_string_col," +
                "  (SUM(metric_col1) - SUM(metric_col2)) / SUM(metric_col1) AS ratio_metric_col" +
                "  FROM duplicate_values_in_columns WHERE dim_col != another_dim_col" +
                "  GROUP BY string_col" +
                "  ORDER BY string_col\""))
                .matches("VALUES (VARCHAR 'string2', DOUBLE '1000.0', 1, DOUBLE '-1.0')");

        assertThat(query("SELECT DISTINCT string_col, another_string_col" +
                "  FROM \"SELECT string_col, another_string_col" +
                "  FROM duplicate_values_in_columns WHERE dim_col = another_dim_col\""))
                .matches("VALUES (VARCHAR 'string1', VARCHAR 'string1')," +
                        "  (VARCHAR 'string1', VARCHAR 'another_string1')," +
                        "  (VARCHAR 'string2', VARCHAR 'another_string2')");

        assertThat(query("SELECT string_col, sum_metric_col1" +
                "  FROM \"SELECT string_col," +
                "  SUM(CASE WHEN dim_col = another_dim_col THEN metric_col1 ELSE 0 END) AS sum_metric_col1" +
                "  FROM duplicate_values_in_columns GROUP BY string_col ORDER BY string_col\""))
                .matches("VALUES (VARCHAR 'string1', DOUBLE '1110.0')," +
                        "  (VARCHAR 'string2', DOUBLE '100.0')");

        assertThat(query("SELECT \"percentile(int_col, 90.0)\"" +
                "  FROM \"SELECT percentile(int_col, 90) FROM " + ALL_TYPES_TABLE + "\""))
                .matches("VALUES (DOUBLE '56.0')");

        assertThat(query("SELECT bool_col, \"percentile(int_col, 90.0)\"" +
                "  FROM \"SELECT bool_col, percentile(int_col, 90) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col\""))
                .matches("VALUES (true, DOUBLE '56.0')," +
                        "  (false, DOUBLE '0.0')");

        assertThat(query("SELECT \"sqrt(percentile(sqrt(int_col),'26.457513110645905'))\"" +
                "  FROM \"SELECT sqrt(percentile(sqrt(int_col), sqrt(700))) FROM " + ALL_TYPES_TABLE + "\""))
                .matches("VALUES (DOUBLE '2.7108060108295344')");

        assertThat(query("SELECT int_col, \"sqrt(percentile(sqrt(int_col),'26.457513110645905'))\"" +
                "  FROM \"SELECT int_col, sqrt(percentile(sqrt(int_col), sqrt(700))) FROM " + ALL_TYPES_TABLE + " GROUP BY int_col\""))
                .matches("VALUES (54, DOUBLE '2.7108060108295344')," +
                        "  (55, DOUBLE '2.7232698153315003')," +
                        "  (56, DOUBLE '2.7355647997347607')," +
                        "  (0, DOUBLE '0.0')");
    }

    @Test
    public void testAggregationPushdownWithArrays()
    {
        assertThat(query("SELECT string_array_col, count(*) FROM " + ALL_TYPES_TABLE + " WHERE int_col = 54 GROUP BY 1"))
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, ProjectNode.class);
        assertThat(query("SELECT int_array_col, string_array_col, count(*) FROM " + ALL_TYPES_TABLE + " WHERE int_col = 54 GROUP BY 1, 2"))
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, ProjectNode.class);
        assertThat(query("SELECT int_array_col, \"count(*)\"" +
                "  FROM \"SELECT int_array_col, COUNT(*) FROM " + ALL_TYPES_TABLE +
                "  WHERE int_col = 54 GROUP BY 1\""))
                .isFullyPushedDown()
                .matches("VALUES (-10001, BIGINT '3')," +
                        "(54, BIGINT '3')," +
                        "(1000, BIGINT '3')");
        assertThat(query("SELECT int_array_col, string_array_col, \"count(*)\"" +
                "  FROM \"SELECT int_array_col, string_array_col, COUNT(*) FROM " + ALL_TYPES_TABLE +
                "  WHERE int_col = 56 AND string_col = 'string_8400' GROUP BY 1, 2\""))
                .isFullyPushedDown()
                .matches("VALUES (-10001, VARCHAR 'string_8400', BIGINT '1')," +
                        "(-10001, VARCHAR 'string2_8402', BIGINT '1')," +
                        "(1000, VARCHAR 'string2_8402', BIGINT '1')," +
                        "(56, VARCHAR 'string2_8402', BIGINT '1')," +
                        "(-10001, VARCHAR 'string1_8401', BIGINT '1')," +
                        "(56, VARCHAR 'string1_8401', BIGINT '1')," +
                        "(1000, VARCHAR 'string_8400', BIGINT '1')," +
                        "(56, VARCHAR 'string_8400', BIGINT '1')," +
                        "(1000, VARCHAR 'string1_8401', BIGINT '1')");
    }

    @Test
    public void testVarbinary()
    {
        String expectedValues = "VALUES (X'')," +
                "  (X'73 74 72 69 6e 67 5f 30')," +
                "  (X'73 74 72 69 6e 67 5f 31 32 30 30')," +
                "  (X'73 74 72 69 6e 67 5f 32 34 30 30')," +
                "  (X'73 74 72 69 6e 67 5f 33 36 30 30')," +
                "  (X'73 74 72 69 6e 67 5f 34 38 30 30')," +
                "  (X'73 74 72 69 6e 67 5f 36 30 30 30')," +
                "  (X'73 74 72 69 6e 67 5f 37 32 30 30')," +
                "  (X'73 74 72 69 6e 67 5f 38 34 30 30')," +
                "  (X'73 74 72 69 6e 67 5f 39 36 30 30')";
        // The filter on string_col is to have a deterministic result set: the default limit for broker queries is 10 rows.
        assertThat(query("SELECT bytes_col FROM alltypes WHERE string_col != 'array_null'"))
                .matches(expectedValues);
        assertThat(query("SELECT bytes_col FROM \"SELECT bytes_col, string_col FROM alltypes\" WHERE string_col != 'array_null'"))
                .matches(expectedValues);
    }

    @Test
    public void testTimeBoundary()
    {
        // Note: This table uses Pinot TIMESTAMP and not LONG as the time column type.
        Instant startInstant = initialUpdatedAt.truncatedTo(DAYS);
        String expectedValues = "VALUES " +
                "(VARCHAR 'string_8', BIGINT '8', TIMESTAMP '" + MILLIS_FORMATTER.format(startInstant.minus(1, DAYS).plusMillis(1000)) + "')," +
                "(VARCHAR 'string_9', BIGINT '9', TIMESTAMP '" + MILLIS_FORMATTER.format(startInstant.minus(1, DAYS).plusMillis(2000)) + "')," +
                "(VARCHAR 'string_10', BIGINT '10', TIMESTAMP '" + MILLIS_FORMATTER.format(startInstant.minus(1, DAYS).plusMillis(3000)) + "')," +
                "(VARCHAR 'string_11', BIGINT '11', TIMESTAMP '" + MILLIS_FORMATTER.format(startInstant.minus(1, DAYS).plusMillis(4000)) + "')";
        assertThat(query("SELECT stringcol, longcol, updatedat FROM " + HYBRID_TABLE_NAME))
                .matches(expectedValues);
        // Verify that this matches the time boundary behavior on the broker
        assertThat(query("SELECT stringcol, longcol, updatedat FROM \"SELECT stringcol, longcol, updatedat FROM " + HYBRID_TABLE_NAME + "\""))
                .matches(expectedValues);
    }

    @Test
    public void testTimestamp()
    {
        assertThat(query("SELECT ts FROM " + ALL_TYPES_TABLE + " ORDER BY ts LIMIT 1")).matches("VALUES (TIMESTAMP '1970-01-01 00:00:00.000')");
        assertThat(query("SELECT min(ts) FROM " + ALL_TYPES_TABLE)).matches("VALUES (TIMESTAMP '1970-01-01 00:00:00.000')");
        assertThat(query("SELECT max(ts) FROM " + ALL_TYPES_TABLE)).isFullyPushedDown();
        assertThat(query("SELECT ts FROM " + ALL_TYPES_TABLE + " ORDER BY ts DESC LIMIT 1")).matches("SELECT max(ts) FROM " + ALL_TYPES_TABLE);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);
        for (int i = 0, step = 1200; i < MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES - 2; i++) {
            String initialUpdatedAtStr = formatter.format(initialUpdatedAt.plusMillis((long) i * step));
            assertThat(query("SELECT ts FROM " + ALL_TYPES_TABLE + " WHERE ts >= TIMESTAMP '" + initialUpdatedAtStr + "' ORDER BY ts LIMIT 1"))
                    .matches("SELECT ts FROM " + ALL_TYPES_TABLE + " WHERE ts <= TIMESTAMP '" + initialUpdatedAtStr + "' ORDER BY ts DESC LIMIT 1");
            assertThat(query("SELECT ts FROM " + ALL_TYPES_TABLE + " WHERE ts = TIMESTAMP '" + initialUpdatedAtStr + "' LIMIT 1"))
                    .matches("VALUES (TIMESTAMP '" + initialUpdatedAtStr + "')");
        }
        assertThat(query("SELECT timestamp_col FROM " + ALL_TYPES_TABLE + " WHERE timestamp_col < TIMESTAMP '1971-01-01 00:00:00.000'")).isFullyPushedDown();
        assertThat(query("SELECT timestamp_col FROM " + ALL_TYPES_TABLE + " WHERE timestamp_col < TIMESTAMP '1970-01-01 00:00:00.000'")).isFullyPushedDown();
    }

    @Test
    public void testJson()
    {
        assertThat(query("SELECT json_col FROM " + JSON_TYPE_TABLE))
                .matches("VALUES (JSON '{\"id\":0,\"name\":\"user_0\"}')," +
                        "  (JSON '{\"id\":1,\"name\":\"user_1\"}')," +
                        "  (JSON '{\"id\":2,\"name\":\"user_2\"}')");
        assertThat(query("SELECT json_col" +
                "  FROM \"SELECT json_col FROM " + JSON_TYPE_TABLE + "\""))
                .matches("VALUES (JSON '{\"id\":0,\"name\":\"user_0\"}')," +
                        "  (JSON '{\"id\":1,\"name\":\"user_1\"}')," +
                        "  (JSON '{\"id\":2,\"name\":\"user_2\"}')");
        assertThat(query("SELECT name FROM \"SELECT json_extract_scalar(json_col, '$.name', 'STRING', '0') AS name" +
                "  FROM json_type_table WHERE json_extract_scalar(json_col, '$.id', 'INT', '0') = '1'\""))
                .matches("VALUES (VARCHAR 'user_1')");
        assertThat(query("SELECT JSON_EXTRACT_SCALAR(json_col, '$.name') FROM " + JSON_TYPE_TABLE +
                "  WHERE JSON_EXTRACT_SCALAR(json_col, '$.id') = '1'"))
                .matches("VALUES (VARCHAR 'user_1')");
        assertThat(query("SELECT string_col FROM " + JSON_TYPE_TABLE + " WHERE json_col = JSON '{\"id\":0,\"name\":\"user_0\"}'"))
                .matches("VALUES VARCHAR 'string_0'");
    }

    @Test
    public void testHavingClause()
    {
        assertThat(query("SELECT city, \"sum(long_number)\" FROM \"SELECT city, SUM(long_number)" +
                "  FROM my_table" +
                "  GROUP BY city" +
                "  HAVING SUM(long_number) > 10000\""))
                .matches("VALUES (VARCHAR 'Los Angeles', DOUBLE '50000.0')," +
                        "  (VARCHAR 'New York', DOUBLE '20000.0')")
                .isFullyPushedDown();
        assertThat(query("SELECT city, \"sum(long_number)\" FROM \"SELECT city, SUM(long_number) FROM my_table" +
                "  GROUP BY city HAVING SUM(long_number) > 14\"" +
                "  WHERE city != 'New York'"))
                .matches("VALUES (VARCHAR 'Los Angeles', DOUBLE '50000.0')")
                .isFullyPushedDown();
        assertThat(query("SELECT city, SUM(long_number)" +
                "  FROM my_table" +
                "  GROUP BY city" +
                "  HAVING SUM(long_number) > 10000"))
                .matches("VALUES (VARCHAR 'Los Angeles', BIGINT '50000')," +
                        "  (VARCHAR 'New York', BIGINT '20000')")
                .isFullyPushedDown();
        assertThat(query("SELECT city, SUM(long_number) FROM my_table" +
                "  WHERE city != 'New York'" +
                "  GROUP BY city HAVING SUM(long_number) > 10000"))
                .matches("VALUES (VARCHAR 'Los Angeles', BIGINT '50000')")
                .isFullyPushedDown();
    }
}
