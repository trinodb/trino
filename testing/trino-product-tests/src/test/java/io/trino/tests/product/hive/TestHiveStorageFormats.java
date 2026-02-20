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
package io.trino.tests.product.hive;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.units.DataSize;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.testing.containers.HdfsClient;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.apache.parquet.column.ParquetProperties;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Maps.immutableEntry;
import static io.trino.plugin.hive.HiveTimestampPrecision.MICROSECONDS;
import static io.trino.plugin.hive.HiveTimestampPrecision.MILLISECONDS;
import static io.trino.plugin.hive.HiveTimestampPrecision.NANOSECONDS;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_ISSUES;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.Comparator.comparingInt;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * JUnit 5 port of TestHiveStorageFormats.
 * <p>
 * Tests various Hive storage formats (ORC, Parquet, Avro, RCFile, SequenceFile, etc.) in Trino.
 */
@ProductTest
@RequiresEnvironment(HiveStorageFormatsEnvironment.class)
@TestGroup.StorageFormats
@TestGroup.StorageFormatsDetailed
@TestGroup.HmsOnly
class TestHiveStorageFormats
{
    private static final String TPCH_SCHEMA = "tiny";

    private static final List<TimestampAndPrecision> TIMESTAMPS_FROM_HIVE = List.of(
            // write precision is not relevant here, as Hive always uses nanos
            timestampAndPrecision(
                    "1967-01-02 12:01:00.111", // millis, no rounding
                    NANOSECONDS,
                    "1967-01-02 12:01:00.111",
                    "1967-01-02 12:01:00.111000",
                    "1967-01-02 12:01:00.111000000"),
            timestampAndPrecision(
                    "1967-01-02 12:01:00.1114", // hundreds of micros, rounds down in millis, (pre-epoch)
                    NANOSECONDS,
                    "1967-01-02 12:01:00.111",
                    "1967-01-02 12:01:00.111400",
                    "1967-01-02 12:01:00.111400000"),
            timestampAndPrecision(
                    "1967-01-02 12:01:00.1115", // hundreds of micros, rounds up in millis (smallest), pre-epoch
                    NANOSECONDS,
                    "1967-01-02 12:01:00.112",
                    "1967-01-02 12:01:00.111500",
                    "1967-01-02 12:01:00.111500000"),
            timestampAndPrecision(
                    "1967-01-02 12:01:00.111499", // micros, rounds down (largest), pre-epoch
                    NANOSECONDS,
                    "1967-01-02 12:01:00.111",
                    "1967-01-02 12:01:00.111499",
                    "1967-01-02 12:01:00.111499000"),
            timestampAndPrecision(
                    "1967-01-02 12:01:00.1113334", // hundreds of nanos, rounds down
                    NANOSECONDS,
                    "1967-01-02 12:01:00.111",
                    "1967-01-02 12:01:00.111333",
                    "1967-01-02 12:01:00.111333400"),
            timestampAndPrecision(
                    "1967-01-02 23:59:59.999999999", // nanos, rounds up to next day
                    NANOSECONDS,
                    "1967-01-03 00:00:00.000",
                    "1967-01-03 00:00:00.000000",
                    "1967-01-02 23:59:59.999999999"),

            timestampAndPrecision(
                    "1967-01-02 12:01:00.1110019", // hundreds of nanos, rounds down in millis and up in micros, pre-epoch
                    NANOSECONDS,
                    "1967-01-02 12:01:00.111",
                    "1967-01-02 12:01:00.111002",
                    "1967-01-02 12:01:00.111001900"),
            timestampAndPrecision(
                    "1967-01-02 12:01:00.111901001", // nanos, rounds up in millis and down in micros, pre-epoch
                    NANOSECONDS,
                    "1967-01-02 12:01:00.112",
                    "1967-01-02 12:01:00.111901",
                    "1967-01-02 12:01:00.111901001"),
            timestampAndPrecision(
                    "1967-12-31 23:59:59.999999499", // nanos, rounds micros down (largest), rounds millis up to next year, pre-epoch
                    NANOSECONDS,
                    "1968-01-01 00:00:00.000",
                    "1967-12-31 23:59:59.999999",
                    "1967-12-31 23:59:59.999999499"),

            timestampAndPrecision(
                    "2027-01-02 12:01:00.1110019", // hundreds of nanos, rounds down in millis and up in micros, post-epoch
                    NANOSECONDS,
                    "2027-01-02 12:01:00.111",
                    "2027-01-02 12:01:00.111002",
                    "2027-01-02 12:01:00.111001900"),
            timestampAndPrecision(
                    "2027-01-02 12:01:00.111901001", // nanos, rounds up in millis and down in micros, post-epoch
                    NANOSECONDS,
                    "2027-01-02 12:01:00.112",
                    "2027-01-02 12:01:00.111901",
                    "2027-01-02 12:01:00.111901001"),
            timestampAndPrecision(
                    "2027-12-31 23:59:59.999999499", // nanos, rounds micros down (largest), rounds millis up to next year, post-epoch
                    NANOSECONDS,
                    "2028-01-01 00:00:00.000",
                    "2027-12-31 23:59:59.999999",
                    "2027-12-31 23:59:59.999999499"));

    // These check that values are correctly rounded on insertion
    private static final List<TimestampAndPrecision> TIMESTAMPS_FROM_TRINO = List.of(
            timestampAndPrecision(
                    "2020-01-02 12:01:00.999", // millis as millis (no rounding)
                    MILLISECONDS,
                    "2020-01-02 12:01:00.999",
                    "2020-01-02 12:01:00.999000",
                    "2020-01-02 12:01:00.999000000"),
            timestampAndPrecision(
                    "2020-01-02 12:01:00.111499999", // nanos as millis rounds down (largest)
                    MILLISECONDS,
                    "2020-01-02 12:01:00.111",
                    "2020-01-02 12:01:00.111000",
                    "2020-01-02 12:01:00.111000000"),
            timestampAndPrecision(
                    "2020-01-02 12:01:00.1115", // micros as millis rounds up (smallest)
                    MILLISECONDS,
                    "2020-01-02 12:01:00.112",
                    "2020-01-02 12:01:00.112000",
                    "2020-01-02 12:01:00.112000000"),
            timestampAndPrecision(
                    "2020-01-02 12:01:00.111333", // micros as micros (no rounding)
                    MICROSECONDS,
                    "2020-01-02 12:01:00.111",
                    "2020-01-02 12:01:00.111333",
                    "2020-01-02 12:01:00.111333000"),
            timestampAndPrecision(
                    "2020-01-02 12:01:00.1113334", // nanos as micros rounds down
                    MICROSECONDS,
                    "2020-01-02 12:01:00.111",
                    "2020-01-02 12:01:00.111333",
                    "2020-01-02 12:01:00.111333000"),
            timestampAndPrecision(
                    "2020-01-02 12:01:00.111333777", // nanos as micros rounds up
                    MICROSECONDS,
                    "2020-01-02 12:01:00.111",
                    "2020-01-02 12:01:00.111334",
                    "2020-01-02 12:01:00.111334000"),
            timestampAndPrecision(
                    "2020-01-02 12:01:00.111333444", // nanos as nanos (no rounding)
                    NANOSECONDS,
                    "2020-01-02 12:01:00.111",
                    "2020-01-02 12:01:00.111333",
                    "2020-01-02 12:01:00.111333444"),
            timestampAndPrecision(
                    "2020-01-02 23:59:59.999911333", // nanos as millis rounds up to next day
                    MILLISECONDS,
                    "2020-01-03 00:00:00.000",
                    "2020-01-03 00:00:00.000000",
                    "2020-01-03 00:00:00.000000000"),
            timestampAndPrecision(
                    "2020-12-31 23:59:59.99999", // micros as millis rounds up to next year
                    MILLISECONDS,
                    "2021-01-01 00:00:00.000",
                    "2021-01-01 00:00:00.000000",
                    "2021-01-01 00:00:00.000000000"));

    // ---- Data Providers ----

    static Stream<String> storageFormats()
    {
        return storageFormatsWithConfiguration()
                .map(StorageFormat::getName)
                .distinct();
    }

    static Stream<StorageFormat> storageFormatsWithConfiguration()
    {
        return Stream.of(
                storageFormat("ORC", ImmutableMap.of("hive.orc_optimized_writer_validate", "true")),
                storageFormat("PARQUET", ImmutableMap.of("hive.parquet_optimized_writer_validation_percentage", "100")),
                storageFormat("RCBINARY", ImmutableMap.of("hive.rcfile_optimized_writer_validate", "true")),
                storageFormat("RCTEXT", ImmutableMap.of("hive.rcfile_optimized_writer_validate", "true")),
                storageFormat("SEQUENCEFILE"),
                storageFormat("TEXTFILE"),
                storageFormat("TEXTFILE", ImmutableMap.of(), ImmutableMap.of("textfile_field_separator", "F", "textfile_field_separator_escape", "E")),
                storageFormat("AVRO"));
    }

    static Stream<StorageFormat> storageFormatsWithNullFormat()
    {
        return Stream.of(
                storageFormat("TEXTFILE"),
                storageFormat("RCTEXT"),
                storageFormat("SEQUENCEFILE"));
    }

    static Stream<StorageFormat> storageFormatsWithZeroByteFile()
    {
        return Stream.of(
                storageFormat("ORC"),
                storageFormat("PARQUET"),
                storageFormat("RCBINARY"),
                storageFormat("RCTEXT"),
                storageFormat("SEQUENCEFILE"),
                storageFormat("TEXTFILE"),
                storageFormat("AVRO"),
                storageFormat("CSV"));
    }

    static Stream<StorageFormat> storageFormatsWithNanosecondPrecision()
    {
        return storageFormatsWithConfiguration()
                // nanoseconds are not supported with Avro
                .filter(format -> !"AVRO".equals(format.getName()));
    }

    static Stream<Arguments> storageFormatsWithNanosecondPrecisionAsArguments()
    {
        return storageFormatsWithNanosecondPrecision().map(Arguments::of);
    }

    // ---- Tests ----

    @Test
    void verifyDataProviderCompleteness(HiveStorageFormatsEnvironment env)
    {
        String formatsDescription = (String) env.executeTrino("SELECT description FROM system.metadata.table_properties WHERE catalog_name = CURRENT_CATALOG AND property_name = 'format'").getOnlyValue();
        Pattern pattern = Pattern.compile("Hive storage format for the table. Possible values: \\[([A-Z]+(, [A-z]+)+)]");
        assertThat(formatsDescription).matches(pattern);
        Matcher matcher = pattern.matcher(formatsDescription);
        verify(matcher.matches());

        // HiveStorageFormat.values
        List<String> allFormats = Splitter.on(",").trimResults().splitToList(matcher.group(1));

        Set<String> allFormatsToTest = allFormats.stream()
                // Hive CSV storage format only supports VARCHAR, so needs to be excluded from any generic tests
                .filter(format -> !"CSV".equals(format))
                // REGEX is read-only
                .filter(format -> !"REGEX".equals(format))
                // ESRI is read-only
                .filter(format -> !"ESRI".equals(format))
                // SEQUENCEFILE_PROTOBUF is read-only
                .filter(format -> !"SEQUENCEFILE_PROTOBUF".equals(format))
                // TODO when using JSON serde Hive fails with ClassNotFoundException: org.apache.hive.hcatalog.data.JsonSerDe
                .filter(format -> !"JSON".equals(format))
                // OPENX is not supported in Hive by default
                .filter(format -> !"OPENX_JSON".equals(format))
                .collect(toImmutableSet());

        assertThat(storageFormats().collect(toImmutableSet()))
                .isEqualTo(allFormatsToTest);
    }

    @ParameterizedTest
    @MethodSource("storageFormatsWithConfiguration")
    void testInsertIntoTable(StorageFormat storageFormat, HiveStorageFormatsEnvironment env)
    {
        String tableName = "storage_formats_test_insert_into_" + storageFormat.getName().toLowerCase(ENGLISH);

        env.executeTrinoUpdate(format("DROP TABLE IF EXISTS %s", tableName));

        String createTable = format(
                "CREATE TABLE %s(" +
                        "   orderkey      BIGINT," +
                        "   partkey       BIGINT," +
                        "   suppkey       BIGINT," +
                        "   linenumber    INTEGER," +
                        "   quantity      DOUBLE," +
                        "   extendedprice DOUBLE," +
                        "   discount      DOUBLE," +
                        "   tax           DOUBLE," +
                        "   linestatus    VARCHAR," +
                        "   shipinstruct  VARCHAR," +
                        "   shipmode      VARCHAR," +
                        "   comment       VARCHAR," +
                        "   returnflag    VARCHAR" +
                        ") WITH (%s)",
                tableName,
                storageFormat.getStoragePropertiesAsSql());
        env.executeTrinoUpdate(createTable);

        // Use session to set admin role and session properties before inserting
        env.executeTrinoInSession(session -> {
            setAdminRole(session);
            setSessionProperties(session, storageFormat);
            session.executeUpdate(format("INSERT INTO %s " +
                    "SELECT " +
                    "orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, " +
                    "linestatus, shipinstruct, shipmode, comment, returnflag " +
                    "FROM tpch.%s.lineitem", tableName, TPCH_SCHEMA));
        });

        assertResultEqualForLineitemTable(env,
                "select sum(tax), sum(discount), sum(linenumber) from %s", tableName);

        env.executeTrinoUpdate(format("DROP TABLE %s", tableName));
    }

    @ParameterizedTest
    @MethodSource("storageFormatsWithConfiguration")
    void testCreateTableAs(StorageFormat storageFormat, HiveStorageFormatsEnvironment env)
    {
        String tableName = "storage_formats_test_create_table_as_select_" + storageFormat.getName().toLowerCase(ENGLISH);

        env.executeTrinoUpdate(format("DROP TABLE IF EXISTS %s", tableName));

        // Use session to set admin role and session properties before creating table
        env.executeTrinoInSession(session -> {
            setAdminRole(session);
            setSessionProperties(session, storageFormat);
            session.executeUpdate(format(
                    "CREATE TABLE %s WITH (%s) AS " +
                            "SELECT " +
                            "partkey, suppkey, extendedprice " +
                            "FROM tpch.%s.lineitem",
                    tableName,
                    storageFormat.getStoragePropertiesAsSql(),
                    TPCH_SCHEMA));
        });

        assertResultEqualForLineitemTable(env,
                "select sum(extendedprice), sum(suppkey), count(partkey) from %s", tableName);

        env.executeTrinoUpdate(format("DROP TABLE %s", tableName));
    }

    @ParameterizedTest
    @MethodSource("storageFormatsWithConfiguration")
    void testInsertIntoPartitionedTable(StorageFormat storageFormat, HiveStorageFormatsEnvironment env)
    {
        String tableName = "storage_formats_test_insert_into_partitioned_" + storageFormat.getName().toLowerCase(ENGLISH);

        env.executeTrinoUpdate(format("DROP TABLE IF EXISTS %s", tableName));

        String createTable = format(
                "CREATE TABLE %s(" +
                        "   orderkey      BIGINT," +
                        "   partkey       BIGINT," +
                        "   suppkey       BIGINT," +
                        "   linenumber    INTEGER," +
                        "   quantity      DOUBLE," +
                        "   extendedprice DOUBLE," +
                        "   discount      DOUBLE," +
                        "   tax           DOUBLE," +
                        "   linestatus    VARCHAR," +
                        "   shipinstruct  VARCHAR," +
                        "   shipmode      VARCHAR," +
                        "   comment       VARCHAR," +
                        "   returnflag    VARCHAR" +
                        ") WITH (format='%s', partitioned_by = ARRAY['returnflag'])",
                tableName,
                storageFormat.getName());
        env.executeTrinoUpdate(createTable);

        // Use session to set admin role and session properties before inserting
        env.executeTrinoInSession(session -> {
            setAdminRole(session);
            setSessionProperties(session, storageFormat);
            session.executeUpdate(format("INSERT INTO %s " +
                    "SELECT " +
                    "orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, " +
                    "linestatus, shipinstruct, shipmode, comment, returnflag " +
                    "FROM tpch.%s.lineitem", tableName, TPCH_SCHEMA));
        });

        assertResultEqualForLineitemTable(env,
                "select sum(tax), sum(discount), sum(length(returnflag)) from %s", tableName);

        env.executeTrinoUpdate(format("DROP TABLE %s", tableName));
    }

    @ParameterizedTest
    @MethodSource("storageFormatsWithNullFormat")
    void testInsertAndSelectWithNullFormat(StorageFormat storageFormat, HiveStorageFormatsEnvironment env)
            throws SQLException
    {
        String nullFormat = "null_value";
        String tableName = format(
                "test_storage_format_%s_insert_and_select_with_null_format",
                storageFormat.getName());
        env.executeTrinoUpdate(format(
                "CREATE TABLE %s (value VARCHAR) " +
                        "WITH (format = '%s', null_format = '%s')",
                tableName,
                storageFormat.getName(),
                nullFormat));

        // \N is the default null format
        String[] values = {nullFormat, null, "non-null", "", "\\N"};
        Row[] storedValues = Arrays.stream(values).map(Row::row).toArray(Row[]::new);
        storedValues[0] = row((Object) null); // if you put in the null format, it saves as null

        // Insert with prepared statement
        try (Connection conn = env.createTrinoConnection();
                PreparedStatement stmt = conn.prepareStatement(format("INSERT INTO %s VALUES (?)", tableName))) {
            for (String value : values) {
                stmt.setString(1, value);
                stmt.executeUpdate();
            }
        }

        assertThat(env.executeTrino(format("SELECT * FROM %s", tableName))).containsOnly(storedValues);

        env.executeTrinoUpdate(format("DROP TABLE %s", tableName));
    }

    @ParameterizedTest
    @MethodSource("storageFormatsWithZeroByteFile")
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSelectFromZeroByteFile(StorageFormat storageFormat, HiveStorageFormatsEnvironment env)
    {
        String tableName = format(
                "test_storage_format_%s_zero_byte_file",
                storageFormat.getName());
        HdfsClient hdfsClient = env.createHdfsClient();
        hdfsClient.saveFile(format("%s/%s/zero_byte", env.getWarehouseDirectory(), tableName), "");
        env.executeTrinoUpdate(format(
                "CREATE TABLE %s" +
                        " (name varchar) " +
                        "WITH ( " +
                        "   format = '%s'" +
                        ")",
                tableName,
                storageFormat.getName()));
        assertThat(env.executeTrino("SELECT * FROM " + tableName)).hasNoRows();
        assertThat(env.executeHive("SELECT * FROM " + tableName)).hasNoRows();
        env.executeTrinoUpdate("DROP TABLE " + tableName);
    }

    @ParameterizedTest
    @MethodSource("storageFormatsWithNullFormat")
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSelectWithNullFormat(StorageFormat storageFormat, HiveStorageFormatsEnvironment env)
    {
        String nullFormat = "null_value";
        String tableName = format(
                "test_storage_format_%s_select_with_null_format",
                storageFormat.getName());
        env.executeTrinoUpdate(format(
                "CREATE TABLE %s (value VARCHAR) " +
                        "WITH (format = '%s', null_format = '%s')",
                tableName,
                storageFormat.getName(),
                nullFormat));

        // Manually format data for insertion b/c Hive's PreparedStatement can't handle nulls
        env.executeHiveUpdate(format("INSERT INTO %s VALUES ('non-null'), (NULL), ('%s')",
                tableName, nullFormat));

        assertThat(env.executeTrino(format("SELECT * FROM %s", tableName)))
                .containsOnly(row("non-null"), row((Object) null), row((Object) null));

        env.executeHiveUpdate(format("DROP TABLE %s", tableName));
    }

    @ParameterizedTest
    @MethodSource("storageFormatsWithConfiguration")
    void testCreatePartitionedTableAs(StorageFormat storageFormat, HiveStorageFormatsEnvironment env)
    {
        String tableName = "storage_formats_test_create_table_as_select_partitioned_" + storageFormat.getName().toLowerCase(ENGLISH);

        env.executeTrinoUpdate(format("DROP TABLE IF EXISTS %s", tableName));

        // Use session to set admin role and session properties before creating table
        env.executeTrinoInSession(session -> {
            setAdminRole(session);
            setSessionProperties(session, storageFormat);
            session.executeUpdate(format(
                    "CREATE TABLE %s WITH (%s, partitioned_by = ARRAY['returnflag']) AS " +
                            "SELECT " +
                            "tax, discount, returnflag " +
                            "FROM tpch.%s.lineitem",
                    tableName,
                    storageFormat.getStoragePropertiesAsSql(),
                    TPCH_SCHEMA));
        });

        assertResultEqualForLineitemTable(env,
                "select sum(tax), sum(discount), sum(length(returnflag)) from %s", tableName);

        env.executeTrinoUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    void testOrcTableCreatedInTrino(HiveStorageFormatsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE TABLE orc_table_created_in_trino WITH (format='ORC') AS SELECT 42 a");
        assertThat(env.executeHive("SELECT * FROM orc_table_created_in_trino"))
                .containsOnly(row(42));
        // Hive 3.1 validates (`org.apache.orc.impl.ReaderImpl#ensureOrcFooter`) ORC footer only when loading it from the cache, so when querying *second* time.
        assertThat(env.executeHive("SELECT * FROM orc_table_created_in_trino"))
                .containsOnly(row(42));
        assertThat(env.executeHive("SELECT * FROM orc_table_created_in_trino WHERE a < 43"))
                .containsOnly(row(42));
        env.executeTrinoUpdate("DROP TABLE orc_table_created_in_trino");
    }

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testNestedFieldsWrittenByHive(String format, HiveStorageFormatsEnvironment env)
    {
        testNestedFields(format, WriterEngine.HIVE, env);
    }

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testNestedFieldsWrittenByTrino(String format, HiveStorageFormatsEnvironment env)
    {
        testNestedFields(format, WriterEngine.TRINO, env);
    }

    private void testNestedFields(String format, WriterEngine writer, HiveStorageFormatsEnvironment env)
    {
        String tableName = "test_nested_fields_written_by_" + writer.name().toLowerCase(ENGLISH);
        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + tableName);
        env.executeTrinoUpdate("CREATE TABLE " + tableName + " (" +
                "  r row(a int), " +
                "  rr row(r row(a int)), " +
                "  ra row(a array(int)), " +
                "  dummy varchar) WITH (format='" + format + "')");

        switch (writer) {
            case HIVE:
                ensureDummyExists(env);
                env.executeHiveUpdate("INSERT INTO " + tableName + " SELECT " +
                        "named_struct('a', 42), " +
                        "named_struct('r', named_struct('a', 43)), " +
                        "named_struct('a', array(11, 22, 33)), " +
                        "'dummy value' " +
                        "FROM dummy");
                break;
            case TRINO:
                env.executeTrinoUpdate("INSERT INTO " + tableName + " VALUES (" +
                        "row(42), " +
                        "row(row(43)), " +
                        "row(ARRAY[11, 22, 33]), " +
                        "'dummy value')");
                break;
            default:
                throw new IllegalStateException("Unsupported writer: " + writer);
        }

        assertThat(env.executeTrino("SELECT * FROM " + tableName))
                .containsOnly(row(
                        rowBuilder().addField("a", 42).build(),
                        rowBuilder()
                                .addField("r", rowBuilder().addField("a", 43).build())
                                .build(),
                        rowBuilder()
                                .addField("a", List.of(11, 22, 33))
                                .build(),
                        "dummy value"));

        // with dereference
        assertThat(env.executeTrino("SELECT r.a, rr.r.a, ra.a[2] FROM " + tableName))
                .containsOnly(row(42, 43, 22));

        // with dereference in predicate
        assertThat(env.executeTrino("SELECT dummy FROM " + tableName + " WHERE r.a = 42 AND rr.r.a = 43 AND ra.a[2] = 22"))
                .containsOnly(row("dummy value"));

        // verify with Hive if data written by Trino
        if (writer != WriterEngine.HIVE) {
            QueryResult queryResult = null;
            try {
                queryResult = env.executeHive("SELECT * FROM " + tableName);
                verify(queryResult != null);
            }
            catch (RuntimeException e) {
                if ("AVRO".equals(format)) {
                    // TODO (https://github.com/trinodb/trino/issues/9285) Some versions of Hive cannot read Avro nested structs written by Trino
                    assertThat(e.getCause())
                            .hasToString("java.sql.SQLException: java.io.IOException: org.apache.avro.AvroTypeException: Found record_1, expecting union");
                }
                else {
                    throw e;
                }
            }
            if (queryResult != null) {
                assertThat(queryResult)
                        .containsOnly(row(
                                "{\"a\":42}",
                                "{\"r\":{\"a\":43}}",
                                "{\"a\":[11,22,33]}",
                                "dummy value"));
            }
        }

        env.executeTrinoUpdate("DROP TABLE " + tableName);
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testOrcStructsWithNonLowercaseFields(HiveStorageFormatsEnvironment env)
    {
        String tableName = "orc_structs_with_non_lowercase";

        ensureDummyExists(env);
        env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);

        env.executeHiveUpdate(format(
                "CREATE TABLE %s (" +
                        "   c_bigint BIGINT," +
                        "   c_struct struct<testCustId:string, requestDate:string>)" +
                        "STORED AS ORC ",
                tableName));

        env.executeHiveUpdate(format(
                "INSERT INTO %s"
                        // insert with SELECT because hive does not support array/map/struct functions in VALUES
                        + " SELECT"
                        + "   1,"
                        + "   named_struct('testCustId', '1234', 'requestDate', 'some day')"
                        // some hive versions don't allow INSERT from SELECT without FROM
                        + " FROM dummy",
                tableName));

        // Test with projection pushdown enabled
        env.executeTrinoInSession(session -> {
            session.executeUpdate("SET SESSION hive.projection_pushdown_enabled = true");
            assertThat(session.executeQuery("SELECT c_struct.testCustId FROM " + tableName)).containsOnly(row("1234"));
            assertThat(session.executeQuery("SELECT c_struct.testcustid FROM " + tableName)).containsOnly(row("1234"));
            assertThat(session.executeQuery("SELECT c_struct.requestDate FROM " + tableName)).containsOnly(row("some day"));
        });

        // Test with projection pushdown disabled
        env.executeTrinoInSession(session -> {
            session.executeUpdate("SET SESSION hive.projection_pushdown_enabled = false");
            assertThat(session.executeQuery("SELECT c_struct.testCustId FROM " + tableName)).containsOnly(row("1234"));
            assertThat(session.executeQuery("SELECT c_struct.testcustid FROM " + tableName)).containsOnly(row("1234"));
            assertThat(session.executeQuery("SELECT c_struct.requestDate FROM " + tableName)).containsOnly(row("some day"));
        });
    }

    @ParameterizedTest
    @MethodSource("storageFormatsWithNanosecondPrecisionAsArguments")
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testTimestampCreatedFromHive(StorageFormat storageFormat, HiveStorageFormatsEnvironment env)
    {
        String tableName = createSimpleTimestampTable("timestamps_from_hive", storageFormat, env);

        // insert records one by one so that we have one file per record, which
        // allows us to exercise predicate push-down in Parquet (which only
        // works when the value range has a min = max)
        for (TimestampAndPrecision entry : TIMESTAMPS_FROM_HIVE) {
            env.executeHiveUpdate(format("INSERT INTO %s VALUES (%s, '%s')", tableName, entry.getId(), entry.getWriteValue()));
        }

        assertSimpleTimestamps(tableName, TIMESTAMPS_FROM_HIVE, env);
        env.executeTrinoUpdate("DROP TABLE " + tableName);
    }

    @ParameterizedTest
    @MethodSource("storageFormatsWithNanosecondPrecisionAsArguments")
    void testTimestampCreatedFromTrino(StorageFormat storageFormat, HiveStorageFormatsEnvironment env)
    {
        String tableName = createSimpleTimestampTable("timestamps_from_trino", storageFormat, env);

        // insert records one by one so that we have one file per record, which
        // allows us to exercise predicate push-down in Parquet (which only
        // works when the value range has a min = max)
        for (TimestampAndPrecision entry : TIMESTAMPS_FROM_TRINO) {
            env.executeTrinoInSession(session -> {
                session.executeUpdate(format("SET SESSION hive.timestamp_precision = '%s'", entry.getPrecision().name()));
                session.executeUpdate(format("INSERT INTO %s VALUES (%s, TIMESTAMP '%s')", tableName, entry.getId(), entry.getWriteValue()));
            });
        }

        assertSimpleTimestamps(tableName, TIMESTAMPS_FROM_TRINO, env);
        env.executeTrinoUpdate("DROP TABLE " + tableName);
    }

    @ParameterizedTest
    @MethodSource("storageFormatsWithNanosecondPrecisionAsArguments")
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testStructTimestampsFromHive(StorageFormat format, HiveStorageFormatsEnvironment env)
    {
        String tableName = createStructTimestampTable("hive_struct_timestamp", format, env);
        ensureDummyExists(env);

        // Insert one at a time because inserting with UNION ALL sometimes makes
        // data invisible to Trino (see https://github.com/trinodb/trino/issues/6485)
        for (TimestampAndPrecision entry : TIMESTAMPS_FROM_HIVE) {
            env.executeHiveUpdate(format(
                    "INSERT INTO %1$s"
                            // insert with SELECT because hive does not support array/map/struct functions in VALUES
                            + " SELECT"
                            + "   %3$s,"
                            + "   array(%2$s),"
                            + "   map(%2$s, %2$s),"
                            + "   named_struct('col', %2$s),"
                            + "   array(map(%2$s, named_struct('col', array(%2$s))))"
                            // some hive versions don't allow INSERT from SELECT without FROM
                            + " FROM dummy",
                    tableName,
                    format("TIMESTAMP '%s'", entry.getWriteValue()),
                    entry.getId()));
        }

        assertStructTimestamps(tableName, TIMESTAMPS_FROM_HIVE, env);
        env.executeTrinoUpdate(format("DROP TABLE %s", tableName));
    }

    @ParameterizedTest
    @MethodSource("storageFormatsWithNanosecondPrecisionAsArguments")
    void testStructTimestampsFromTrino(StorageFormat format, HiveStorageFormatsEnvironment env)
    {
        String tableName = createStructTimestampTable("trino_struct_timestamp", format, env);

        // Insert data grouped by write-precision so it rounds as expected
        TIMESTAMPS_FROM_TRINO.stream()
                .collect(Collectors.groupingBy(TimestampAndPrecision::getPrecision))
                .forEach((precision, data) -> {
                    env.executeTrinoInSession(session -> {
                        setAdminRole(session);
                        session.executeUpdate(format("SET SESSION hive.timestamp_precision = '%s'", precision.name()));
                        session.executeUpdate(format(
                                "INSERT INTO %s VALUES (%s)",
                                tableName,
                                data.stream().map(entry -> format(
                                        "%s,"
                                                + " array[%2$s],"
                                                + " map(array[%2$s], array[%2$s]),"
                                                + " row(%2$s),"
                                                + " array[map(array[%2$s], array[row(array[%2$s])])]",
                                        entry.getId(),
                                        format("TIMESTAMP '%s'", entry.getWriteValue())))
                                        .collect(joining("), ("))));
                    });
                });

        assertStructTimestamps(tableName, TIMESTAMPS_FROM_TRINO, env);
        env.executeTrinoUpdate(format("DROP TABLE %s", tableName));
    }

    // These are regression tests for issue: https://github.com/trinodb/trino/issues/5518
    // The Parquet session properties are set to ensure that the correct situations in the Parquet writer are met to replicate the bug.
    // Not included in the STORAGE_FORMATS group since they require a large insert, which takes some time.
    @Test
    void testLargeParquetInsert(HiveStorageFormatsEnvironment env)
    {
        DataSize reducedRowGroupSize = DataSize.ofBytes(ParquetProperties.DEFAULT_PAGE_SIZE / 4);
        runLargeInsert(storageFormat(
                "PARQUET",
                ImmutableMap.of(
                        "hive.parquet_writer_page_size", reducedRowGroupSize.toBytesValueString(),
                        "task_scale_writers_enabled", "false",
                        "task_min_writer_count", "1")), env);
    }

    @Test
    void testLargeOrcInsert(HiveStorageFormatsEnvironment env)
    {
        runLargeInsert(storageFormat("ORC", ImmutableMap.of("hive.orc_optimized_writer_validate", "true")), env);
    }

    private void runLargeInsert(StorageFormat storageFormat, HiveStorageFormatsEnvironment env)
    {
        String tableName = "test_large_insert_" + storageFormat.getName() + randomNameSuffix();
        // Use session to set session properties before creating table and inserting
        env.executeTrinoInSession(session -> {
            setSessionProperties(session, storageFormat);
            session.executeUpdate("CREATE TABLE " + tableName + " WITH (" + storageFormat.getStoragePropertiesAsSql() + ") AS SELECT * FROM tpch.sf1.lineitem WHERE false");
            session.executeUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.sf1.lineitem");
        });

        assertThat(env.executeTrino("SELECT count(*) FROM " + tableName)).containsOnly(row(6001215L));
        env.executeTrinoUpdate("DROP TABLE " + tableName);
    }

    // ---- Helper Methods ----

    private String createSimpleTimestampTable(String tableNamePrefix, StorageFormat format, HiveStorageFormatsEnvironment env)
    {
        return createTestTable(tableNamePrefix, format, "(id BIGINT, ts TIMESTAMP)", env);
    }

    /**
     * Assertions for tables created by {@link #createSimpleTimestampTable(String, StorageFormat, HiveStorageFormatsEnvironment)}
     */
    private static void assertSimpleTimestamps(String tableName, List<TimestampAndPrecision> data, HiveStorageFormatsEnvironment env)
    {
        SoftAssertions softly = new SoftAssertions();
        for (TimestampAndPrecision entry : data) {
            for (HiveTimestampPrecision precision : HiveTimestampPrecision.values()) {
                // Assert also with `CAST AS varchar` on the server side to avoid any JDBC-related issues
                softly.check(() -> {
                    env.executeTrinoInSession(session -> {
                        session.executeUpdate(format("SET SESSION hive.timestamp_precision = '%s'", precision.name()));
                        assertThat(session.executeQuery(
                                format("SELECT id, typeof(ts), CAST(ts AS varchar), ts FROM %s WHERE id = %s", tableName, entry.getId())))
                                .as("timestamp(%d)", precision.getPrecision())
                                .containsOnly(row(
                                        entry.getId(),
                                        entry.getReadType(precision),
                                        entry.getReadValue(precision),
                                        Timestamp.valueOf(entry.getReadValue(precision))));
                    });
                });
            }
        }
        softly.assertAll();
    }

    private String createStructTimestampTable(String tableNamePrefix, StorageFormat format, HiveStorageFormatsEnvironment env)
    {
        return createTestTable(tableNamePrefix, format, ""
                + "("
                + "   id INTEGER,"
                + "   arr ARRAY(TIMESTAMP),"
                + "   map MAP(TIMESTAMP, TIMESTAMP),"
                + "   row ROW(col TIMESTAMP),"
                + "   nested ARRAY(MAP(TIMESTAMP, ROW(col ARRAY(TIMESTAMP))))"
                + ")", env);
    }

    /**
     * Assertions for tables created by {@link #createStructTimestampTable(String, StorageFormat, HiveStorageFormatsEnvironment)}
     */
    private void assertStructTimestamps(String tableName, Collection<TimestampAndPrecision> data, HiveStorageFormatsEnvironment env)
    {
        SoftAssertions softly = new SoftAssertions();
        for (HiveTimestampPrecision precision : HiveTimestampPrecision.values()) {
            // Check that the correct types are read
            String type = format("timestamp(%d)", precision.getPrecision());
            softly.check(() -> env.executeTrinoInSession(session -> {
                session.executeUpdate(format("SET SESSION hive.timestamp_precision = '%s'", precision.name()));
                assertThat(session.executeQuery(format(
                        "SELECT"
                                + "   typeof(arr),"
                                + "   typeof(map),"
                                + "   typeof(row),"
                                + "   typeof(nested)"
                                + " FROM %s"
                                + " LIMIT 1",
                        tableName)))
                        .as("timestamp container types")
                        .containsOnly(row(
                                format("array(%s)", type),
                                format("map(%1$s, %1$s)", type),
                                format("row(\"col\" %s)", type),
                                format("array(map(%1$s, row(\"col\" array(%1$s))))", type)));
            }));

            // Check the values as varchar
            softly.check(() -> env.executeTrinoInSession(session -> {
                session.executeUpdate(format("SET SESSION hive.timestamp_precision = '%s'", precision.name()));
                assertThat(session.executeQuery(format(
                        "SELECT"
                                + "   id,"
                                + "   CAST(arr[1] AS VARCHAR),"
                                + "   CAST(map_entries(map)[1][1] AS VARCHAR)," // key
                                + "   CAST(map_entries(map)[1][2] AS VARCHAR)," // value
                                + "   CAST(row.col AS VARCHAR),"
                                + "   CAST(map_entries(nested[1])[1][1] AS VARCHAR)," // key
                                + "   CAST(map_entries(nested[1])[1][2].col[1] AS VARCHAR)" // value
                                + " FROM %s"
                                + " ORDER BY id",
                        tableName)))
                        .as("timestamp containers as varchar")
                        .containsExactlyInOrder(data.stream()
                                .sorted(comparingInt(TimestampAndPrecision::getId))
                                .map(e -> Row.fromList(Lists.asList(
                                        e.getId(),
                                        nCopies(6, e.getReadValue(precision)).toArray())))
                                .collect(toList()));
            }));

            // Check the values directly
            softly.check(() -> env.executeTrinoInSession(session -> {
                session.executeUpdate(format("SET SESSION hive.timestamp_precision = '%s'", precision.name()));
                assertThat(session.executeQuery(format(
                        "SELECT"
                                + "   id,"
                                + "   arr[1],"
                                + "   map_entries(map)[1][1]," // key
                                + "   map_entries(map)[1][2]," // value
                                + "   row.col,"
                                + "   map_entries(nested[1])[1][1]," // key
                                + "   map_entries(nested[1])[1][2].col[1]" // value
                                + " FROM %s"
                                + " ORDER BY id",
                        tableName)))
                        .as("timestamp containers")
                        .containsExactlyInOrder(data.stream()
                                .sorted(comparingInt(TimestampAndPrecision::getId))
                                .map(e -> Row.fromList(Lists.asList(
                                        e.getId(),
                                        nCopies(6, Timestamp.valueOf(e.getReadValue(precision))).toArray())))
                                .collect(toList()));
            }));
        }
        softly.assertAll();
    }

    private String createTestTable(String tableNamePrefix, StorageFormat format, String sql, HiveStorageFormatsEnvironment env)
    {
        String formatName = format.getName().toLowerCase(ENGLISH);
        String tableName = format("%s_%s_%s", tableNamePrefix, formatName, randomNameSuffix());
        // Use session to set admin role and session properties before creating table
        env.executeTrinoInSession(session -> {
            setAdminRole(session);
            setSessionProperties(session, format);
            session.executeUpdate(
                    format("CREATE TABLE %s %s WITH (%s)", tableName, sql, format.getStoragePropertiesAsSql()));
        });
        return tableName;
    }

    /**
     * Run the given query on the given table and the TPCH {@code lineitem} table
     * (in the schema {@code TPCH_SCHEMA}, asserting that the results are equal.
     */
    private static void assertResultEqualForLineitemTable(HiveStorageFormatsEnvironment env, String query, String tableName)
    {
        QueryResult expected = env.executeTrino(format(query, "tpch." + TPCH_SCHEMA + ".lineitem"));
        List<Row> expectedRows = expected.rows().stream()
                .map(columns -> row(columns.toArray()))
                .collect(toImmutableList());
        QueryResult actual = env.executeTrino(format(query, tableName));
        assertThat(actual)
                .hasColumns(expected.getColumnTypes())
                .containsExactlyInOrder(expectedRows);
    }

    private static void setAdminRole(ProductTestEnvironment.TrinoSession session)
    {
        try {
            session.executeUpdate("SET ROLE admin");
        }
        catch (SQLException _) {
            // The test environments do not properly setup or manage
            // roles, so try to set the role, but ignore any errors
        }
    }

    /**
     * Ensures that a view named "dummy" with exactly one row exists in the default schema.
     */
    // These tests run on versions of Hive (1.1.0 on CDH 5) that don't fully support SELECT without FROM
    private void ensureDummyExists(HiveStorageFormatsEnvironment env)
    {
        env.executeHiveUpdate("DROP TABLE IF EXISTS dummy");
        env.executeHiveUpdate("CREATE TABLE dummy (dummy varchar(1))");
        env.executeHiveUpdate("INSERT INTO dummy VALUES ('x')");
    }

    private static void setSessionProperties(ProductTestEnvironment.TrinoSession session, StorageFormat storageFormat)
            throws SQLException
    {
        setSessionProperties(session, storageFormat.getSessionProperties());
    }

    private static void setSessionProperties(ProductTestEnvironment.TrinoSession session, Map<String, String> sessionProperties)
            throws SQLException
    {
        // create more than one split
        session.executeUpdate("SET SESSION task_min_writer_count = 4");
        session.executeUpdate("SET SESSION task_scale_writers_enabled = false");
        session.executeUpdate("SET SESSION redistribute_writes = false");
        for (Map.Entry<String, String> sessionProperty : sessionProperties.entrySet()) {
            setSessionProperty(session, sessionProperty.getKey(), sessionProperty.getValue());
        }
    }

    private static void setSessionProperty(ProductTestEnvironment.TrinoSession session, String key, String value)
            throws SQLException
    {
        if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false") || value.chars().allMatch(Character::isDigit)) {
            session.executeUpdate(format("SET SESSION %s = %s", key, value));
            return;
        }
        session.executeUpdate(format("SET SESSION %s = '%s'", key, value));
    }

    private static StorageFormat storageFormat(String name)
    {
        return storageFormat(name, ImmutableMap.of());
    }

    private static StorageFormat storageFormat(String name, Map<String, String> sessionProperties)
    {
        return new StorageFormat(name, sessionProperties, ImmutableMap.of());
    }

    private static StorageFormat storageFormat(
            String name,
            Map<String, String> sessionProperties,
            Map<String, String> properties)
    {
        return new StorageFormat(name, sessionProperties, properties);
    }

    // ---- Inner Classes ----

    /**
     * Enum for which engine writes the data in nested field tests.
     */
    private enum WriterEngine
    {
        HIVE,
        TRINO
    }

    public static class StorageFormat
    {
        private final String name;
        private final Map<String, String> properties;
        private final Map<String, String> sessionProperties;

        private StorageFormat(
                String name,
                Map<String, String> sessionProperties,
                Map<String, String> properties)
        {
            this.name = requireNonNull(name, "name is null");
            this.properties = requireNonNull(properties, "properties is null");
            this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
        }

        public String getName()
        {
            return name;
        }

        public String getStoragePropertiesAsSql()
        {
            return Stream.concat(
                    Stream.of(immutableEntry("format", name)),
                    properties.entrySet().stream())
                    .map(entry -> format("%s = '%s'", entry.getKey(), entry.getValue()))
                    .collect(joining(", "));
        }

        public Map<String, String> getSessionProperties()
        {
            return sessionProperties;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("name", name)
                    .add("properties", properties)
                    .add("sessionProperties", sessionProperties)
                    .toString();
        }
    }

    /**
     * Create {@code TimestampAndPrecision} with specified write precision and rounded fractional seconds.
     *
     * @param writeValue The literal value to write.
     * @param precision Precision for writing value.
     * @param milliReadValue Expected value when reading with millisecond precision.
     * @param microReadValue Expected value when reading with microsecond precision.
     * @param nanoReadValue Expected value when reading with nanosecond precision.
     */
    private static TimestampAndPrecision timestampAndPrecision(
            String writeValue,
            HiveTimestampPrecision precision,
            String milliReadValue,
            String microReadValue,
            String nanoReadValue)
    {
        Map<HiveTimestampPrecision, String> readValues = Map.of(
                MILLISECONDS, milliReadValue,
                MICROSECONDS, microReadValue,
                NANOSECONDS, nanoReadValue);
        return new TimestampAndPrecision(precision, writeValue, readValues);
    }

    private static class TimestampAndPrecision
    {
        private static int counter;
        private final int id;
        // precision used when writing the data
        private final HiveTimestampPrecision precision;
        // inserted value
        private final String writeValue;
        // expected values to be read back at various precisions
        private final Map<HiveTimestampPrecision, String> readValues;

        public TimestampAndPrecision(HiveTimestampPrecision precision, String writeValue, Map<HiveTimestampPrecision, String> readValues)
        {
            this.id = counter++;
            this.precision = precision;
            this.writeValue = writeValue;
            this.readValues = readValues;
        }

        public int getId()
        {
            return id;
        }

        public HiveTimestampPrecision getPrecision()
        {
            return precision;
        }

        public String getWriteValue()
        {
            return writeValue;
        }

        public String getReadValue(HiveTimestampPrecision precision)
        {
            return requireNonNull(readValues.get(precision), () -> "no read value for " + precision);
        }

        public String getReadType(HiveTimestampPrecision precision)
        {
            return format("timestamp(%s)", precision.getPrecision());
        }
    }

    private static io.trino.jdbc.Row.Builder rowBuilder()
    {
        return io.trino.jdbc.Row.builder();
    }
}
