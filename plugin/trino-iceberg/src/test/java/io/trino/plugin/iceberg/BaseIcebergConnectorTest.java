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
package io.trino.plugin.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.operator.OperatorStats;
import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.server.DynamicFilterService;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.JsonUtil;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.trino.SystemSessionProperties.DETERMINE_PARTITION_COUNT_FOR_WRITE_ENABLED;
import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.SCALE_WRITERS;
import static io.trino.SystemSessionProperties.TASK_MAX_WRITER_COUNT;
import static io.trino.SystemSessionProperties.TASK_MIN_WRITER_COUNT;
import static io.trino.SystemSessionProperties.USE_PREFERRED_WRITE_PARTITIONING;
import static io.trino.plugin.iceberg.IcebergFileFormat.AVRO;
import static io.trino.plugin.iceberg.IcebergFileFormat.ORC;
import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergSessionProperties.COLLECT_EXTENDED_STATISTICS_ON_WRITE;
import static io.trino.plugin.iceberg.IcebergSessionProperties.DYNAMIC_FILTERING_WAIT_TIMEOUT;
import static io.trino.plugin.iceberg.IcebergSessionProperties.EXTENDED_STATISTICS_ENABLED;
import static io.trino.plugin.iceberg.IcebergSplitManager.ICEBERG_DOMAIN_COMPACTION_THRESHOLD;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.withSmallRowGroups;
import static io.trino.plugin.iceberg.IcebergUtil.TRINO_QUERY_ID_NAME;
import static io.trino.plugin.iceberg.IcebergUtil.TRINO_USER_NAME;
import static io.trino.plugin.iceberg.IcebergUtil.getLatestMetadataLocation;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TransactionBuilder.transaction;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.util.Collections.nCopies;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.apache.iceberg.TableMetadata.newTableMetadata;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.offset;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@Isolated // TODO remove
@TestInstance(PER_CLASS)
public abstract class BaseIcebergConnectorTest
        extends BaseConnectorTest
{
    private static final Pattern WITH_CLAUSE_EXTRACTOR = Pattern.compile(".*(WITH\\s*\\([^)]*\\))\\s*$", Pattern.DOTALL);

    protected final IcebergFileFormat format;

    protected TrinoFileSystem fileSystem;
    protected TimeUnit storageTimePrecision;

    protected BaseIcebergConnectorTest(IcebergFileFormat format)
    {
        this.format = requireNonNull(format, "format is null");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createQueryRunnerBuilder()
                .build();
    }

    protected IcebergQueryRunner.Builder createQueryRunnerBuilder()
    {
        return IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.<String, String>builder()
                        .put("iceberg.file-format", format.name())
                        // Only allow some extra properties. Add "sorted_by" so that we can test that the property is disallowed by the connector explicitly.
                        .put("iceberg.allowed-extra-properties", "extra.property.one,extra.property.two,extra.property.three,sorted_by")
                        // Allows testing the sorting writer flushing to the file system with smaller tables
                        .put("iceberg.writer-sort-buffer-size", "1MB")
                        .buildOrThrow())
                .setInitialTables(REQUIRED_TPCH_TABLES);
    }

    @BeforeAll
    public void initFileSystem()
    {
        fileSystem = getFileSystemFactory(getDistributedQueryRunner()).create(SESSION);
    }

    @BeforeAll
    public void initStorageTimePrecision()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "inspect_storage_precision", "(i int)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (2)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (3)", 1);

            long countWithSecondFraction = (Long) computeScalar("SELECT count(*) FILTER (WHERE \"$file_modified_time\" != date_trunc('second', \"$file_modified_time\")) FROM " + table.getName());
            // In the unlikely case where all files just happen to end up having no second fraction while storage actually supports millisecond precision,
            // we will run the test with reduced precision.
            storageTimePrecision = countWithSecondFraction == 0 ? SECONDS : MILLISECONDS;
        }
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_OR_REPLACE_TABLE,
                 SUPPORTS_REPORTING_WRITTEN_BYTES -> true;
            case SUPPORTS_ADD_COLUMN_NOT_NULL_CONSTRAINT,
                 SUPPORTS_RENAME_MATERIALIZED_VIEW_ACROSS_SCHEMAS,
                 SUPPORTS_TOPN_PUSHDOWN -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    public void testAddRowFieldCaseInsensitivity()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute,
                "test_add_row_field_case_insensitivity_",
                "AS SELECT CAST(row(row(2)) AS row(\"CHILD\" row(grandchild_1 integer))) AS col")) {
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(CHILD row(grandchild_1 integer))");

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN col.child.grandchild_2 integer");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(CHILD row(grandchild_1 integer, grandchild_2 integer))");

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN col.CHILD.grandchild_3 integer");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(CHILD row(grandchild_1 integer, grandchild_2 integer, grandchild_3 integer))");
        }
    }

    @Test
    @Override
    public void testAddAndDropColumnName()
    {
        for (String columnName : testColumnNameDataProvider()) {
            if (columnName.equals("a.dot")) {
                assertThatThrownBy(() -> testAddAndDropColumnName(columnName, requiresDelimiting(columnName)))
                        .hasMessage("Failed to add column: Cannot add column with ambiguous name: a.dot, use addColumn(parent, name, type)");
                return;
            }
            testAddAndDropColumnName(columnName, requiresDelimiting(columnName));
        }
    }

    @Override
    protected void verifyVersionedQueryFailurePermissible(Exception e)
    {
        assertThat(e)
                .hasMessageMatching("Version pointer type is not supported: .*|" +
                        "Unsupported type for temporal table version: .*|" +
                        "Unsupported type for table version: .*|" +
                        "No version history table tpch.nation at or before .*|" +
                        "Iceberg snapshot ID does not exists: .*|" +
                        "Cannot find snapshot with reference name: .*");
    }

    @Override
    protected void verifyConcurrentUpdateFailurePermissible(Exception e)
    {
        assertThat(e).hasMessageMatching("Failed to commit the transaction during write.*|" +
                "Failed to commit during write.*");
    }

    @Override
    protected void verifyConcurrentAddColumnFailurePermissible(Exception e)
    {
        assertThat(e)
                .hasMessageStartingWith("Failed to add column: Failed to replace table due to concurrent updates")
                .rootCause()
                .hasMessageContaining("Cannot update Iceberg table: supplied previous location does not match current location");
    }

    @Test
    public void testDeleteOnV1Table()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_", "WITH (format_version = 1) AS SELECT * FROM orders")) {
            assertQueryFails("DELETE FROM " + table.getName() + " WHERE custkey <= 100", "Iceberg table updates require at least format version 2");
        }
    }

    @Test
    @Override
    public void testCharVarcharComparison()
    {
        // with char->varchar coercion on table creation, this is essentially varchar/varchar comparison
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_char_varchar",
                "(k, v) AS VALUES" +
                        "   (-1, CAST(NULL AS CHAR(3))), " +
                        "   (3, CAST('   ' AS CHAR(3)))," +
                        "   (6, CAST('x  ' AS CHAR(3)))")) {
            // varchar of length shorter than column's length
            assertThat(query("SELECT k, v FROM " + table.getName() + " WHERE v = CAST('  ' AS varchar(2))")).returnsEmptyResult();
            // varchar of length longer than column's length
            assertThat(query("SELECT k, v FROM " + table.getName() + " WHERE v = CAST('    ' AS varchar(4))")).returnsEmptyResult();
            // value that's not all-spaces
            assertThat(query("SELECT k, v FROM " + table.getName() + " WHERE v = CAST('x ' AS varchar(2))")).returnsEmptyResult();
            // exact match
            assertQuery("SELECT k, v FROM " + table.getName() + " WHERE v = CAST('   ' AS varchar(3))", "VALUES (3, '   ')");
        }
    }

    @Test
    @Override
    public void testShowCreateSchema()
    {
        assertThat(computeActual("SHOW CREATE SCHEMA tpch").getOnlyValue().toString())
                .matches("CREATE SCHEMA iceberg.tpch\n" +
                        "AUTHORIZATION USER user\n" +
                        "WITH \\(\n" +
                        "\\s+location = '.*/tpch'\n" +
                        "\\)");
    }

    @Override
    protected MaterializedResult getDescribeOrdersResult()
    {
        return resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar", "", "")
                .build();
    }

    @Override
    @Test
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .matches("\\QCREATE TABLE iceberg.tpch.orders (\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar,\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar,\n" +
                        "   clerk varchar,\n" +
                        "   shippriority integer,\n" +
                        "   comment varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = '" + format.name() + "',\n" +
                        "   format_version = 2,\n" +
                        "   location = '\\E.*/tpch/orders-.*\\Q'\n" +
                        ")\\E");
    }

    @Test
    public void testPartitionedByRealWithNaN()
    {
        String tableName = "test_partitioned_by_real" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH(partitioning = ARRAY['part']) AS SELECT 1 AS id, real 'NaN' AS part", 1);

        assertQuery("SELECT part FROM " + tableName, "VALUES cast('NaN' as real)");
        assertQuery("SELECT id FROM " + tableName + " WHERE is_nan(part)", "VALUES 1");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testPartitionedByDoubleWithNaN()
    {
        String tableName = "test_partitioned_by_double" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH(partitioning = ARRAY['part']) AS SELECT 1 AS id, double 'NaN' AS part", 1);

        assertQuery("SELECT part FROM " + tableName, "VALUES cast('NaN' as double)");
        assertQuery("SELECT id FROM " + tableName + " WHERE is_nan(part)", "VALUES 1");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDecimal()
    {
        testDecimalWithPrecisionAndScale(1, 0);
        testDecimalWithPrecisionAndScale(8, 6);
        testDecimalWithPrecisionAndScale(9, 8);
        testDecimalWithPrecisionAndScale(10, 8);

        testDecimalWithPrecisionAndScale(18, 1);
        testDecimalWithPrecisionAndScale(18, 8);
        testDecimalWithPrecisionAndScale(18, 17);

        testDecimalWithPrecisionAndScale(17, 16);
        testDecimalWithPrecisionAndScale(18, 17);
        testDecimalWithPrecisionAndScale(24, 10);
        testDecimalWithPrecisionAndScale(30, 10);
        testDecimalWithPrecisionAndScale(37, 26);
        testDecimalWithPrecisionAndScale(38, 37);

        testDecimalWithPrecisionAndScale(38, 17);
        testDecimalWithPrecisionAndScale(38, 37);
    }

    private void testDecimalWithPrecisionAndScale(int precision, int scale)
    {
        checkArgument(precision >= 1 && precision <= 38, "Decimal precision (%s) must be between 1 and 38 inclusive", precision);
        checkArgument(scale < precision && scale >= 0, "Decimal scale (%s) must be less than the precision (%s) and non-negative", scale, precision);

        String decimalType = format("DECIMAL(%d,%d)", precision, scale);
        String beforeTheDecimalPoint = "12345678901234567890123456789012345678".substring(0, precision - scale);
        String afterTheDecimalPoint = "09876543210987654321098765432109876543".substring(0, scale);
        String decimalValue = format("%s.%s", beforeTheDecimalPoint, afterTheDecimalPoint);

        assertUpdate(format("CREATE TABLE test_iceberg_decimal (x %s)", decimalType));
        assertUpdate(format("INSERT INTO test_iceberg_decimal (x) VALUES (CAST('%s' AS %s))", decimalValue, decimalType), 1);
        assertQuery("SELECT * FROM test_iceberg_decimal", format("SELECT CAST('%s' AS %s)", decimalValue, decimalType));
        assertUpdate("DROP TABLE test_iceberg_decimal");
    }

    @Test
    public void testTime()
    {
        testSelectOrPartitionedByTime(false);
    }

    @Test
    public void testPartitionedByTime()
    {
        testSelectOrPartitionedByTime(true);
    }

    private void testSelectOrPartitionedByTime(boolean partitioned)
    {
        String tableName = format("test_%s_by_time", partitioned ? "partitioned" : "selected");
        String partitioning = partitioned ? "WITH(partitioning = ARRAY['x'])" : "";
        assertUpdate(format("CREATE TABLE %s (x TIME(6), y BIGINT) %s", tableName, partitioning));
        assertUpdate(format("INSERT INTO %s VALUES (TIME '10:12:34', 12345)", tableName), 1);
        assertQuery(format("SELECT COUNT(*) FROM %s", tableName), "SELECT 1");
        assertQuery(format("SELECT x FROM %s", tableName), "SELECT CAST('10:12:34' AS TIME)");
        assertUpdate(format("INSERT INTO %s VALUES (TIME '9:00:00', 67890)", tableName), 1);
        assertQuery(format("SELECT COUNT(*) FROM %s", tableName), "SELECT 2");
        assertQuery(format("SELECT x FROM %s WHERE x = TIME '10:12:34'", tableName), "SELECT CAST('10:12:34' AS TIME)");
        assertQuery(format("SELECT x FROM %s WHERE x = TIME '9:00:00'", tableName), "SELECT CAST('9:00:00' AS TIME)");
        assertQuery(format("SELECT x FROM %s WHERE y = 12345", tableName), "SELECT CAST('10:12:34' AS TIME)");
        assertQuery(format("SELECT x FROM %s WHERE y = 67890", tableName), "SELECT CAST('9:00:00' AS TIME)");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testPartitionByTimestamp()
    {
        testSelectOrPartitionedByTimestamp(true);
    }

    @Test
    public void testSelectByTimestamp()
    {
        testSelectOrPartitionedByTimestamp(false);
    }

    private void testSelectOrPartitionedByTimestamp(boolean partitioned)
    {
        String tableName = format("test_%s_by_timestamp", partitioned ? "partitioned" : "selected");
        assertUpdate(format("CREATE TABLE %s (_timestamp timestamp(6)) %s",
                tableName, partitioned ? "WITH (partitioning = ARRAY['_timestamp'])" : ""));
        @Language("SQL") String select1 = "SELECT TIMESTAMP '2017-05-01 10:12:34' _timestamp";
        @Language("SQL") String select2 = "SELECT TIMESTAMP '2017-10-01 10:12:34' _timestamp";
        @Language("SQL") String select3 = "SELECT TIMESTAMP '2018-05-01 10:12:34' _timestamp";
        assertUpdate(format("INSERT INTO %s %s", tableName, select1), 1);
        assertUpdate(format("INSERT INTO %s %s", tableName, select2), 1);
        assertUpdate(format("INSERT INTO %s %s", tableName, select3), 1);
        assertQuery(format("SELECT COUNT(*) from %s", tableName), "SELECT 3");

        assertQuery(format("SELECT * from %s WHERE _timestamp = TIMESTAMP '2017-05-01 10:12:34'", tableName), select1);
        assertQuery(format("SELECT * from %s WHERE _timestamp < TIMESTAMP '2017-06-01 10:12:34'", tableName), select1);
        assertQuery(format("SELECT * from %s WHERE _timestamp = TIMESTAMP '2017-10-01 10:12:34'", tableName), select2);
        assertQuery(format("SELECT * from %s WHERE _timestamp > TIMESTAMP '2017-06-01 10:12:34' AND _timestamp < TIMESTAMP '2018-05-01 10:12:34'", tableName), select2);
        assertQuery(format("SELECT * from %s WHERE _timestamp = TIMESTAMP '2018-05-01 10:12:34'", tableName), select3);
        assertQuery(format("SELECT * from %s WHERE _timestamp > TIMESTAMP '2018-01-01 10:12:34'", tableName), select3);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testPartitionByTimestampWithTimeZone()
    {
        testSelectOrPartitionedByTimestampWithTimeZone(true);
    }

    @Test
    public void testSelectByTimestampWithTimeZone()
    {
        testSelectOrPartitionedByTimestampWithTimeZone(false);
    }

    private void testSelectOrPartitionedByTimestampWithTimeZone(boolean partitioned)
    {
        String tableName = format("test_%s_by_timestamptz", partitioned ? "partitioned" : "selected");
        assertUpdate(format(
                "CREATE TABLE %s (_timestamptz timestamp(6) with time zone) %s",
                tableName,
                partitioned ? "WITH (partitioning = ARRAY['_timestamptz'])" : ""));

        String instant1Utc = "TIMESTAMP '2021-10-31 00:30:00.005000 UTC'";
        String instant1La = "TIMESTAMP '2021-10-30 17:30:00.005000 America/Los_Angeles'";
        String instant2Utc = "TIMESTAMP '2021-10-31 00:30:00.006000 UTC'";
        String instant2La = "TIMESTAMP '2021-10-30 17:30:00.006000 America/Los_Angeles'";
        String instant3Utc = "TIMESTAMP '2021-10-31 00:30:00.007000 UTC'";
        String instant3La = "TIMESTAMP '2021-10-30 17:30:00.007000 America/Los_Angeles'";
        // regression test value for https://github.com/trinodb/trino/issues/12852
        String instant4Utc = "TIMESTAMP '1969-12-01 05:06:07.234567 UTC'";

        assertUpdate(format("INSERT INTO %s VALUES %s", tableName, instant1Utc), 1);
        assertUpdate(format("INSERT INTO %s VALUES %s", tableName, instant2La /* non-UTC for this one */), 1);
        assertUpdate(format("INSERT INTO %s VALUES %s", tableName, instant3Utc), 1);
        assertUpdate(format("INSERT INTO %s VALUES %s", tableName, instant4Utc), 1);
        assertQuery(format("SELECT COUNT(*) from %s", tableName), "SELECT 4");

        // =
        assertThat(query(format("SELECT * from %s WHERE _timestamptz = %s", tableName, instant1Utc)))
                .matches("VALUES " + instant1Utc);
        assertThat(query(format("SELECT * from %s WHERE _timestamptz = %s", tableName, instant1La)))
                .matches("VALUES " + instant1Utc);
        assertThat(query(format("SELECT * from %s WHERE _timestamptz = %s", tableName, instant2Utc)))
                .matches("VALUES " + instant2Utc);
        assertThat(query(format("SELECT * from %s WHERE _timestamptz = %s", tableName, instant2La)))
                .matches("VALUES " + instant2Utc);
        assertThat(query(format("SELECT * from %s WHERE _timestamptz = %s", tableName, instant3Utc)))
                .matches("VALUES " + instant3Utc);
        assertThat(query(format("SELECT * from %s WHERE _timestamptz = %s", tableName, instant3La)))
                .matches("VALUES " + instant3Utc);
        assertThat(query(format("SELECT * from %s WHERE _timestamptz = %s", tableName, instant4Utc)))
                .matches("VALUES " + instant4Utc);

        // <
        assertThat(query(format("SELECT * from %s WHERE _timestamptz < %s", tableName, instant2Utc)))
                .matches(format("VALUES %s, %s", instant1Utc, instant4Utc));
        assertThat(query(format("SELECT * from %s WHERE _timestamptz < %s", tableName, instant2La)))
                .matches(format("VALUES %s, %s", instant1Utc, instant4Utc));
        assertThat(query(format("SELECT * from %s WHERE _timestamptz < %s", tableName, instant3Utc)))
                .matches(format("VALUES %s, %s, %s", instant1Utc, instant2Utc, instant4Utc));
        assertThat(query(format("SELECT * from %s WHERE _timestamptz < %s", tableName, instant3La)))
                .matches(format("VALUES %s, %s, %s", instant1Utc, instant2Utc, instant4Utc));

        // <=
        assertThat(query(format("SELECT * from %s WHERE _timestamptz <= %s", tableName, instant2Utc)))
                .matches(format("VALUES %s, %s, %s", instant1Utc, instant2Utc, instant4Utc));
        assertThat(query(format("SELECT * from %s WHERE _timestamptz <= %s", tableName, instant2La)))
                .matches(format("VALUES %s, %s, %s", instant1Utc, instant2Utc, instant4Utc));

        // >
        assertThat(query(format("SELECT * from %s WHERE _timestamptz > %s", tableName, instant2Utc)))
                .matches("VALUES " + instant3Utc);
        assertThat(query(format("SELECT * from %s WHERE _timestamptz > %s", tableName, instant2La)))
                .matches("VALUES " + instant3Utc);
        assertThat(query(format("SELECT * from %s WHERE _timestamptz > %s", tableName, instant1Utc)))
                .matches(format("VALUES %s, %s", instant2Utc, instant3Utc));
        assertThat(query(format("SELECT * from %s WHERE _timestamptz > %s", tableName, instant1La)))
                .matches(format("VALUES %s, %s", instant2Utc, instant3Utc));

        // >=
        assertThat(query(format("SELECT * from %s WHERE _timestamptz >= %s", tableName, instant2Utc)))
                .matches(format("VALUES %s, %s", instant2Utc, instant3Utc));
        assertThat(query(format("SELECT * from %s WHERE _timestamptz >= %s", tableName, instant2La)))
                .matches(format("VALUES %s, %s", instant2Utc, instant3Utc));

        // open range
        assertThat(query(format("SELECT * from %s WHERE _timestamptz > %s AND _timestamptz < %s", tableName, instant1Utc, instant3Utc)))
                .matches("VALUES " + instant2Utc);
        assertThat(query(format("SELECT * from %s WHERE _timestamptz > %s AND _timestamptz < %s", tableName, instant1La, instant3La)))
                .matches("VALUES " + instant2Utc);

        // closed range
        assertThat(query(format("SELECT * from %s WHERE _timestamptz BETWEEN %s AND %s", tableName, instant1Utc, instant2Utc)))
                .matches(format("VALUES %s, %s", instant1Utc, instant2Utc));
        assertThat(query(format("SELECT * from %s WHERE _timestamptz BETWEEN %s AND %s", tableName, instant1La, instant2La)))
                .matches(format("VALUES %s, %s", instant1Utc, instant2Utc));

        // !=
        assertThat(query(format("SELECT * from %s WHERE _timestamptz != %s", tableName, instant1Utc)))
                .matches(format("VALUES %s, %s, %s", instant2Utc, instant3Utc, instant4Utc));
        assertThat(query(format("SELECT * from %s WHERE _timestamptz != %s", tableName, instant1La)))
                .matches(format("VALUES %s, %s, %s", instant2Utc, instant3Utc, instant4Utc));
        assertThat(query(format("SELECT * from %s WHERE _timestamptz != %s", tableName, instant2Utc)))
                .matches(format("VALUES %s, %s, %s", instant1Utc, instant3Utc, instant4Utc));
        assertThat(query(format("SELECT * from %s WHERE _timestamptz != %s", tableName, instant2La)))
                .matches(format("VALUES %s, %s, %s", instant1Utc, instant3Utc, instant4Utc));
        assertThat(query(format("SELECT * from %s WHERE _timestamptz != %s", tableName, instant4Utc)))
                .matches(format("VALUES %s, %s, %s", instant1Utc, instant2Utc, instant3Utc));

        // IS DISTINCT FROM
        assertThat(query(format("SELECT * from %s WHERE _timestamptz IS DISTINCT FROM %s", tableName, instant1Utc)))
                .matches(format("VALUES %s, %s, %s", instant2Utc, instant3Utc, instant4Utc));
        assertThat(query(format("SELECT * from %s WHERE _timestamptz IS DISTINCT FROM %s", tableName, instant1La)))
                .matches(format("VALUES %s, %s, %s", instant2Utc, instant3Utc, instant4Utc));
        assertThat(query(format("SELECT * from %s WHERE _timestamptz IS DISTINCT FROM %s", tableName, instant2Utc)))
                .matches(format("VALUES %s, %s, %s", instant1Utc, instant3Utc, instant4Utc));
        assertThat(query(format("SELECT * from %s WHERE _timestamptz IS DISTINCT FROM %s", tableName, instant2La)))
                .matches(format("VALUES %s, %s, %s", instant1Utc, instant3Utc, instant4Utc));
        assertThat(query(format("SELECT * from %s WHERE _timestamptz IS DISTINCT FROM %s", tableName, instant4Utc)))
                .matches(format("VALUES %s, %s, %s", instant1Utc, instant2Utc, instant3Utc));

        // IS NOT DISTINCT FROM
        assertThat(query(format("SELECT * from %s WHERE _timestamptz IS NOT DISTINCT FROM %s", tableName, instant1Utc)))
                .matches("VALUES " + instant1Utc);
        assertThat(query(format("SELECT * from %s WHERE _timestamptz IS NOT DISTINCT FROM %s", tableName, instant1La)))
                .matches("VALUES " + instant1Utc);
        assertThat(query(format("SELECT * from %s WHERE _timestamptz IS NOT DISTINCT FROM %s", tableName, instant2Utc)))
                .matches("VALUES " + instant2Utc);
        assertThat(query(format("SELECT * from %s WHERE _timestamptz IS NOT DISTINCT FROM %s", tableName, instant2La)))
                .matches("VALUES " + instant2Utc);
        assertThat(query(format("SELECT * from %s WHERE _timestamptz IS NOT DISTINCT FROM %s", tableName, instant3Utc)))
                .matches("VALUES " + instant3Utc);
        assertThat(query(format("SELECT * from %s WHERE _timestamptz IS NOT DISTINCT FROM %s", tableName, instant3La)))
                .matches("VALUES " + instant3Utc);
        assertThat(query(format("SELECT * from %s WHERE _timestamptz IS NOT DISTINCT FROM %s", tableName, instant4Utc)))
                .matches("VALUES " + instant4Utc);

        if (partitioned) {
            assertThat(query(format("SELECT record_count, file_count, partition._timestamptz FROM \"%s$partitions\"", tableName)))
                    .matches(format(
                            "VALUES (BIGINT '1', BIGINT '1', %s), (BIGINT '1', BIGINT '1', %s), (BIGINT '1', BIGINT '1', %s), (BIGINT '1', BIGINT '1', %s)",
                            instant1Utc,
                            instant2Utc,
                            instant3Utc,
                            instant4Utc));
        }
        else {
            if (format != AVRO) {
                assertThat(query(format("SELECT record_count, file_count, data._timestamptz FROM \"%s$partitions\"", tableName)))
                        .matches(format(
                                "VALUES (BIGINT '4', BIGINT '4', CAST(ROW(%s, %s, 0, NULL) AS row(min timestamp(6) with time zone, max timestamp(6) with time zone, null_count bigint, nan_count bigint)))",
                                format == ORC ? "TIMESTAMP '1969-12-01 05:06:07.234000 UTC'" : instant4Utc,
                                format == ORC ? "TIMESTAMP '2021-10-31 00:30:00.007999 UTC'" : instant3Utc));
            }
            else {
                assertThat(query(format("SELECT record_count, file_count, data._timestamptz FROM \"%s$partitions\"", tableName)))
                        .skippingTypesCheck()
                        .matches("VALUES (BIGINT '4', BIGINT '4', CAST(NULL AS row(min timestamp(6) with time zone, max timestamp(6) with time zone, null_count bigint, nan_count bigint)))");
            }
        }

        if (partitioned) {
            // show stats
            assertThat(query("SHOW STATS FOR " + tableName))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "('_timestamptz', NULL, 4e0, 0e0, NULL, '1969-12-01 05:06:07.234 UTC', '2021-10-31 00:30:00.007 UTC'), " +
                            "(NULL, NULL, NULL, NULL, 4e0, NULL, NULL)");
        }
        else {
            // show stats
            if (format != AVRO) {
                assertThat(query("SHOW STATS FOR " + tableName))
                        .skippingTypesCheck()
                        .matches("VALUES " +
                                "('_timestamptz', NULL, 4e0, 0e0, NULL, '1969-12-01 05:06:07.234 UTC', '2021-10-31 00:30:00.007 UTC'), " +
                                "(NULL, NULL, NULL, NULL, 4e0, NULL, NULL)");
            }
            else {
                assertThat(query("SHOW STATS FOR " + tableName))
                        .skippingTypesCheck()
                        .matches("VALUES " +
                                "('_timestamptz', NULL, 4e0, 0e0, NULL, NULL, NULL), " +
                                "(NULL, NULL, NULL, NULL, 4e0, NULL, NULL)");
            }
        }

        if (partitioned) {
            // show stats with predicate
            assertThat(query("SHOW STATS FOR (SELECT * FROM " + tableName + " WHERE _timestamptz = " + instant1La + ")"))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            // TODO (https://github.com/trinodb/trino/issues/9716) the min/max values are off by 1 millisecond
                            "('_timestamptz', NULL, 1e0, 0e0, NULL, '2021-10-31 00:30:00.005 UTC', '2021-10-31 00:30:00.005 UTC'), " +
                            "(NULL, NULL, NULL, NULL, 1e0, NULL, NULL)");
        }
        else {
            // show stats with predicate
            assertThat(query("SHOW STATS FOR (SELECT * FROM " + tableName + " WHERE _timestamptz = " + instant1La + ")"))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "('_timestamptz', null, 1e0, 0e0, NULL, '2021-10-31 00:30:00.005 UTC', '2021-10-31 00:30:00.005 UTC'), " +
                            "(NULL, NULL, NULL, NULL, 1e0, NULL, NULL)");
        }

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testUuid()
    {
        testSelectOrPartitionedByUuid(false);
    }

    @Test
    public void testPartitionedByUuid()
    {
        testSelectOrPartitionedByUuid(true);
    }

    private void testSelectOrPartitionedByUuid(boolean partitioned)
    {
        String tableName = format("test_%s_by_uuid", partitioned ? "partitioned" : "selected");
        String partitioning = partitioned ? "WITH (partitioning = ARRAY['x'])" : "";
        assertUpdate(format("DROP TABLE IF EXISTS %s", tableName));
        assertUpdate(format("CREATE TABLE %s (x uuid, y bigint) %s", tableName, partitioning));

        assertUpdate(format("INSERT INTO %s VALUES (UUID '406caec7-68b9-4778-81b2-a12ece70c8b1', 12345)", tableName), 1);
        assertQuery(format("SELECT count(*) FROM %s", tableName), "SELECT 1");
        assertQuery(format("SELECT x FROM %s", tableName), "SELECT CAST('406caec7-68b9-4778-81b2-a12ece70c8b1' AS UUID)");

        assertUpdate(format("INSERT INTO %s VALUES (UUID 'f79c3e09-677c-4bbd-a479-3f349cb785e7', 67890)", tableName), 1);
        assertUpdate(format("INSERT INTO %s VALUES (NULL, 7531)", tableName), 1);
        assertQuery(format("SELECT count(*) FROM %s", tableName), "SELECT 3");
        assertQuery(format("SELECT * FROM %s WHERE x = UUID '406caec7-68b9-4778-81b2-a12ece70c8b1'", tableName), "SELECT CAST('406caec7-68b9-4778-81b2-a12ece70c8b1' AS UUID), 12345");
        assertQuery(format("SELECT * FROM %s WHERE x = UUID 'f79c3e09-677c-4bbd-a479-3f349cb785e7'", tableName), "SELECT CAST('f79c3e09-677c-4bbd-a479-3f349cb785e7' AS UUID), 67890");
        assertQuery(
                format("SELECT * FROM %s WHERE x >= UUID '406caec7-68b9-4778-81b2-a12ece70c8b1'", tableName),
                "VALUES (CAST('f79c3e09-677c-4bbd-a479-3f349cb785e7' AS UUID), 67890), (CAST('406caec7-68b9-4778-81b2-a12ece70c8b1' AS UUID), 12345)");
        assertQuery(
                format("SELECT * FROM %s WHERE x >= UUID 'f79c3e09-677c-4bbd-a479-3f349cb785e7'", tableName),
                "SELECT CAST('f79c3e09-677c-4bbd-a479-3f349cb785e7' AS UUID), 67890");
        assertQuery(format("SELECT * FROM %s WHERE x IS NULL", tableName), "SELECT NULL, 7531");
        assertQuery(format("SELECT x FROM %s WHERE y = 12345", tableName), "SELECT CAST('406caec7-68b9-4778-81b2-a12ece70c8b1' AS UUID)");
        assertQuery(format("SELECT x FROM %s WHERE y = 67890", tableName), "SELECT CAST('f79c3e09-677c-4bbd-a479-3f349cb785e7' AS UUID)");
        assertQuery(format("SELECT x FROM %s WHERE y = 7531", tableName), "SELECT NULL");

        assertUpdate(format("INSERT INTO %s VALUES (UUID '206caec7-68b9-4778-81b2-a12ece70c8b1', 313), (UUID '906caec7-68b9-4778-81b2-a12ece70c8b1', 314)", tableName), 2);
        assertThat(query("SELECT y FROM " + tableName + " WHERE x >= UUID '206caec7-68b9-4778-81b2-a12ece70c8b1'"))
                .matches("VALUES BIGINT '12345', 67890, 313, 314");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testNestedUuid()
    {
        assertUpdate("CREATE TABLE test_nested_uuid (int_t int, row_t row(uuid_t uuid, int_t int), map_t map(int, uuid), array_t array(uuid))");

        String uuid = "UUID '406caec7-68b9-4778-81b2-a12ece70c8b1'";
        String value = format("VALUES (2, row(%1$s, 1), map(array[1], array[%1$s]), array[%1$s, %1$s])", uuid);
        assertUpdate("INSERT INTO test_nested_uuid " + value, 1);

        assertThat(query("SELECT row_t.int_t, row_t.uuid_t FROM test_nested_uuid"))
                .matches("VALUES (1, UUID '406caec7-68b9-4778-81b2-a12ece70c8b1')");
        assertThat(query("SELECT map_t[1] FROM test_nested_uuid"))
                .matches("VALUES UUID '406caec7-68b9-4778-81b2-a12ece70c8b1'");
        assertThat(query("SELECT array_t FROM test_nested_uuid"))
                .matches("VALUES ARRAY[UUID '406caec7-68b9-4778-81b2-a12ece70c8b1', UUID '406caec7-68b9-4778-81b2-a12ece70c8b1']");

        assertQuery("SELECT row_t.int_t FROM test_nested_uuid WHERE row_t.uuid_t = UUID '406caec7-68b9-4778-81b2-a12ece70c8b1'", "VALUES 1");
        assertQuery("SELECT int_t FROM test_nested_uuid WHERE row_t.uuid_t = UUID '406caec7-68b9-4778-81b2-a12ece70c8b1'", "VALUES 2");
    }

    @Test
    public void testCreatePartitionedTable()
    {
        assertUpdate("" +
                "CREATE TABLE test_partitioned_table (" +
                "  a_boolean boolean, " +
                "  an_integer integer, " +
                "  a_bigint bigint, " +
                "  a_real real, " +
                "  a_double double, " +
                "  a_short_decimal decimal(5,2), " +
                "  a_long_decimal decimal(38,20), " +
                "  a_varchar varchar, " +
                "  a_varbinary varbinary, " +
                "  a_date date, " +
                "  a_time time(6), " +
                "  a_timestamp timestamp(6), " +
                "  a_timestamptz timestamp(6) with time zone, " +
                "  a_uuid uuid, " +
                "  a_row row(id integer, vc varchar), " +
                "  an_array array(varchar), " +
                "  a_map map(integer, varchar), " +
                "  \"a quoted, field\" varchar" +
                ") " +
                "WITH (" +
                "partitioning = ARRAY[" +
                "  'a_boolean', " +
                "  'an_integer', " +
                "  'a_bigint', " +
                "  'a_real', " +
                "  'a_double', " +
                "  'a_short_decimal', " +
                "  'a_long_decimal', " +
                "  'a_varchar', " +
                "  'a_varbinary', " +
                "  'a_date', " +
                "  'a_time', " +
                "  'a_timestamp', " +
                "  'a_timestamptz', " +
                "  'a_uuid', " +
                "  '\"a quoted, field\"' " +
                // Note: partitioning on non-primitive columns is not allowed in Iceberg
                "  ]" +
                ")");

        assertQueryReturnsEmptyResult("SELECT * FROM test_partitioned_table");

        String values = "VALUES (" +
                "true, " +
                "1, " +
                "BIGINT '1', " +
                "REAL '1.0', " +
                "DOUBLE '1.0', " +
                "CAST(1.0 AS decimal(5,2)), " +
                "CAST(11.0 AS decimal(38,20)), " +
                "VARCHAR 'onefsadfdsf', " +
                "X'000102f0feff', " +
                "DATE '2021-07-24'," +
                "TIME '02:43:57.987654', " +
                "TIMESTAMP '2021-07-24 03:43:57.987654'," +
                "TIMESTAMP '2021-07-24 04:43:57.987654 UTC', " +
                "UUID '20050910-1330-11e9-ffff-2a86e4085a59', " +
                "CAST(ROW(42, 'this is a random value') AS ROW(id int, vc varchar)), " +
                "ARRAY[VARCHAR 'uno', 'dos', 'tres'], " +
                "map(ARRAY[1,2], ARRAY['ek', VARCHAR 'one']), " +
                "VARCHAR 'tralala')";

        String nullValues = nCopies(18, "NULL").stream()
                .collect(joining(", ", "VALUES (", ")"));

        assertUpdate("INSERT INTO test_partitioned_table " + values, 1);
        assertUpdate("INSERT INTO test_partitioned_table " + nullValues, 1);

        // SELECT
        assertThat(query("SELECT * FROM test_partitioned_table"))
                .matches(values + " UNION ALL " + nullValues);

        // SELECT with predicates
        assertThat(query("SELECT * FROM test_partitioned_table WHERE " +
                "    a_boolean = true " +
                "AND an_integer = 1 " +
                "AND a_bigint = BIGINT '1' " +
                "AND a_real = REAL '1.0' " +
                "AND a_double = DOUBLE '1.0' " +
                "AND a_short_decimal = CAST(1.0 AS decimal(5,2)) " +
                "AND a_long_decimal = CAST(11.0 AS decimal(38,20)) " +
                "AND a_varchar = VARCHAR 'onefsadfdsf' " +
                "AND a_varbinary = X'000102f0feff' " +
                "AND a_date = DATE '2021-07-24' " +
                "AND a_time = TIME '02:43:57.987654' " +
                "AND a_timestamp = TIMESTAMP '2021-07-24 03:43:57.987654' " +
                "AND a_timestamptz = TIMESTAMP '2021-07-24 04:43:57.987654 UTC' " +
                "AND a_uuid = UUID '20050910-1330-11e9-ffff-2a86e4085a59' " +
                "AND a_row = CAST(ROW(42, 'this is a random value') AS ROW(id int, vc varchar)) " +
                "AND an_array = ARRAY[VARCHAR 'uno', 'dos', 'tres'] " +
                "AND a_map = map(ARRAY[1,2], ARRAY['ek', VARCHAR 'one']) " +
                "AND \"a quoted, field\" = VARCHAR 'tralala' " +
                ""))
                .matches(values);

        assertThat(query("SELECT * FROM test_partitioned_table WHERE " +
                "    a_boolean IS NULL " +
                "AND an_integer IS NULL " +
                "AND a_bigint IS NULL " +
                "AND a_real IS NULL " +
                "AND a_double IS NULL " +
                "AND a_short_decimal IS NULL " +
                "AND a_long_decimal IS NULL " +
                "AND a_varchar IS NULL " +
                "AND a_varbinary IS NULL " +
                "AND a_date IS NULL " +
                "AND a_time IS NULL " +
                "AND a_timestamp IS NULL " +
                "AND a_timestamptz IS NULL " +
                "AND a_uuid IS NULL " +
                "AND a_row IS NULL " +
                "AND an_array IS NULL " +
                "AND a_map IS NULL " +
                "AND \"a quoted, field\" IS NULL " +
                ""))
                .skippingTypesCheck()
                .matches(nullValues);

        // SHOW STATS
        switch (format) {
            case ORC -> {
                assertQuery("SHOW STATS FOR test_partitioned_table",
                        "VALUES " +
                                "  ('a_boolean', NULL, 1e0, 0.5, NULL, 'true', 'true'), " +
                                "  ('an_integer', NULL, 1e0, 0.5, NULL, '1', '1'), " +
                                "  ('a_bigint', NULL, 1e0, 0.5, NULL, '1', '1'), " +
                                "  ('a_real', NULL, 1e0, 0.5, NULL, '1.0', '1.0'), " +
                                "  ('a_double', NULL, 1e0, 0.5, NULL, '1.0', '1.0'), " +
                                "  ('a_short_decimal', NULL, 1e0, 0.5, NULL, '1.0', '1.0'), " +
                                "  ('a_long_decimal', NULL, 1e0, 0.5, NULL, '11.0', '11.0'), " +
                                "  ('a_varchar', NULL, 1e0, 0.5, NULL, NULL, NULL), " +
                                "  ('a_varbinary', NULL, 1e0, 0.5, NULL, NULL, NULL), " +
                                "  ('a_date', NULL, 1e0, 0.5, NULL, '2021-07-24', '2021-07-24'), " +
                                "  ('a_time', NULL, 1e0, 0.5, NULL, NULL, NULL), " +
                                "  ('a_timestamp', NULL, 1e0, 0.5, NULL, '2021-07-24 03:43:57.987654', '2021-07-24 03:43:57.987654'), " +
                                "  ('a_timestamptz', NULL, 1e0, 0.5, NULL, '2021-07-24 04:43:57.987 UTC', '2021-07-24 04:43:57.987 UTC'), " +
                                "  ('a_uuid', NULL, 1e0, 0.5, NULL, NULL, NULL), " +
                                "  ('a_row', NULL, NULL, 0.5, NULL, NULL, NULL), " +
                                "  ('an_array', NULL, NULL, 0.5, NULL, NULL, NULL), " +
                                "  ('a_map', NULL, NULL, 0.5, NULL, NULL, NULL), " +
                                "  ('a quoted, field', NULL, 1e0, 0.5, NULL, NULL, NULL), " +
                                "  (NULL, NULL, NULL, NULL, 2e0, NULL, NULL)");
            }
            case PARQUET -> {
                assertThat(query("SHOW STATS FOR test_partitioned_table"))
                        .skippingTypesCheck()
                        .matches("VALUES " +
                                "  ('a_boolean', NULL, 1e0, 0.5e0, NULL, 'true', 'true'), " +
                                "  ('an_integer', NULL, 1e0, 0.5e0, NULL, '1', '1'), " +
                                "  ('a_bigint', NULL, 1e0, 0.5e0, NULL, '1', '1'), " +
                                "  ('a_real', NULL, 1e0, 0.5e0, NULL, '1.0', '1.0'), " +
                                "  ('a_double', NULL, 1e0, 0.5e0, NULL, '1.0', '1.0'), " +
                                "  ('a_short_decimal', NULL, 1e0, 0.5e0, NULL, '1.0', '1.0'), " +
                                "  ('a_long_decimal', NULL, 1e0, 0.5e0, NULL, '11.0', '11.0'), " +
                                "  ('a_varchar', 213e0, 1e0, 0.5e0, NULL, NULL, NULL), " +
                                "  ('a_varbinary', 103e0, 1e0, 0.5e0, NULL, NULL, NULL), " +
                                "  ('a_date', NULL, 1e0, 0.5e0, NULL, '2021-07-24', '2021-07-24'), " +
                                "  ('a_time', NULL, 1e0, 0.5e0, NULL, NULL, NULL), " +
                                "  ('a_timestamp', NULL, 1e0, 0.5e0, NULL, '2021-07-24 03:43:57.987654', '2021-07-24 03:43:57.987654'), " +
                                "  ('a_timestamptz', NULL, 1e0, 0.5e0, NULL, '2021-07-24 04:43:57.987 UTC', '2021-07-24 04:43:57.987 UTC'), " +
                                "  ('a_uuid', NULL, 1e0, 0.5e0, NULL, NULL, NULL), " +
                                "  ('a_row', NULL, NULL, NULL, NULL, NULL, NULL), " +
                                "  ('an_array', NULL, NULL, NULL, NULL, NULL, NULL), " +
                                "  ('a_map', NULL, NULL, NULL, NULL, NULL, NULL), " +
                                "  ('a quoted, field', 202e0, 1e0, 0.5e0, NULL, NULL, NULL), " +
                                "  (NULL, NULL, NULL, NULL, 2e0, NULL, NULL)");
            }
            case AVRO -> {
                assertThat(query("SHOW STATS FOR test_partitioned_table"))
                        .skippingTypesCheck()
                        .matches("VALUES " +
                                "  ('a_boolean', NULL, 1e0, 0.5e0, NULL, 'true', 'true'), " +
                                "  ('an_integer', NULL, 1e0, 0.5e0, NULL, '1', '1'), " +
                                "  ('a_bigint', NULL, 1e0, 0.5e0, NULL, '1', '1'), " +
                                "  ('a_real', NULL, 1e0, 0.5e0, NULL, '1.0', '1.0'), " +
                                "  ('a_double', NULL, 1e0, 0.5e0, NULL, '1.0', '1.0'), " +
                                "  ('a_short_decimal', NULL, 1e0, 0.5e0, NULL, '1.0', '1.0'), " +
                                "  ('a_long_decimal', NULL, 1e0, 0.5e0, NULL, '11.0', '11.0'), " +
                                "  ('a_varchar', NULL, 1e0, 0.5e0, NULL, NULL, NULL), " +
                                "  ('a_varbinary', NULL, 1e0, 0.5e0, NULL, NULL, NULL), " +
                                "  ('a_date', NULL, 1e0, 0.5e0, NULL, '2021-07-24', '2021-07-24'), " +
                                "  ('a_time', NULL, 1e0, 0.5e0, NULL, NULL, NULL), " +
                                "  ('a_timestamp', NULL, 1e0, 0.5e0, NULL, '2021-07-24 03:43:57.987654', '2021-07-24 03:43:57.987654'), " +
                                "  ('a_timestamptz', NULL, 1e0, 0.5e0, NULL, '2021-07-24 04:43:57.987 UTC', '2021-07-24 04:43:57.987 UTC'), " +
                                "  ('a_uuid', NULL, 1e0, 0.5e0, NULL, NULL, NULL), " +
                                "  ('a_row', NULL, NULL, NULL, NULL, NULL, NULL), " +
                                "  ('an_array', NULL, NULL, NULL, NULL, NULL, NULL), " +
                                "  ('a_map', NULL, NULL, NULL, NULL, NULL, NULL), " +
                                "  ('a quoted, field', NULL, 1e0, 0.5e0, NULL, NULL, NULL), " +
                                "  (NULL, NULL, NULL, NULL, 2e0, NULL, NULL)");
            }
        }

        // $partitions
        String schema = getSession().getSchema().orElseThrow();
        assertThat(query("SELECT column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name = 'test_partitioned_table$partitions' "))
                .skippingTypesCheck()
                .matches("VALUES 'partition', 'record_count', 'file_count', 'total_size'");
        assertThat(query("SELECT " +
                "  record_count," +
                "  file_count, " +
                "  partition.a_boolean, " +
                "  partition.an_integer, " +
                "  partition.a_bigint, " +
                "  partition.a_real, " +
                "  partition.a_double, " +
                "  partition.a_short_decimal, " +
                "  partition.a_long_decimal, " +
                "  partition.a_varchar, " +
                "  partition.a_varbinary, " +
                "  partition.a_date, " +
                "  partition.a_time, " +
                "  partition.a_timestamp, " +
                "  partition.a_timestamptz, " +
                "  partition.a_uuid, " +
                "  partition.\"a quoted, field\" " +
                // Note: partitioning on non-primitive columns is not allowed in Iceberg
                " FROM \"test_partitioned_table$partitions\" "))
                .matches("" +
                        "VALUES (" +
                        "  BIGINT '1', " +
                        "  BIGINT '1', " +
                        "  true, " +
                        "  1, " +
                        "  BIGINT '1', " +
                        "  REAL '1.0', " +
                        "  DOUBLE '1.0', " +
                        "  CAST(1.0 AS decimal(5,2)), " +
                        "  CAST(11.0 AS decimal(38,20)), " +
                        "  VARCHAR 'onefsadfdsf', " +
                        "  X'000102f0feff', " +
                        "  DATE '2021-07-24'," +
                        "  TIME '02:43:57.987654', " +
                        "  TIMESTAMP '2021-07-24 03:43:57.987654'," +
                        "  TIMESTAMP '2021-07-24 04:43:57.987654 UTC', " +
                        "  UUID '20050910-1330-11e9-ffff-2a86e4085a59', " +
                        "  VARCHAR 'tralala' " +
                        ")" +
                        "UNION ALL " +
                        "VALUES (" +
                        "  BIGINT '1', " +
                        "  BIGINT '1', " +
                        "  NULL, " +
                        "  NULL, " +
                        "  NULL, " +
                        "  NULL, " +
                        "  NULL, " +
                        "  NULL, " +
                        "  NULL, " +
                        "  NULL, " +
                        "  NULL, " +
                        "  NULL, " +
                        "  NULL, " +
                        "  NULL, " +
                        "  NULL, " +
                        "  NULL, " +
                        "  NULL  " +
                        ")");

        assertUpdate("DROP TABLE test_partitioned_table");
    }

    @Test
    public void testCreatePartitionedTableWithNestedTypes()
    {
        assertUpdate("" +
                "CREATE TABLE test_partitioned_table_nested_type (" +
                "  _string VARCHAR" +
                ", _struct ROW(_field1 INT, _field2 VARCHAR)" +
                ", _date DATE" +
                ") " +
                "WITH (" +
                "  partitioning = ARRAY['_date']" +
                ")");

        assertUpdate("DROP TABLE test_partitioned_table_nested_type");
    }

    @Test
    public void testCreateTableWithUnsupportedNestedFieldPartitioning()
    {
        assertQueryFails(
                "CREATE TABLE test_partitioned_table_nested_field_3 (grandparent ROW(parent ROW(child VARCHAR))) WITH (partitioning = ARRAY['\"grandparent.parent\"'])",
                "\\QUnable to parse partitioning value: Cannot partition by non-primitive source field: struct<3: child: optional string>");
        assertQueryFails(
                "CREATE TABLE test_partitioned_table_nested_field_inside_array (parent ARRAY(ROW(child VARCHAR))) WITH (partitioning = ARRAY['\"parent.child\"'])",
                "\\QPartitioning field [parent.element.child] cannot be contained in a array");
        assertQueryFails(
                "CREATE TABLE test_partitioned_table_nested_field_inside_map (parent MAP(ROW(child INTEGER), ARRAY(VARCHAR))) WITH (partitioning = ARRAY['\"parent.key.child\"'])",
                "\\QPartitioning field [parent.key.child] cannot be contained in a map");
        assertQueryFails(
                "CREATE TABLE test_partitioned_table_nested_field_year_transform_in_string (parent ROW(child VARCHAR)) WITH (partitioning = ARRAY['year(\"parent.child\")'])",
                "\\QUnable to parse partitioning value: Invalid source type string for transform: year");
    }

    @Test
    public void testNestedFieldPartitionedTable()
    {
        String tableName = "test_nested_field_partitioned_table_" + randomNameSuffix();
        assertQuerySucceeds("CREATE TABLE " + tableName + "(id INTEGER, name VARCHAR, parent ROW(child VARCHAR, child2 VARCHAR))" +
                " WITH (partitioning = ARRAY['id', '\"parent.child\"', '\"parent.child2\"'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'presto', ROW('a', 'b'))", 1);

        assertThat(query("SELECT id, name, parent.child, parent.child2 FROM " + tableName))
                .skippingTypesCheck()
                .matches("VALUES (1, 'presto', 'a', 'b')");

        assertUpdate("UPDATE " + tableName + " SET name = 'trino' WHERE parent.child = 'a'", 1);
        assertQuerySucceeds("DELETE FROM " + tableName);
        assertThat(query("SELECT * FROM " + tableName))
                .returnsEmptyResult();
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'trino', ROW('a', 'b'))", 1);

        assertThat(query("SELECT id, name, parent.child, parent.child2 FROM " + tableName))
                .skippingTypesCheck()
                .matches("VALUES (1, 'trino', 'a', 'b')");

        String newTableName = "test_nested_field_partitioned_table_" + randomNameSuffix();
        assertQuerySucceeds("ALTER TABLE " + tableName + " RENAME TO " + newTableName);

        assertQuerySucceeds(withSingleWriterPerTask(getSession()), "ALTER TABLE " + newTableName + " EXECUTE OPTIMIZE");
        assertQuerySucceeds(prepareCleanUpSession(), "ALTER TABLE " + newTableName + " EXECUTE expire_snapshots(retention_threshold => '0s')");

        assertThat(query("SELECT id, name, parent.child, parent.child2 FROM " + newTableName))
                .skippingTypesCheck()
                .matches("VALUES (1, 'trino', 'a', 'b')");

        assertUpdate("DROP TABLE " + newTableName);
    }

    @Test
    public void testMultipleLevelNestedFieldPartitionedTable()
    {
        String tableName = "test_multiple_level_nested_field_partitioned_table_" + randomNameSuffix();
        assertQuerySucceeds("CREATE TABLE " + tableName + "(id INTEGER, gradparent ROW(parent ROW(child VARCHAR)))" +
                " WITH (partitioning = ARRAY['\"gradparent.parent.child\"'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, ROW(ROW('trino')))", 1);

        assertThat(query("SELECT id, gradparent.parent.child FROM " + tableName))
                .skippingTypesCheck()
                .matches("VALUES (1, 'trino')");

        assertUpdate("UPDATE " + tableName + " SET id = 2 WHERE gradparent.parent.child = 'trino'", 1);
        assertQuerySucceeds("DELETE FROM " + tableName);
        assertThat(query("SELECT * FROM " + tableName))
                .returnsEmptyResult();
        assertUpdate("INSERT INTO " + tableName + " VALUES (3, ROW(ROW('trino')))", 1);

        assertThat(query("SELECT id, gradparent.parent.child FROM " + tableName))
                .skippingTypesCheck()
                .matches("VALUES (3, 'trino')");

        String newTableName = "test_multiple_level_nested_field_partitioned_table_" + randomNameSuffix();
        assertQuerySucceeds("ALTER TABLE " + tableName + " RENAME TO " + newTableName);

        assertQuerySucceeds(withSingleWriterPerTask(getSession()), "ALTER TABLE " + newTableName + " EXECUTE OPTIMIZE");
        assertQuerySucceeds(prepareCleanUpSession(), "ALTER TABLE " + newTableName + " EXECUTE expire_snapshots(retention_threshold => '0s')");

        assertThat(query("SELECT id, gradparent.parent.child FROM " + newTableName))
                .skippingTypesCheck()
                .matches("VALUES (3, 'trino')");

        assertUpdate("DROP TABLE " + newTableName);
    }

    @Test
    public void testNestedFieldPartitionedTableHavingSameChildName()
    {
        String tableName = "test_nested_field_partitioned_table_having_same_child_name_" + randomNameSuffix();
        assertQuerySucceeds("CREATE TABLE " + tableName + "(id INTEGER, gradparent ROW(parent ROW(child VARCHAR)), parent ROW(child VARCHAR))" +
                " WITH (partitioning = ARRAY['\"gradparent.parent.child\"'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, ROW(ROW('trino')), ROW('trinodb'))", 1);

        assertThat(query("SELECT id, gradparent.parent.child, parent.child FROM " + tableName))
                .skippingTypesCheck()
                .matches("VALUES (1, 'trino', 'trinodb')");

        assertUpdate("UPDATE " + tableName + " SET id = 2 WHERE gradparent.parent.child = 'trino'", 1);
        assertQuerySucceeds("DELETE FROM " + tableName);
        assertThat(query("SELECT * FROM " + tableName))
                .returnsEmptyResult();
        assertUpdate("INSERT INTO " + tableName + " VALUES (3, ROW(ROW('trino')), ROW('trinodb'))", 1);

        assertThat(query("SELECT id, gradparent.parent.child, parent.child FROM " + tableName))
                .skippingTypesCheck()
                .matches("VALUES (3, 'trino', 'trinodb')");

        String newTableName = "test_nested_field_partitioned_table_having_same_child_name_" + randomNameSuffix();
        assertQuerySucceeds("ALTER TABLE " + tableName + " RENAME TO " + newTableName);

        assertQuerySucceeds(withSingleWriterPerTask(getSession()), "ALTER TABLE " + newTableName + " EXECUTE OPTIMIZE");
        assertQuerySucceeds(prepareCleanUpSession(), "ALTER TABLE " + newTableName + " EXECUTE expire_snapshots(retention_threshold => '0s')");

        assertThat(query("SELECT id, gradparent.parent.child, parent.child FROM " + newTableName))
                .skippingTypesCheck()
                .matches("VALUES (3, 'trino', 'trinodb')");

        assertUpdate("DROP TABLE " + newTableName);
    }

    @Test
    public void testMergeWithNestedFieldPartitionedTable()
    {
        String sourceTable = "test_merge_with_nested_field_partitioned_table_source_" + randomNameSuffix();
        String targetTable = "test_merge_with_nested_field_partitioned_table_target_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + sourceTable + " (customer VARCHAR, purchases INT, address ROW (city VARCHAR))" +
                " WITH (partitioning = ARRAY['\"address.city\"'])");
        assertUpdate(
                "INSERT INTO " + sourceTable + " (customer, purchases, address)" +
                        " VALUES ('Aaron', 6, ROW('Arches')), ('Ed', 7, ROW('Etherville')), ('Carol', 9, ROW('Centreville')), ('Dave', 11, ROW('Darbyshire'))",
                4);

        assertUpdate("CREATE TABLE " + targetTable + " (customer VARCHAR, purchases INT, address ROW (city VARCHAR))" +
                " WITH (partitioning = ARRAY['\"address.city\"'])");
        assertUpdate(
                "INSERT INTO " + targetTable + " (customer, purchases, address) " +
                        " VALUES ('Aaron', 5, ROW('Antioch')), ('Bill', 7, ROW('Buena')), ('Carol', 3, ROW('Cambridge')), ('Dave', 11, ROW('Devon'))",
                4);

        String sql = "MERGE INTO " + targetTable + " t USING " + sourceTable + " s ON (t.customer = s.customer)" +
                "    WHEN MATCHED AND s.address.city = 'Centreville' THEN DELETE" +
                "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases" +
                "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES (s.customer, s.purchases, s.address)";

        assertUpdate(sql, 4);

        assertQuery(
                "SELECT customer, purchases, address.city FROM " + targetTable,
                "VALUES ('Aaron', 11, 'Antioch'), ('Ed', 7, 'Etherville'), ('Bill', 7, 'Buena'), ('Dave', 22, 'Devon')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testSchemaEvolutionWithNestedFieldPartitioning()
    {
        String tableName = "test_schema_evolution_with_nested_field_partitioning_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (c1 bigint, parent1 ROW(child VARCHAR), parent2 ROW(child VARCHAR)) WITH (partitioning = ARRAY['\"parent1.child\"'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, ROW('BLR'), ROW('BLR'))", 1);
        assertQuery("SELECT c1, parent1.child, parent2.child from " + tableName, "VALUES (1, 'BLR', 'BLR')");

        // Drop end column
        assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN parent2");
        assertQuery("SELECT c1, parent1.child FROM " + tableName, "VALUES (1, 'BLR')");

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN parent3 ROW(child VARCHAR)");
        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN parent4 ROW(child VARCHAR)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, ROW('DEL'), ROW('DL'), ROW('IN'))", 1);
        assertQuery("SELECT c1, parent1.child, parent3.child, parent4.child FROM " + tableName, "VALUES (1, 'BLR', NULL, NULL), (2, 'DEL', 'DL', 'IN')");

        // Drop a column (parent3) from middle of table
        assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN parent3");
        assertQuery("SELECT c1, parent1.child, parent4.child FROM " + tableName, "VALUES (1, 'BLR', NULL), (2, 'DEL', 'IN')");

        // Rename nested column
        assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN parent4 TO renamed_parent");

        // Rename nested partitioned column
        assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN parent1 TO renamed_partitioned_parent");

        assertQuery("SHOW COLUMNS FROM " + tableName, "VALUES " +
                "('c1', 'bigint', '', ''), " +
                "('renamed_partitioned_parent', 'row(child varchar)', '', ''), " +
                "('renamed_parent', 'row(child varchar)', '', '')");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreatePartitionedTableAs()
    {
        File tempDir = getDistributedQueryRunner().getCoordinator().getBaseDataDir().toFile();
        String tempDirPath = tempDir.toURI().toASCIIString() + randomNameSuffix();
        assertUpdate(
                "CREATE TABLE test_create_partitioned_table_as " +
                        "WITH (" +
                        "format_version = 2," +
                        "location = '" + tempDirPath + "', " +
                        "partitioning = ARRAY['ORDER_STATUS', 'Ship_Priority', 'Bucket(\"order key\",9)']" +
                        ") " +
                        "AS " +
                        "SELECT orderkey AS \"order key\", shippriority AS ship_priority, orderstatus AS order_status " +
                        "FROM tpch.tiny.orders",
                "SELECT count(*) from orders");

        assertThat(computeScalar("SHOW CREATE TABLE test_create_partitioned_table_as")).isEqualTo(format(
                "CREATE TABLE %s.%s.%s (\n" +
                        "   \"order key\" bigint,\n" +
                        "   ship_priority integer,\n" +
                        "   order_status varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = '%s',\n" +
                        "   format_version = 2,\n" +
                        "   location = '%s',\n" +
                        "   partitioning = ARRAY['order_status','ship_priority','bucket(\"order key\", 9)']\n" +
                        ")",
                getSession().getCatalog().orElseThrow(),
                getSession().getSchema().orElseThrow(),
                "test_create_partitioned_table_as",
                format,
                tempDirPath));

        assertQuery("SELECT * from test_create_partitioned_table_as", "SELECT orderkey, shippriority, orderstatus FROM orders");

        assertUpdate("DROP TABLE test_create_partitioned_table_as");
    }

    @Test
    public void testCreatePartitionedTableWithQuotedIdentifierCasing()
    {
        testCreatePartitionedTableWithQuotedIdentifierCasing("x", "x", true);
        testCreatePartitionedTableWithQuotedIdentifierCasing("X", "x", true);
        testCreatePartitionedTableWithQuotedIdentifierCasing("\"x\"", "x", true);
        testCreatePartitionedTableWithQuotedIdentifierCasing("\"X\"", "x", true);
        testCreatePartitionedTableWithQuotedIdentifierCasing("x", "\"x\"", true);
        testCreatePartitionedTableWithQuotedIdentifierCasing("X", "\"x\"", true);
        testCreatePartitionedTableWithQuotedIdentifierCasing("\"x\"", "\"x\"", true);
        testCreatePartitionedTableWithQuotedIdentifierCasing("\"X\"", "\"x\"", true);
        testCreatePartitionedTableWithQuotedIdentifierCasing("x", "X", true);
        testCreatePartitionedTableWithQuotedIdentifierCasing("X", "X", true);
        testCreatePartitionedTableWithQuotedIdentifierCasing("\"x\"", "X", true);
        testCreatePartitionedTableWithQuotedIdentifierCasing("\"X\"", "X", true);
        testCreatePartitionedTableWithQuotedIdentifierCasing("x", "\"X\"", false);
        testCreatePartitionedTableWithQuotedIdentifierCasing("X", "\"X\"", false);
        testCreatePartitionedTableWithQuotedIdentifierCasing("\"x\"", "\"X\"", false);
        testCreatePartitionedTableWithQuotedIdentifierCasing("\"X\"", "\"X\"", false);
    }

    private void testCreatePartitionedTableWithQuotedIdentifierCasing(String columnName, String partitioningField, boolean success)
    {
        String tableName = "partitioning_" + randomNameSuffix();
        @Language("SQL") String sql = format("CREATE TABLE %s (%s bigint) WITH (partitioning = ARRAY['%s'])", tableName, columnName, partitioningField);
        if (success) {
            assertUpdate(sql);
            assertUpdate("DROP TABLE " + tableName);
        }
        else {
            assertQueryFails(sql, "Unable to parse partitioning value: .*");
        }
    }

    @Test
    public void testPartitionColumnNameConflict()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_conflict_partition", "(ts timestamp, ts_day int) WITH (partitioning = ARRAY['day(ts)'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (TIMESTAMP '2021-07-24 03:43:57.987654', 1)", 1);

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (TIMESTAMP '2021-07-24 03:43:57.987654', 1)");
            assertThat(query("SELECT partition.ts_day_2 FROM \"" + table.getName() + "$partitions\""))
                    .matches("VALUES DATE '2021-07-24'");
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_conflict_partition", "(ts timestamp, ts_day int)")) {
            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES partitioning = ARRAY['day(ts)']");
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (TIMESTAMP '2021-07-24 03:43:57.987654', 1)", 1);

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (TIMESTAMP '2021-07-24 03:43:57.987654', 1)");
            assertThat(query("SELECT partition.ts_day_2 FROM \"" + table.getName() + "$partitions\""))
                    .matches("VALUES DATE '2021-07-24'");
        }
    }

    @Test
    public void testSortByAllTypes()
    {
        String tableName = "test_sort_by_all_types_" + randomNameSuffix();
        assertUpdate("" +
                "CREATE TABLE " + tableName + " (" +
                "  a_boolean boolean, " +
                "  an_integer integer, " +
                "  a_bigint bigint, " +
                "  a_real real, " +
                "  a_double double, " +
                "  a_short_decimal decimal(5,2), " +
                "  a_long_decimal decimal(38,20), " +
                "  a_varchar varchar, " +
                "  a_varbinary varbinary, " +
                "  a_date date, " +
                "  a_time time(6), " +
                "  a_timestamp timestamp(6), " +
                "  a_timestamptz timestamp(6) with time zone, " +
                "  a_uuid uuid, " +
                "  a_row row(id integer, vc varchar, t time(6), ts timestamp(6), tstz timestamp(6) with time zone), " + // not sorted on, but still written to sort temp file, if any
                "  an_array array(varchar), " +
                "  a_map map(integer, varchar) " +
                ") " +
                "WITH (" +
                "sorted_by = ARRAY[" +
                "  'a_boolean', " +
                "  'an_integer', " +
                "  'a_bigint', " +
                "  'a_real', " +
                "  'a_double', " +
                "  'a_short_decimal', " +
                "  'a_long_decimal', " +
                "  'a_varchar', " +
                "  'a_varbinary', " +
                "  'a_date', " +
                "  'a_time', " +
                "  'a_timestamp', " +
                "  'a_timestamptz', " +
                "  'a_uuid'" +
                "  ]" +
                ")");
        String values = "(" +
                "true, " +
                "1, " +
                "BIGINT '2', " +
                "REAL '3.0', " +
                "DOUBLE '4.0', " +
                "DECIMAL '5.00', " +
                "CAST(DECIMAL '6.00' AS decimal(38,20)), " +
                "VARCHAR 'seven', " +
                "X'88888888', " +
                "DATE '2022-09-09', " +
                "TIME '10:10:10.000000', " +
                "TIMESTAMP '2022-11-11 11:11:11.000000', " +
                "TIMESTAMP '2022-11-11 11:11:11.000000 UTC', " +
                "UUID '12121212-1212-1212-1212-121212121212', " +
                "CAST(ROW(13, 'thirteen', TIME '10:10:10.000000', TIMESTAMP '2022-11-11 11:11:11.000000', TIMESTAMP '2022-11-11 11:11:11.000000 UTC') AS row(id integer, vc varchar, t time(6), ts timestamp(6), tstz timestamp(6) with time zone)), " +
                "ARRAY[VARCHAR 'four', 'teen'], " +
                "MAP(ARRAY[15], ARRAY[VARCHAR 'fifteen']))";
        String highValues = "(" +
                "true, " +
                "999999999, " +
                "BIGINT '999999999', " +
                "REAL '999.999', " +
                "DOUBLE '999.999', " +
                "DECIMAL '999.99', " +
                "DECIMAL '6.00', " +
                "'zzzzzzzzzzzzzz', " +
                "X'FFFFFFFF', " +
                "DATE '2099-12-31', " +
                "TIME '23:59:59.999999', " +
                "TIMESTAMP '2099-12-31 23:59:59.000000', " +
                "TIMESTAMP '2099-12-31 23:59:59.000000 UTC', " +
                "UUID 'FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF', " +
                "CAST(ROW(999, 'zzzzzzzz', TIME '23:59:59.999999', TIMESTAMP '2099-12-31 23:59:59.000000', TIMESTAMP '2099-12-31 23:59:59.000000 UTC') AS row(id integer, vc varchar, t time(6), ts timestamp(6), tstz timestamp(6) with time zone)), " +
                "ARRAY['zzzz', 'zzzz'], " +
                "MAP(ARRAY[999], ARRAY['zzzz']))";
        String lowValues = "(" +
                "false, " +
                "0, " +
                "BIGINT '0', " +
                "REAL '0', " +
                "DOUBLE '0', " +
                "DECIMAL '0', " +
                "DECIMAL '0', " +
                "'', " +
                "X'00000000', " +
                "DATE '2000-01-01', " +
                "TIME '00:00:00.000000', " +
                "TIMESTAMP '2000-01-01 00:00:00.000000', " +
                "TIMESTAMP '2000-01-01 00:00:00.000000 UTC', " +
                "UUID '00000000-0000-0000-0000-000000000000', " +
                "CAST(ROW(0, '', TIME '00:00:00.000000', TIMESTAMP '2000-01-01 00:00:00.000000', TIMESTAMP '2000-01-01 00:00:00.000000 UTC') AS row(id integer, vc varchar, t time(6), ts timestamp(6), tstz timestamp(6) with time zone)), " +
                "ARRAY['', ''], " +
                "MAP(ARRAY[0], ARRAY['']))";

        assertUpdate("INSERT INTO " + tableName + " VALUES " + values + ", " + highValues + ", " + lowValues, 3);
        assertThat(query("TABLE " + tableName))
                .matches("VALUES " + values + ", " + highValues + ", " + lowValues);

        // Insert "large" number of rows, supposedly topping over iceberg.writer-sort-buffer-size so that temporary files are utilized by the sorting writer.
        assertUpdate(
                """
                INSERT INTO %s
                SELECT v.*
                FROM (VALUES %s, %s, %s) v
                CROSS JOIN UNNEST (sequence(1, 10_000)) a(i)
                """.formatted(tableName, values, highValues, lowValues), 30000);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testEmptySortedByList()
    {
        String tableName = "test_empty_sorted_by_list_" + randomNameSuffix();
        assertUpdate("" +
                "CREATE TABLE " + tableName + " (a_boolean boolean, an_integer integer) " +
                "  WITH (partitioning = ARRAY['an_integer'], sorted_by = ARRAY[])");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateSortedTableWithQuotedIdentifierCasing()
    {
        testCreateSortedTableWithQuotedIdentifierCasing("col", "col");
        testCreateSortedTableWithQuotedIdentifierCasing("COL", "col");
        testCreateSortedTableWithQuotedIdentifierCasing("\"col\"", "col");
        testCreateSortedTableWithQuotedIdentifierCasing("\"COL\"", "col");
        testCreateSortedTableWithQuotedIdentifierCasing("col", "\"col\"");
        testCreateSortedTableWithQuotedIdentifierCasing("COL", "\"col\"");
        testCreateSortedTableWithQuotedIdentifierCasing("\"col\"", "\"col\"");
        testCreateSortedTableWithQuotedIdentifierCasing("\"COL\"", "\"col\"");
    }

    private void testCreateSortedTableWithQuotedIdentifierCasing(String columnName, String sortField)
    {
        String tableName = "test_create_sorted_table_with_quotes_" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (%s bigint) WITH (sorted_by = ARRAY['%s'])", tableName, columnName, sortField));
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateSortedTableWithSortTransform()
    {
        testCreateSortedTableWithSortTransform("col", "bucket(col, 3)");
        testCreateSortedTableWithSortTransform("col", "bucket(\"col\", 3)");
        testCreateSortedTableWithSortTransform("col", "truncate(col, 3)");
        testCreateSortedTableWithSortTransform("col", "year(col)");
        testCreateSortedTableWithSortTransform("col", "month(col)");
        testCreateSortedTableWithSortTransform("col", "date(col)");
        testCreateSortedTableWithSortTransform("col", "hour(col)");
    }

    private void testCreateSortedTableWithSortTransform(String columnName, String sortField)
    {
        String tableName = "test_sort_with_transform_" + randomNameSuffix();
        assertThat(query(format("CREATE TABLE %s (%s TIMESTAMP(6)) WITH (sorted_by = ARRAY['%s'])", tableName, columnName, sortField)))
                .failure().hasMessageContaining("Unable to parse sort field");
    }

    @Test
    public void testSortOrderChange()
    {
        Session withSmallRowGroups = withSmallRowGroups(getSession());
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_sort_order_change",
                "WITH (sorted_by = ARRAY['comment']) AS SELECT * FROM nation WITH NO DATA")) {
            assertUpdate(withSmallRowGroups, "INSERT INTO " + table.getName() + " SELECT * FROM nation", 25);
            Set<String> sortedByComment = new HashSet<>();
            computeActual("SELECT file_path from \"" + table.getName() + "$files\"").getOnlyColumnAsSet()
                    .forEach(fileName -> sortedByComment.add((String) fileName));

            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES sorted_by = ARRAY['name']");
            assertUpdate(withSmallRowGroups, "INSERT INTO " + table.getName() + " SELECT * FROM nation", 25);

            for (Object filePath : computeActual("SELECT file_path from \"" + table.getName() + "$files\"").getOnlyColumnAsSet()) {
                String path = (String) filePath;
                if (sortedByComment.contains(path)) {
                    assertThat(isFileSorted(path, "comment")).isTrue();
                }
                else {
                    assertThat(isFileSorted(path, "name")).isTrue();
                }
            }
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM nation UNION ALL SELECT * FROM nation");
        }
    }

    @Test
    public void testSortingDisabled()
    {
        Session withSortingDisabled = Session.builder(withSmallRowGroups(getSession()))
                .setCatalogSessionProperty(ICEBERG_CATALOG, "sorted_writing_enabled", "false")
                .build();
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_sorting_disabled",
                "WITH (sorted_by = ARRAY['comment']) AS SELECT * FROM nation WITH NO DATA")) {
            assertUpdate(withSortingDisabled, "INSERT INTO " + table.getName() + " SELECT * FROM nation", 25);
            for (Object filePath : computeActual("SELECT file_path from \"" + table.getName() + "$files\"").getOnlyColumnAsSet()) {
                assertThat(isFileSorted((String) filePath, "comment")).isFalse();
            }
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM nation");
        }
    }

    @Test
    public void testOptimizeWithSortOrder()
    {
        Session withSmallRowGroups = withSmallRowGroups(getSession());
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_optimize_with_sort_order",
                "WITH (sorted_by = ARRAY['comment']) AS SELECT * FROM nation WITH NO DATA")) {
            assertUpdate("INSERT INTO " + table.getName() + " SELECT * FROM nation WHERE nationkey < 10", 10);
            assertUpdate("INSERT INTO " + table.getName() + " SELECT * FROM nation WHERE nationkey >= 10 AND nationkey < 20", 10);
            assertUpdate("INSERT INTO " + table.getName() + " SELECT * FROM nation WHERE nationkey >= 20", 5);
            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES sorted_by = ARRAY['comment']");
            // For optimize we need to set task_min_writer_count to 1, otherwise it will create more than one file.
            assertUpdate(withSingleWriterPerTask(withSmallRowGroups), "ALTER TABLE " + table.getName() + " EXECUTE optimize");

            for (Object filePath : computeActual("SELECT file_path from \"" + table.getName() + "$files\"").getOnlyColumnAsSet()) {
                assertThat(isFileSorted((String) filePath, "comment")).isTrue();
            }
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM nation");
        }
    }

    @Test
    public void testUpdateWithSortOrder()
    {
        Session withSmallRowGroups = withSmallRowGroups(getSession());
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_sorted_update",
                "WITH (sorted_by = ARRAY['comment']) AS TABLE tpch.tiny.customer WITH NO DATA")) {
            assertUpdate(
                    withSmallRowGroups,
                    "INSERT INTO " + table.getName() + " TABLE tpch.tiny.customer",
                    "VALUES 1500");
            assertUpdate(withSmallRowGroups, "UPDATE " + table.getName() + " SET comment = substring(comment, 2)", 1500);
            assertQuery(
                    "SELECT custkey, name, address, nationkey, phone, acctbal, mktsegment, comment FROM " + table.getName(),
                    "SELECT custkey, name, address, nationkey, phone, acctbal, mktsegment, substring(comment, 2) FROM customer");
            for (Object filePath : computeActual("SELECT file_path from \"" + table.getName() + "$files\" WHERE content != 1").getOnlyColumnAsSet()) {
                assertThat(isFileSorted((String) filePath, "comment")).isTrue();
            }
        }
    }

    protected abstract boolean isFileSorted(String path, String sortColumnName);

    @Test
    public void testSortingOnNestedField()
    {
        String tableName = "test_sorting_on_nested_field" + randomNameSuffix();
        assertThat(query("CREATE TABLE " + tableName + " (nationkey BIGINT, row_t ROW(name VARCHAR, regionkey BIGINT, comment VARCHAR)) " +
                "WITH (sorted_by = ARRAY['row_t.comment'])"))
                .failure().hasMessageContaining("Unable to parse sort field: [row_t.comment]");
        assertThat(query("CREATE TABLE " + tableName + " (nationkey BIGINT, row_t ROW(name VARCHAR, regionkey BIGINT, comment VARCHAR)) " +
                "WITH (sorted_by = ARRAY['\"row_t\".\"comment\"'])"))
                .failure().hasMessageContaining("Unable to parse sort field: [\"row_t\".\"comment\"]");
        assertThat(query("CREATE TABLE " + tableName + " (nationkey BIGINT, row_t ROW(name VARCHAR, regionkey BIGINT, comment VARCHAR)) " +
                "WITH (sorted_by = ARRAY['\"row_t.comment\"'])"))
                .failure().hasMessageContaining("Column not found: row_t.comment");
    }

    @Test
    public void testDroppingSortColumn()
    {
        Session withSmallRowGroups = withSmallRowGroups(getSession());
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_dropping_sort_column",
                "WITH (sorted_by = ARRAY['comment']) AS SELECT * FROM nation WITH NO DATA")) {
            assertUpdate(withSmallRowGroups, "INSERT INTO " + table.getName() + " SELECT * FROM nation", 25);
            assertThat(query("ALTER TABLE " + table.getName() + " DROP COLUMN comment"))
                    .failure().hasMessageContaining("Cannot find source column for sort field");
        }
    }

    @Test
    public void testTableComments()
    {
        File tempDir = getDistributedQueryRunner().getCoordinator().getBaseDataDir().toFile();
        String tempDirPath = tempDir.toURI().toASCIIString() + randomNameSuffix();
        String createTableTemplate = "" +
                "CREATE TABLE iceberg.tpch.test_table_comments (\n" +
                "   _x bigint\n" +
                ")\n" +
                "COMMENT '%s'\n" +
                "WITH (\n" +
                format("   format = '%s',\n", format) +
                "   format_version = 2,\n" +
                format("   location = '%s'\n", tempDirPath) +
                ")";
        String createTableWithoutComment = "" +
                "CREATE TABLE iceberg.tpch.test_table_comments (\n" +
                "   _x bigint\n" +
                ")\n" +
                "WITH (\n" +
                "   format = '" + format + "',\n" +
                "   format_version = 2,\n" +
                "   location = '" + tempDirPath + "'\n" +
                ")";
        String createTableSql = format(createTableTemplate, "test table comment", format);
        assertUpdate(createTableSql);
        assertThat(computeScalar("SHOW CREATE TABLE test_table_comments")).isEqualTo(createTableSql);

        assertUpdate("COMMENT ON TABLE test_table_comments IS 'different test table comment'");
        assertThat(computeScalar("SHOW CREATE TABLE test_table_comments")).isEqualTo(format(createTableTemplate, "different test table comment", format));

        assertUpdate("COMMENT ON TABLE test_table_comments IS NULL");
        assertThat(computeScalar("SHOW CREATE TABLE test_table_comments")).isEqualTo(createTableWithoutComment);
        assertUpdate("DROP TABLE iceberg.tpch.test_table_comments");

        assertUpdate(createTableWithoutComment);
        assertThat(computeScalar("SHOW CREATE TABLE test_table_comments")).isEqualTo(createTableWithoutComment);

        assertUpdate("DROP TABLE iceberg.tpch.test_table_comments");
    }

    @Test
    public void testRollbackSnapshot()
    {
        assertUpdate("CREATE TABLE test_rollback (col0 INTEGER, col1 BIGINT)");
        long afterCreateTableId = getCurrentSnapshotId("test_rollback");

        assertUpdate("INSERT INTO test_rollback (col0, col1) VALUES (123, CAST(987 AS BIGINT))", 1);
        long afterFirstInsertId = getCurrentSnapshotId("test_rollback");
        assertQuery("SELECT * FROM test_rollback ORDER BY col0", "VALUES (123, CAST(987 AS BIGINT))");

        // Check that rollback_to_snapshot can be executed also when it does not do any changes
        assertUpdate(format("CALL system.rollback_to_snapshot('tpch', 'test_rollback', %s)", afterFirstInsertId));
        assertQuery("SELECT * FROM test_rollback ORDER BY col0", "VALUES (123, CAST(987 AS BIGINT))");

        assertUpdate("INSERT INTO test_rollback (col0, col1) VALUES (456, CAST(654 AS BIGINT))", 1);
        assertQuery("SELECT * FROM test_rollback ORDER BY col0", "VALUES (123, CAST(987 AS BIGINT)), (456, CAST(654 AS BIGINT))");

        assertUpdate(format("CALL system.rollback_to_snapshot('tpch', 'test_rollback', %s)", afterFirstInsertId));
        assertQuery("SELECT * FROM test_rollback ORDER BY col0", "VALUES (123, CAST(987 AS BIGINT))");

        assertUpdate(format("CALL system.rollback_to_snapshot('tpch', 'test_rollback', %s)", afterCreateTableId));
        assertThat((long) computeActual("SELECT COUNT(*) FROM test_rollback").getOnlyValue()).isEqualTo(0);

        assertUpdate("INSERT INTO test_rollback (col0, col1) VALUES (789, CAST(987 AS BIGINT))", 1);
        long afterSecondInsertId = getCurrentSnapshotId("test_rollback");

        // extra insert which should be dropped on rollback
        assertUpdate("INSERT INTO test_rollback (col0, col1) VALUES (999, CAST(999 AS BIGINT))", 1);

        assertUpdate(format("CALL system.rollback_to_snapshot('tpch', 'test_rollback', %s)", afterSecondInsertId));
        assertQuery("SELECT * FROM test_rollback ORDER BY col0", "VALUES (789, CAST(987 AS BIGINT))");

        assertUpdate("DROP TABLE test_rollback");
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return "NULL value not allowed for NOT NULL column: " + columnName;
    }

    @Test
    public void testSchemaEvolution()
    {
        assertUpdate("CREATE TABLE test_schema_evolution_drop_end (col0 INTEGER, col1 INTEGER, col2 INTEGER)");
        assertUpdate("INSERT INTO test_schema_evolution_drop_end VALUES (0, 1, 2)", 1);
        assertQuery("SELECT * FROM test_schema_evolution_drop_end", "VALUES(0, 1, 2)");
        assertUpdate("ALTER TABLE test_schema_evolution_drop_end DROP COLUMN col2");
        assertQuery("SELECT * FROM test_schema_evolution_drop_end", "VALUES(0, 1)");
        assertUpdate("ALTER TABLE test_schema_evolution_drop_end ADD COLUMN col2 INTEGER");
        assertQuery("SELECT * FROM test_schema_evolution_drop_end", "VALUES(0, 1, NULL)");
        assertUpdate("INSERT INTO test_schema_evolution_drop_end VALUES (3, 4, 5)", 1);
        assertQuery("SELECT * FROM test_schema_evolution_drop_end", "VALUES(0, 1, NULL), (3, 4, 5)");
        assertUpdate("DROP TABLE test_schema_evolution_drop_end");

        assertUpdate("CREATE TABLE test_schema_evolution_drop_middle (col0 INTEGER, col1 INTEGER, col2 INTEGER)");
        assertUpdate("INSERT INTO test_schema_evolution_drop_middle VALUES (0, 1, 2)", 1);
        assertQuery("SELECT * FROM test_schema_evolution_drop_middle", "VALUES(0, 1, 2)");
        assertUpdate("ALTER TABLE test_schema_evolution_drop_middle DROP COLUMN col1");
        assertQuery("SELECT * FROM test_schema_evolution_drop_middle", "VALUES(0, 2)");
        assertUpdate("ALTER TABLE test_schema_evolution_drop_middle ADD COLUMN col1 INTEGER");
        assertUpdate("INSERT INTO test_schema_evolution_drop_middle VALUES (3, 4, 5)", 1);
        assertQuery("SELECT * FROM test_schema_evolution_drop_middle", "VALUES(0, 2, NULL), (3, 4, 5)");
        assertUpdate("DROP TABLE test_schema_evolution_drop_middle");
    }

    @Test
    @Override
    public void testDropRowFieldWhenDuplicates()
    {
        // Override because Iceberg doesn't allow duplicated field names in a row type
        assertThatThrownBy(super::testDropRowFieldWhenDuplicates)
                .hasMessage("Field name 'a' specified more than once");
    }

    @Test
    @Override // Override because ambiguous field name is disallowed in the connector
    public void testDropAmbiguousRowFieldCaseSensitivity()
    {
        assertThatThrownBy(super::testDropAmbiguousRowFieldCaseSensitivity)
                .hasMessage("Field name 'some_field' specified more than once");
    }

    @Test
    public void testDuplicatedFieldNames()
    {
        String tableName = "test_duplicated_field_names" + randomNameSuffix();

        assertQueryFails("CREATE TABLE " + tableName + "(col row(x int, \"X\" int))", "Field name 'x' specified more than once");
        assertQueryFails("CREATE TABLE " + tableName + " AS SELECT cast(NULL AS row(x int, \"X\" int)) col", "Field name 'x' specified more than once");

        assertQueryFails("CREATE TABLE " + tableName + "(col array(row(x int, \"X\" int)))", "Field name 'x' specified more than once");
        assertQueryFails("CREATE TABLE " + tableName + " AS SELECT cast(NULL AS array(row(x int, \"X\" int))) col", "Field name 'x' specified more than once");

        assertQueryFails("CREATE TABLE " + tableName + "(col map(int, row(x int, \"X\" int)))", "Field name 'x' specified more than once");
        assertQueryFails("CREATE TABLE " + tableName + " AS SELECT cast(NULL AS map(int, row(x int, \"X\" int))) col", "Field name 'x' specified more than once");

        assertQueryFails("CREATE TABLE " + tableName + "(col row(a row(x int, \"X\" int)))", "Field name 'x' specified more than once");
        assertQueryFails("CREATE TABLE " + tableName + " AS SELECT cast(NULL AS row(a row(x int, \"X\" int))) col", "Field name 'x' specified more than once");

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_duplicated_field_names_", "(id int)")) {
            assertQueryFails("ALTER TABLE " + table.getName() + " ADD COLUMN col row(x int, \"X\" int)", ".* Field name 'x' specified more than once");

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN col row(\"X\" int)");
            assertQueryFails("ALTER TABLE " + table.getName() + " ADD COLUMN col.x int", "line 1:1: Field 'x' already exists");

            assertQueryFails("ALTER TABLE " + table.getName() + " ALTER COLUMN col SET DATA TYPE row(x int, \"X\" int)", "Field name 'x' specified more than once");
        }
    }

    @Test
    public void testDropPartitionColumn()
    {
        String tableName = "test_drop_partition_column_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR, age INTEGER, nested ROW(f1 integer, f2 integer)) " +
                "WITH (partitioning = ARRAY['id', 'truncate(name, 5)', 'void(age)', '\"nested.f1\"'])");
        assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN id", "Cannot drop partition field: id");
        assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN name", "Cannot drop partition field: name");
        assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN age", "Cannot drop partition field: age");
        assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN nested", "Failed to drop column.*");
        assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN nested.f1", "Cannot drop partition field: nested.f1");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDropColumnUsedInOlderPartitionSpecs()
    {
        String tableName = "test_drop_partition_column_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR, age INTEGER) WITH (partitioning = ARRAY['id', 'truncate(name, 5)', 'void(age)'])");
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES partitioning = ARRAY[]");
        assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN id", "Cannot drop column which is used by an old partition spec: id");
        assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN name", "Cannot drop column which is used by an old partition spec: name");
        assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN age", "Cannot drop column which is used by an old partition spec: age");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testShowStatsAfterAddColumn()
    {
        assertUpdate("CREATE TABLE test_show_stats_after_add_column (col0 INTEGER, col1 INTEGER, col2 INTEGER)");
        // Insert separately to ensure the table has multiple data files
        assertUpdate("INSERT INTO test_show_stats_after_add_column VALUES (1, 2, 3)", 1);
        assertUpdate("INSERT INTO test_show_stats_after_add_column VALUES (4, 5, 6)", 1);
        assertUpdate("INSERT INTO test_show_stats_after_add_column VALUES (NULL, NULL, NULL)", 1);
        assertUpdate("INSERT INTO test_show_stats_after_add_column VALUES (7, 8, 9)", 1);

        if (format != AVRO) {
            assertThat(query("SHOW STATS FOR test_show_stats_after_add_column"))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "  ('col0', NULL, 3e0, 25e-2, NULL, '1', '7')," +
                            "  ('col1', NULL, 3e0, 25e-2, NULL, '2', '8'), " +
                            "  ('col2', NULL, 3e0, 25e-2, NULL, '3', '9'), " +
                            "  (NULL, NULL, NULL, NULL, 4e0, NULL, NULL)");
        }
        else {
            assertThat(query("SHOW STATS FOR test_show_stats_after_add_column"))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "  ('col0', NULL, 3e0, 0.1e0, NULL, NULL, NULL)," +
                            "  ('col1', NULL, 3e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('col2', NULL, 3e0, 0.1e0, NULL, NULL, NULL), " +
                            "  (NULL, NULL, NULL, NULL, 4e0, NULL, NULL)");
        }

        // Columns added after some data files exist will not have valid statistics because not all files have min/max/null count statistics for the new column
        assertUpdate("ALTER TABLE test_show_stats_after_add_column ADD COLUMN col3 INTEGER");
        assertUpdate("INSERT INTO test_show_stats_after_add_column VALUES (10, 11, 12, 13)", 1);
        if (format != AVRO) {
            assertThat(query("SHOW STATS FOR test_show_stats_after_add_column"))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "  ('col0', NULL, 4e0, 2e-1, NULL, '1', '10')," +
                            "  ('col1', NULL, 4e0, 2e-1, NULL, '2', '11'), " +
                            "  ('col2', NULL, 4e0, 2e-1, NULL, '3', '12'), " +
                            "  ('col3', NULL, NULL, NULL, NULL, NULL, NULL), " +
                            "  (NULL, NULL, NULL, NULL, 5e0, NULL, NULL)");
        }
        else {
            assertThat(query("SHOW STATS FOR test_show_stats_after_add_column"))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "  ('col0', NULL, 4e0, 0.1e0, NULL, NULL, NULL)," +
                            "  ('col1', NULL, 4e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('col2', NULL, 4e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('col3', NULL, NULL, NULL, NULL, NULL, NULL), " +
                            "  (NULL, NULL, NULL, NULL, 5e0, NULL, NULL)");
        }
    }

    @Test
    public void testLargeInOnPartitionedColumns()
    {
        assertUpdate("CREATE TABLE test_in_predicate_large_set (col1 BIGINT, col2 BIGINT) WITH (partitioning = ARRAY['col2'])");
        assertUpdate("INSERT INTO test_in_predicate_large_set VALUES (1, 10)", 1L);
        assertUpdate("INSERT INTO test_in_predicate_large_set VALUES (2, 20)", 1L);

        List<String> predicates = range(0, 25_000).boxed()
                .map(Object::toString)
                .collect(toImmutableList());
        String filter = format("col2 IN (%s)", join(",", predicates));
        assertThat(query("SELECT * FROM test_in_predicate_large_set WHERE " + filter))
                .matches("TABLE test_in_predicate_large_set");

        assertUpdate("DROP TABLE test_in_predicate_large_set");
    }

    @Test
    public void testTableNameCollision()
    {
        String tableName = "test_rename_table_" + randomNameSuffix();
        String tmpName = "test_rename_table_tmp_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tmpName + " AS SELECT 1 as a", 1);
            assertUpdate("ALTER TABLE " + tmpName + " RENAME TO " + tableName);
            assertUpdate("CREATE TABLE " + tmpName + " AS SELECT 2 as a", 1);
            assertQuery("SELECT * FROM " + tmpName, "VALUES 2");
            assertQuery("SELECT * FROM " + tableName, "VALUES 1");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
            assertUpdate("DROP TABLE IF EXISTS " + tmpName);
        }
    }

    @Test
    public void testCreateTableSucceedsOnEmptyDirectory()
    {
        File tempDir = getDistributedQueryRunner().getCoordinator().getBaseDataDir().toFile();
        String tmpName = "test_rename_table_tmp_" + randomNameSuffix();
        Path newPath = tempDir.toPath().resolve(tmpName);
        File directory = newPath.toFile();
        verify(directory.mkdirs(), "Could not make directory on filesystem");
        try {
            assertUpdate("CREATE TABLE " + tmpName + " WITH (location='" + directory + "') AS SELECT 1 as a", 1);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tmpName);
        }
    }

    @Test
    public void testCreateTableLike()
    {
        IcebergFileFormat otherFormat = (format == PARQUET) ? ORC : PARQUET;
        testCreateTableLikeForFormat(otherFormat);
    }

    private void testCreateTableLikeForFormat(IcebergFileFormat otherFormat)
    {
        File tempDir = getDistributedQueryRunner().getCoordinator().getBaseDataDir().toFile();
        String tempDirPath = tempDir.toURI().toASCIIString() + randomNameSuffix();

        // LIKE source INCLUDING PROPERTIES copies all the properties of the source table, including the `location`.
        // For this reason the source and the copied table will share the same directory.
        // This test does not drop intentionally the created tables to avoid affecting the source table or the information_schema.
        assertUpdate(format("CREATE TABLE test_create_table_like_original (col1 INTEGER, aDate DATE) WITH(format = '%s', location = '%s', partitioning = ARRAY['aDate'])", format, tempDirPath));
        assertThat(getTablePropertiesString("test_create_table_like_original")).isEqualTo(format(
                """
                        WITH (
                           format = '%s',
                           format_version = 2,
                           location = '%s',
                           partitioning = ARRAY['adate']
                        )""",
                format,
                tempDirPath));

        assertUpdate("CREATE TABLE test_create_table_like_copy0 (LIKE test_create_table_like_original, col2 INTEGER)");
        assertUpdate("INSERT INTO test_create_table_like_copy0 (col1, aDate, col2) VALUES (1, CAST('1950-06-28' AS DATE), 3)", 1);
        assertQuery("SELECT * from test_create_table_like_copy0", "VALUES(1, CAST('1950-06-28' AS DATE), 3)");

        assertUpdate("CREATE TABLE test_create_table_like_copy1 (LIKE test_create_table_like_original)");
        assertThat(getTablePropertiesString("test_create_table_like_copy1")).isEqualTo(format(
                """
                        WITH (
                           format = '%s',
                           format_version = 2,
                           location = '%s'
                        )""",
                format,
                getTableLocation("test_create_table_like_copy1")));

        assertUpdate("CREATE TABLE test_create_table_like_copy2 (LIKE test_create_table_like_original EXCLUDING PROPERTIES)");
        assertThat(getTablePropertiesString("test_create_table_like_copy2")).isEqualTo(format(
                """
                        WITH (
                           format = '%s',
                           format_version = 2,
                           location = '%s'
                        )""",
                format,
                getTableLocation("test_create_table_like_copy2")));
        assertUpdate("DROP TABLE test_create_table_like_copy2");

        assertQueryFails("CREATE TABLE test_create_table_like_copy3 (LIKE test_create_table_like_original INCLUDING PROPERTIES)",
                "Cannot create a table on a non-empty location.*");

        assertQueryFails(format("CREATE TABLE test_create_table_like_copy4 (LIKE test_create_table_like_original INCLUDING PROPERTIES) WITH (format = '%s')", otherFormat),
                "Cannot create a table on a non-empty location.*");
    }

    private String getTablePropertiesString(String tableName)
    {
        MaterializedResult showCreateTable = computeActual("SHOW CREATE TABLE " + tableName);
        String createTable = (String) getOnlyElement(showCreateTable.getOnlyColumnAsSet());
        Matcher matcher = WITH_CLAUSE_EXTRACTOR.matcher(createTable);
        return matcher.matches() ? matcher.group(1) : null;
    }

    @Test
    public void testPredicating()
    {
        assertUpdate("CREATE TABLE test_predicating_on_real (col REAL)");
        assertUpdate("INSERT INTO test_predicating_on_real VALUES 1.2", 1);
        assertQuery("SELECT * FROM test_predicating_on_real WHERE col = 1.2", "VALUES 1.2");
        assertUpdate("DROP TABLE test_predicating_on_real");
    }

    @Test
    public void testHourTransformTimestamp()
    {
        assertUpdate("CREATE TABLE test_hour_transform_timestamp (d timestamp(6), b bigint) WITH (partitioning = ARRAY['hour(d)'])");

        @Language("SQL") String values = "VALUES " +
                "(NULL, 101)," +
                "(TIMESTAMP '1969-12-31 22:22:22.222222', 8)," +
                "(TIMESTAMP '1969-12-31 23:33:11.456789', 9)," +
                "(TIMESTAMP '1969-12-31 23:44:55.567890', 10)," +
                "(TIMESTAMP '1970-01-01 00:55:44.765432', 11)," +
                "(TIMESTAMP '2015-01-01 10:01:23.123456', 1)," +
                "(TIMESTAMP '2015-01-01 10:10:02.987654', 2)," +
                "(TIMESTAMP '2015-01-01 10:55:00.456789', 3)," +
                "(TIMESTAMP '2015-05-15 12:05:01.234567', 4)," +
                "(TIMESTAMP '2015-05-15 12:21:02.345678', 5)," +
                "(TIMESTAMP '2020-02-21 13:11:11.876543', 6)," +
                "(TIMESTAMP '2020-02-21 13:12:12.654321', 7)";
        assertUpdate("INSERT INTO test_hour_transform_timestamp " + values, 12);
        assertQuery("SELECT * FROM test_hour_transform_timestamp", values);

        @Language("SQL") String expected = "VALUES " +
                "(NULL, 1, NULL, NULL, 101, 101), " +
                "(-2, 1, TIMESTAMP '1969-12-31 22:22:22.222222', TIMESTAMP '1969-12-31 22:22:22.222222', 8, 8), " +
                "(-1, 2, TIMESTAMP '1969-12-31 23:33:11.456789', TIMESTAMP '1969-12-31 23:44:55.567890', 9, 10), " +
                "(0, 1, TIMESTAMP '1970-01-01 00:55:44.765432', TIMESTAMP '1970-01-01 00:55:44.765432', 11, 11), " +
                "(394474, 3, TIMESTAMP '2015-01-01 10:01:23.123456', TIMESTAMP '2015-01-01 10:55:00.456789', 1, 3), " +
                "(397692, 2, TIMESTAMP '2015-05-15 12:05:01.234567', TIMESTAMP '2015-05-15 12:21:02.345678', 4, 5), " +
                "(439525, 2, TIMESTAMP '2020-02-21 13:11:11.876543', TIMESTAMP '2020-02-21 13:12:12.654321', 6, 7)";
        String expectedTimestampStats = "NULL, 11e0, 0.0833333e0, NULL, '1969-12-31 22:22:22.222222', '2020-02-21 13:12:12.654321'";
        String expectedBigIntStats = "NULL, 12e0, 0e0, NULL, '1', '101'";
        if (format == ORC) {
            expected = "VALUES " +
                    "(NULL, 1, NULL, NULL, 101, 101), " +
                    "(-2, 1, TIMESTAMP '1969-12-31 22:22:22.222000', TIMESTAMP '1969-12-31 22:22:22.222999', 8, 8), " +
                    "(-1, 2, TIMESTAMP '1969-12-31 23:33:11.456000', TIMESTAMP '1969-12-31 23:44:55.567999', 9, 10), " +
                    "(0, 1, TIMESTAMP '1970-01-01 00:55:44.765000', TIMESTAMP '1970-01-01 00:55:44.765999', 11, 11), " +
                    "(394474, 3, TIMESTAMP '2015-01-01 10:01:23.123000', TIMESTAMP '2015-01-01 10:55:00.456999', 1, 3), " +
                    "(397692, 2, TIMESTAMP '2015-05-15 12:05:01.234000', TIMESTAMP '2015-05-15 12:21:02.345999', 4, 5), " +
                    "(439525, 2, TIMESTAMP '2020-02-21 13:11:11.876000', TIMESTAMP '2020-02-21 13:12:12.654999', 6, 7)";
            expectedTimestampStats = "NULL, 11e0, 0.0833333e0, NULL, '1969-12-31 22:22:22.222000', '2020-02-21 13:12:12.654999'";
        }
        else if (format == AVRO) {
            expected = "VALUES " +
                    "(NULL, 1, NULL, NULL, NULL, NULL), " +
                    "(-2, 1, NULL, NULL, NULL, NULL), " +
                    "(-1, 2, NULL, NULL, NULL, NULL), " +
                    "(0, 1, NULL, NULL, NULL, NULL), " +
                    "(394474, 3, NULL, NULL, NULL, NULL), " +
                    "(397692, 2, NULL, NULL, NULL, NULL), " +
                    "(439525, 2, NULL, NULL, NULL, NULL)";
            expectedTimestampStats = "NULL, 11e0, 0.0833333e0, NULL, NULL, NULL";
            expectedBigIntStats = "NULL, 12e0, 0e0, NULL, NULL, NULL";
        }

        assertQuery("SELECT partition.d_hour, record_count, data.d.min, data.d.max, data.b.min, data.b.max FROM \"test_hour_transform_timestamp$partitions\"", expected);

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertQuery(
                "SELECT * FROM test_hour_transform_timestamp WHERE day_of_week(d) = 3 AND b % 7 = 3",
                "VALUES (TIMESTAMP '1969-12-31 23:44:55.567890', 10)");

        assertThat(query("SHOW STATS FOR test_hour_transform_timestamp"))
                .skippingTypesCheck()
                .matches("VALUES " +
                        "  ('d', " + expectedTimestampStats + "), " +
                        "  ('b', " + expectedBigIntStats + "), " +
                        "  (NULL, NULL, NULL, NULL, 12e0, NULL, NULL)");

        assertThat(query("SELECT * FROM test_hour_transform_timestamp WHERE d IS NOT NULL"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_hour_transform_timestamp WHERE d IS NULL"))
                .isFullyPushedDown();

        assertThat(query("SELECT * FROM test_hour_transform_timestamp WHERE d >= DATE '2015-05-15'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_hour_transform_timestamp WHERE CAST(d AS date) >= DATE '2015-05-15'"))
                .isFullyPushedDown();

        assertThat(query("SELECT * FROM test_hour_transform_timestamp WHERE d >= TIMESTAMP '2015-05-15 12:00:00'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_hour_transform_timestamp WHERE d >= TIMESTAMP '2015-05-15 12:00:00.000001'"))
                .isNotFullyPushedDown(FilterNode.class);

        // date()
        assertThat(query("SELECT * FROM test_hour_transform_timestamp WHERE date(d) = DATE '2015-05-15'"))
                .isFullyPushedDown();

        // year()
        assertThat(query("SELECT * FROM test_hour_transform_timestamp WHERE year(d) = 2015"))
                .isFullyPushedDown();

        // date_trunc
        assertThat(query("SELECT * FROM test_hour_transform_timestamp WHERE date_trunc('hour', d) = TIMESTAMP '2015-05-15 12:00:00'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_hour_transform_timestamp WHERE date_trunc('day', d) = DATE '2015-05-15'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_hour_transform_timestamp WHERE date_trunc('month', d) = DATE '2015-05-01'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_hour_transform_timestamp WHERE date_trunc('year', d) = DATE '2015-01-01'"))
                .isFullyPushedDown();

        assertUpdate("DROP TABLE test_hour_transform_timestamp");
    }

    @Test
    public void testHourTransformTimestampWithTimeZone()
    {
        assertUpdate("CREATE TABLE test_hour_transform_timestamptz (d timestamp(6) with time zone, b integer) WITH (partitioning = ARRAY['hour(d)'])");

        @Language("SQL") String values = "VALUES " +
                "(NULL, 101)," +
                "(TIMESTAMP '1969-12-31 22:22:22.222222 UTC', 8)," +
                "(TIMESTAMP '1969-12-31 23:33:11.456789 UTC', 9)," +
                "(TIMESTAMP '1969-12-31 23:44:55.567890 UTC', 10)," +
                "(TIMESTAMP '1970-01-01 00:55:44.765432 UTC', 11)," +
                "(TIMESTAMP '2015-01-01 10:01:23.123456 UTC', 1)," +
                "(TIMESTAMP '2015-01-01 10:10:02.987654 UTC', 2)," +
                "(TIMESTAMP '2015-01-01 10:55:00.456789 UTC', 3)," +
                "(TIMESTAMP '2015-05-15 12:05:01.234567 UTC', 4)," +
                "(TIMESTAMP '2015-05-15 12:21:02.345678 UTC', 5)," +
                "(TIMESTAMP '2020-02-21 13:11:11.876543 UTC', 6)," +
                "(TIMESTAMP '2020-02-21 13:12:12.654321 UTC', 7)";
        assertUpdate("INSERT INTO test_hour_transform_timestamptz " + values, 12);
        assertThat(query("SELECT * FROM test_hour_transform_timestamptz"))
                .matches(values);

        @Language("SQL") String expected = "VALUES " +
                "(NULL, BIGINT '1', NULL, NULL, 101, 101), " +
                "(-2, 1, TIMESTAMP '1969-12-31 22:22:22.222222 UTC', TIMESTAMP '1969-12-31 22:22:22.222222 UTC', 8, 8), " +
                "(-1, 2, TIMESTAMP '1969-12-31 23:33:11.456789 UTC', TIMESTAMP '1969-12-31 23:44:55.567890 UTC', 9, 10), " +
                "(0, 1, TIMESTAMP '1970-01-01 00:55:44.765432 UTC', TIMESTAMP '1970-01-01 00:55:44.765432 UTC', 11, 11), " +
                "(394474, 3, TIMESTAMP '2015-01-01 10:01:23.123456 UTC', TIMESTAMP '2015-01-01 10:55:00.456789 UTC', 1, 3), " +
                "(397692, 2, TIMESTAMP '2015-05-15 12:05:01.234567 UTC', TIMESTAMP '2015-05-15 12:21:02.345678 UTC', 4, 5), " +
                "(439525, 2, TIMESTAMP '2020-02-21 13:11:11.876543 UTC', TIMESTAMP '2020-02-21 13:12:12.654321 UTC', 6, 7)";
        String expectedTimestampStats = "NULL, 11e0, 0.0833333e0, NULL, '1969-12-31 22:22:22.222 UTC', '2020-02-21 13:12:12.654 UTC'";
        String expectedBigIntStats = "NULL, 12e0, 0e0, NULL, '1', '101'";
        if (format == ORC) {
            expected = "VALUES " +
                    "(NULL, BIGINT '1', NULL, NULL, 101, 101), " +
                    "(-2, 1, TIMESTAMP '1969-12-31 22:22:22.222000 UTC', TIMESTAMP '1969-12-31 22:22:22.222999 UTC', 8, 8), " +
                    "(-1, 2, TIMESTAMP '1969-12-31 23:33:11.456000 UTC', TIMESTAMP '1969-12-31 23:44:55.567999 UTC', 9, 10), " +
                    "(0, 1, TIMESTAMP '1970-01-01 00:55:44.765000 UTC', TIMESTAMP '1970-01-01 00:55:44.765999 UTC', 11, 11), " +
                    "(394474, 3, TIMESTAMP '2015-01-01 10:01:23.123000 UTC', TIMESTAMP '2015-01-01 10:55:00.456999 UTC', 1, 3), " +
                    "(397692, 2, TIMESTAMP '2015-05-15 12:05:01.234000 UTC', TIMESTAMP '2015-05-15 12:21:02.345999 UTC', 4, 5), " +
                    "(439525, 2, TIMESTAMP '2020-02-21 13:11:11.876000 UTC', TIMESTAMP '2020-02-21 13:12:12.654999 UTC', 6, 7)";
            expectedTimestampStats = "NULL, 11e0, 0.0833333e0, NULL, '1969-12-31 22:22:22.222 UTC', '2020-02-21 13:12:12.654 UTC'";
        }
        else if (format == AVRO) {
            expected = "VALUES " +
                    "(NULL, BIGINT '1', CAST(NULL AS timestamp(6) with time zone), CAST(NULL AS timestamp(6) with time zone), CAST(NULL AS integer), CAST(NULL AS integer)), " +
                    "(-2, 1, NULL, NULL, NULL, NULL), " +
                    "(-1, 2, NULL, NULL, NULL, NULL), " +
                    "(0, 1, NULL, NULL, NULL, NULL), " +
                    "(394474, 3, NULL, NULL, NULL, NULL), " +
                    "(397692, 2, NULL, NULL, NULL, NULL), " +
                    "(439525, 2, NULL, NULL, NULL, NULL)";
            expectedTimestampStats = "NULL, 11e0, 0.0833333e0, NULL, NULL, NULL";
            expectedBigIntStats = "NULL, 12e0, 0e0, NULL, NULL, NULL";
        }

        assertThat(query("SELECT partition.d_hour, record_count, data.d.min, data.d.max, data.b.min, data.b.max FROM \"test_hour_transform_timestamptz$partitions\""))
                .matches(expected);

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertThat(query("SELECT * FROM test_hour_transform_timestamptz WHERE day_of_week(d) = 3 AND b % 7 = 3"))
                .matches("VALUES (TIMESTAMP '1969-12-31 23:44:55.567890 UTC', 10)");

        assertThat(query("SHOW STATS FOR test_hour_transform_timestamptz"))
                .skippingTypesCheck()
                .matches("VALUES " +
                        "  ('d', " + expectedTimestampStats + "), " +
                        "  ('b', " + expectedBigIntStats + "), " +
                        "  (NULL, NULL, NULL, NULL, 12e0, NULL, NULL)");

        assertThat(query("SELECT * FROM test_hour_transform_timestamptz WHERE d IS NOT NULL"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_hour_transform_timestamptz WHERE d IS NULL"))
                .isFullyPushedDown();

        assertThat(query("SELECT * FROM test_hour_transform_timestamptz WHERE d >= DATE '2015-05-15'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_hour_transform_timestamptz WHERE CAST(d AS date) >= DATE '2015-05-15'"))
                .isFullyPushedDown();

        assertThat(query("SELECT * FROM test_hour_transform_timestamptz WHERE d >= TIMESTAMP '2015-05-15 12:00:00 UTC'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_hour_transform_timestamptz WHERE d >= TIMESTAMP '2015-05-15 12:00:00.000001 UTC'"))
                .isNotFullyPushedDown(FilterNode.class);

        // date()
        assertThat(query("SELECT * FROM test_hour_transform_timestamptz WHERE date(d) = DATE '2015-05-15'"))
                .isFullyPushedDown();

        // year()
        assertThat(query("SELECT * FROM test_hour_transform_timestamptz WHERE year(d) = 2015"))
                .isFullyPushedDown();

        // date_trunc
        assertThat(query("SELECT * FROM test_hour_transform_timestamptz WHERE date_trunc('hour', d) = TIMESTAMP '2015-05-15 12:00:00.000000 UTC'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_hour_transform_timestamptz WHERE date_trunc('day', d) = TIMESTAMP '2015-05-15 00:00:00.000000 UTC'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_hour_transform_timestamptz WHERE date_trunc('month', d) = TIMESTAMP '2015-05-01 00:00:00.000000 UTC'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_hour_transform_timestamptz WHERE date_trunc('year', d) = TIMESTAMP '2015-01-01 00:00:00.000000 UTC'"))
                .isFullyPushedDown();

        assertUpdate("DROP TABLE test_hour_transform_timestamptz");
    }

    @Test
    public void testPartitionPredicatePushdownWithHistoricalPartitionSpecs()
    {
        // Start with a bucket transform, which cannot be used for predicate pushdown
        String tableName = "test_partition_predicate_pushdown_with_historical_partition_specs";
        assertUpdate("CREATE TABLE " + tableName + " (d TIMESTAMP(6), b INTEGER) WITH (partitioning = ARRAY['bucket(b, 3)'])");
        @Language("SQL") String selectQuery = "SELECT b FROM " + tableName + " WHERE CAST(d AS date) < DATE '2015-01-02'";

        @Language("SQL") String initialValues =
                "(TIMESTAMP '1969-12-31 22:22:22.222222', 8)," +
                        "(TIMESTAMP '1969-12-31 23:33:11.456789', 9)," +
                        "(TIMESTAMP '1969-12-31 23:44:55.567890', 10)";
        assertUpdate("INSERT INTO " + tableName + " VALUES " + initialValues, 3);
        assertThat(query(selectQuery))
                .containsAll("VALUES 8, 9, 10")
                .isNotFullyPushedDown(FilterNode.class);

        @Language("SQL") String hourTransformValues =
                "(TIMESTAMP '2015-01-01 10:01:23.123456', 1)," +
                        "(TIMESTAMP '2015-01-02 10:10:02.987654', 2)," +
                        "(TIMESTAMP '2015-01-03 10:55:00.456789', 3)";
        // While the bucket transform is still used the hour transform still cannot be used for pushdown
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES partitioning = ARRAY['hour(d)']");
        assertUpdate("INSERT INTO " + tableName + " VALUES " + hourTransformValues, 3);
        assertThat(query(selectQuery))
                .containsAll("VALUES 1, 8, 9, 10")
                .isNotFullyPushedDown(FilterNode.class);

        // The old partition scheme is no longer used so pushdown using the hour transform is allowed
        assertUpdate("DELETE FROM " + tableName + " WHERE year(d) = 1969", 3);
        assertUpdate("ALTER TABLE " + tableName + " EXECUTE optimize");
        assertUpdate("INSERT INTO " + tableName + " VALUES " + initialValues, 3);
        assertThat(query(selectQuery))
                .containsAll("VALUES 1, 8, 9, 10")
                .isFullyPushedDown();

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testPartitionPredicatePushdownWithNestedFieldPartitioning()
    {
        // Start with a bucket transform, which cannot be used for predicate pushdown
        String tableName = "test_partition_predicate_pushdown_with_nested_field_partitioning";
        assertUpdate("CREATE TABLE " + tableName + " (parent ROW(child1 TIMESTAMP(6), child2 INTEGER)) WITH (partitioning = ARRAY['bucket(\"parent.child2\", 3)'])");
        String selectQuery = "SELECT parent.child2 FROM " + tableName + " WHERE CAST(parent.child1 AS date) < DATE '2015-01-02'";

        String initialValues =
                "ROW(ROW(TIMESTAMP '1969-12-31 22:22:22.222222', 8))," +
                        "ROW(ROW(TIMESTAMP '1969-12-31 23:33:11.456789', 9))," +
                        "ROW(ROW(TIMESTAMP '1969-12-31 23:44:55.567890', 10))";
        assertUpdate("INSERT INTO " + tableName + " VALUES " + initialValues, 3);
        assertThat(query(selectQuery))
                .containsAll("VALUES 8, 9, 10")
                .isNotFullyPushedDown(FilterNode.class);

        String hourTransformValues =
                "ROW(ROW(TIMESTAMP '2015-01-01 10:01:23.123456', 1))," +
                        "ROW(ROW(TIMESTAMP '2015-01-02 10:10:02.987654', 2))," +
                        "ROW(ROW(TIMESTAMP '2015-01-03 10:55:00.456789', 3))";
        // While the bucket transform is still used, the hour transform cannot be used for pushdown
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES partitioning = ARRAY['hour(\"parent.child1\")']");
        assertUpdate("INSERT INTO " + tableName + " VALUES " + hourTransformValues, 3);
        assertThat(query(selectQuery))
                .containsAll("VALUES 1, 8, 9, 10")
                .isNotFullyPushedDown(FilterNode.class);

        // The old partition scheme is no longer used so pushdown using the hour transform is allowed
        assertUpdate("DELETE FROM " + tableName + " WHERE year(parent.child1) = 1969", 3);
        assertUpdate("ALTER TABLE " + tableName + " EXECUTE optimize");
        assertUpdate("INSERT INTO " + tableName + " VALUES " + initialValues, 3);
        assertThat(query(selectQuery))
                .containsAll("VALUES 1, 8, 9, 10")
                .isFullyPushedDown();

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDayTransformDate()
    {
        assertUpdate("CREATE TABLE test_day_transform_date (d DATE, b BIGINT) WITH (partitioning = ARRAY['day(d)'])");

        @Language("SQL") String values = "VALUES " +
                "(NULL, 101)," +
                "(DATE '1969-01-01', 10), " +
                "(DATE '1969-12-31', 11), " +
                "(DATE '1970-01-01', 1), " +
                "(DATE '1970-03-04', 2), " +
                "(DATE '2015-01-01', 3), " +
                "(DATE '2015-01-13', 4), " +
                "(DATE '2015-01-13', 5), " +
                "(DATE '2015-05-15', 6), " +
                "(DATE '2015-05-15', 7), " +
                "(DATE '2020-02-21', 8), " +
                "(DATE '2020-02-21', 9)";
        assertUpdate("INSERT INTO test_day_transform_date " + values, 12);
        assertQuery("SELECT * FROM test_day_transform_date", values);

        String expected = "VALUES " +
                "(NULL, 1, NULL, NULL, 101, 101), " +
                "(DATE '1969-01-01', 1, DATE '1969-01-01', DATE '1969-01-01', 10, 10), " +
                "(DATE '1969-12-31', 1, DATE '1969-12-31', DATE '1969-12-31', 11, 11), " +
                "(DATE '1970-01-01', 1, DATE '1970-01-01', DATE '1970-01-01', 1, 1), " +
                "(DATE '1970-03-04', 1, DATE '1970-03-04', DATE '1970-03-04', 2, 2), " +
                "(DATE '2015-01-01', 1, DATE '2015-01-01', DATE '2015-01-01', 3, 3), " +
                "(DATE '2015-01-13', 2, DATE '2015-01-13', DATE '2015-01-13', 4, 5), " +
                "(DATE '2015-05-15', 2, DATE '2015-05-15', DATE '2015-05-15', 6, 7), " +
                "(DATE '2020-02-21', 2, DATE '2020-02-21', DATE '2020-02-21', 8, 9)";
        if (format == AVRO) {
            expected = "VALUES " +
                    "(NULL, 1, NULL, NULL, NULL, NULL), " +
                    "(DATE '1969-01-01', 1, NULL, NULL, NULL, NULL), " +
                    "(DATE '1969-12-31', 1, NULL, NULL, NULL, NULL), " +
                    "(DATE '1970-01-01', 1, NULL, NULL, NULL, NULL), " +
                    "(DATE '1970-03-04', 1, NULL, NULL, NULL, NULL), " +
                    "(DATE '2015-01-01', 1, NULL, NULL, NULL, NULL), " +
                    "(DATE '2015-01-13', 2, NULL, NULL, NULL, NULL), " +
                    "(DATE '2015-05-15', 2, NULL, NULL, NULL, NULL), " +
                    "(DATE '2020-02-21', 2, NULL, NULL, NULL, NULL)";
        }
        assertQuery(
                "SELECT partition.d_day, record_count, data.d.min, data.d.max, data.b.min, data.b.max FROM \"test_day_transform_date$partitions\"",
                expected);

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertQuery(
                "SELECT * FROM test_day_transform_date WHERE day_of_week(d) = 3 AND b % 7 = 3",
                "VALUES (DATE '1969-01-01', 10)");

        String expectedTransformed = "VALUES " +
                "  ('d', NULL, 8e0, 0.0833333e0, NULL, '1969-01-01', '2020-02-21'), " +
                "  ('b', NULL, 12e0, 0e0, NULL, '1', '101'), " +
                "  (NULL, NULL, NULL, NULL, 12e0, NULL, NULL)";
        if (format == AVRO) {
            expectedTransformed = "VALUES " +
                    "  ('d', NULL, 8e0, 0.1e0, NULL, NULL, NULL), " +
                    "  ('b', NULL, 12e0, 0e0, NULL, NULL, NULL), " +
                    "  (NULL, NULL, NULL, NULL, 12e0, NULL, NULL)";
        }
        assertThat(query("SHOW STATS FOR test_day_transform_date"))
                .skippingTypesCheck()
                .matches(expectedTransformed);

        assertThat(query("SELECT * FROM test_day_transform_date WHERE d IS NOT NULL"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_day_transform_date WHERE d IS NULL"))
                .isFullyPushedDown();

        assertThat(query("SELECT * FROM test_day_transform_date WHERE d >= DATE '2015-01-13'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_day_transform_date WHERE CAST(d AS date) >= DATE '2015-01-13'"))
                .isFullyPushedDown();

        // d comparison with TIMESTAMP can be unwrapped
        assertThat(query("SELECT * FROM test_day_transform_date WHERE d >= TIMESTAMP '2015-01-13 00:00:00'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_day_transform_date WHERE d >= TIMESTAMP '2015-01-13 00:00:00.000001'"))
                .isFullyPushedDown();

        // date()
        assertThat(query("SELECT * FROM test_day_transform_date WHERE date(d) = DATE '2015-01-13'"))
                .isFullyPushedDown();

        // year()
        assertThat(query("SELECT * FROM test_day_transform_date WHERE year(d) = 2015"))
                .isFullyPushedDown();

        // date_trunc
        assertThat(query("SELECT * FROM test_day_transform_date WHERE date_trunc('day', d) = DATE '2015-01-13'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_day_transform_date WHERE date_trunc('month', d) = DATE '2015-01-01'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_day_transform_date WHERE date_trunc('year', d) = DATE '2015-01-01'"))
                .isFullyPushedDown();

        assertUpdate("DROP TABLE test_day_transform_date");
    }

    @Test
    public void testDayTransformTimestamp()
    {
        assertUpdate("CREATE TABLE test_day_transform_timestamp (d TIMESTAMP(6), b BIGINT) WITH (partitioning = ARRAY['day(d)'])");

        @Language("SQL") String values = "VALUES " +
                "(NULL, 101)," +
                "(TIMESTAMP '1969-12-25 15:13:12.876543', 8)," +
                "(TIMESTAMP '1969-12-30 18:47:33.345678', 9)," +
                "(TIMESTAMP '1969-12-31 00:00:00.000000', 10)," +
                "(TIMESTAMP '1969-12-31 05:06:07.234567', 11)," +
                "(TIMESTAMP '1970-01-01 12:03:08.456789', 12)," +
                "(TIMESTAMP '2015-01-01 10:01:23.123456', 1)," +
                "(TIMESTAMP '2015-01-01 11:10:02.987654', 2)," +
                "(TIMESTAMP '2015-01-01 12:55:00.456789', 3)," +
                "(TIMESTAMP '2015-05-15 13:05:01.234567', 4)," +
                "(TIMESTAMP '2015-05-15 14:21:02.345678', 5)," +
                "(TIMESTAMP '2020-02-21 15:11:11.876543', 6)," +
                "(TIMESTAMP '2020-02-21 16:12:12.654321', 7)";
        assertUpdate("INSERT INTO test_day_transform_timestamp " + values, 13);
        assertQuery("SELECT * FROM test_day_transform_timestamp", values);

        @Language("SQL") String expected = "VALUES " +
                "(NULL, 1, NULL, NULL, 101, 101), " +
                "(DATE '1969-12-25', 1, TIMESTAMP '1969-12-25 15:13:12.876543', TIMESTAMP '1969-12-25 15:13:12.876543', 8, 8), " +
                "(DATE '1969-12-30', 1, TIMESTAMP '1969-12-30 18:47:33.345678', TIMESTAMP '1969-12-30 18:47:33.345678', 9, 9), " +
                "(DATE '1969-12-31', 2, TIMESTAMP '1969-12-31 00:00:00.000000', TIMESTAMP '1969-12-31 05:06:07.234567', 10, 11), " +
                "(DATE '1970-01-01', 1, TIMESTAMP '1970-01-01 12:03:08.456789', TIMESTAMP '1970-01-01 12:03:08.456789', 12, 12), " +
                "(DATE '2015-01-01', 3, TIMESTAMP '2015-01-01 10:01:23.123456', TIMESTAMP '2015-01-01 12:55:00.456789', 1, 3), " +
                "(DATE '2015-05-15', 2, TIMESTAMP '2015-05-15 13:05:01.234567', TIMESTAMP '2015-05-15 14:21:02.345678', 4, 5), " +
                "(DATE '2020-02-21', 2, TIMESTAMP '2020-02-21 15:11:11.876543', TIMESTAMP '2020-02-21 16:12:12.654321', 6, 7)";
        String expectedTimestampStats = "VALUES " +
                "  ('d', NULL, 12e0, 0.0769231e0, NULL, '1969-12-25 15:13:12.876543', '2020-02-21 16:12:12.654321'), " +
                "  ('b', NULL, 13e0, 0e0, NULL, '1', '101'), " +
                "  (NULL, NULL, NULL, NULL, 13e0, NULL, NULL)";

        if (format == ORC) {
            expected = "VALUES " +
                    "(NULL, 1, NULL, NULL, 101, 101), " +
                    "(DATE '1969-12-25', 1, TIMESTAMP '1969-12-25 15:13:12.876000', TIMESTAMP '1969-12-25 15:13:12.876999', 8, 8), " +
                    "(DATE '1969-12-30', 1, TIMESTAMP '1969-12-30 18:47:33.345000', TIMESTAMP '1969-12-30 18:47:33.345999', 9, 9), " +
                    "(DATE '1969-12-31', 2, TIMESTAMP '1969-12-31 00:00:00.000000', TIMESTAMP '1969-12-31 05:06:07.234999', 10, 11), " +
                    "(DATE '1970-01-01', 1, TIMESTAMP '1970-01-01 12:03:08.456000', TIMESTAMP '1970-01-01 12:03:08.456999', 12, 12), " +
                    "(DATE '2015-01-01', 3, TIMESTAMP '2015-01-01 10:01:23.123000', TIMESTAMP '2015-01-01 12:55:00.456999', 1, 3), " +
                    "(DATE '2015-05-15', 2, TIMESTAMP '2015-05-15 13:05:01.234000', TIMESTAMP '2015-05-15 14:21:02.345999', 4, 5), " +
                    "(DATE '2020-02-21', 2, TIMESTAMP '2020-02-21 15:11:11.876000', TIMESTAMP '2020-02-21 16:12:12.654999', 6, 7)";
            expectedTimestampStats = "VALUES " +
                    "  ('d', NULL, 12e0, 0.0769231e0, NULL, '1969-12-25 15:13:12.876000', '2020-02-21 16:12:12.654999'), " +
                    "  ('b', NULL, 13e0, 0e0, NULL, '1', '101'), " +
                    "  (NULL, NULL, NULL, NULL, 13e0, NULL, NULL)";
        }
        else if (format == AVRO) {
            expected = "VALUES " +
                    "(NULL, 1, NULL, NULL, NULL, NULL), " +
                    "(DATE '1969-12-25', 1, NULL, NULL, NULL, NULL), " +
                    "(DATE '1969-12-30', 1, NULL, NULL, NULL, NULL), " +
                    "(DATE '1969-12-31', 2, NULL, NULL, NULL, NULL), " +
                    "(DATE '1970-01-01', 1, NULL, NULL, NULL, NULL), " +
                    "(DATE '2015-01-01', 3, NULL, NULL, NULL, NULL), " +
                    "(DATE '2015-05-15', 2, NULL, NULL, NULL, NULL), " +
                    "(DATE '2020-02-21', 2, NULL, NULL, NULL, NULL)";
            expectedTimestampStats = "VALUES " +
                    "  ('d', NULL, 12e0, 0.076923e0, NULL, NULL, NULL), " +
                    "  ('b', NULL, 13e0, 0e0, NULL, NULL, NULL), " +
                    "  (NULL, NULL, NULL, NULL, 13e0, NULL, NULL)";
        }

        assertQuery("SELECT partition.d_day, record_count, data.d.min, data.d.max, data.b.min, data.b.max FROM \"test_day_transform_timestamp$partitions\"", expected);

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertQuery(
                "SELECT * FROM test_day_transform_timestamp WHERE day_of_week(d) = 3 AND b % 7 = 3",
                "VALUES (TIMESTAMP '1969-12-31 00:00:00.000000', 10)");

        assertThat(query("SHOW STATS FOR test_day_transform_timestamp"))
                .skippingTypesCheck()
                .matches(expectedTimestampStats);

        assertThat(query("SELECT * FROM test_day_transform_timestamp WHERE d IS NOT NULL"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_day_transform_timestamp WHERE d IS NULL"))
                .isFullyPushedDown();

        assertThat(query("SELECT * FROM test_day_transform_timestamp WHERE d >= DATE '2015-05-15'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_day_transform_timestamp WHERE CAST(d AS date) >= DATE '2015-05-15'"))
                .isFullyPushedDown();

        assertThat(query("SELECT * FROM test_day_transform_timestamp WHERE d >= TIMESTAMP '2015-05-15 00:00:00'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_day_transform_timestamp WHERE d >= TIMESTAMP '2015-05-15 00:00:00.000001'"))
                .isNotFullyPushedDown(FilterNode.class);

        // date()
        assertThat(query("SELECT * FROM test_day_transform_timestamp WHERE date(d) = DATE '2015-05-15'"))
                .isFullyPushedDown();

        // year()
        assertThat(query("SELECT * FROM test_day_transform_timestamp WHERE year(d) = 2015"))
                .isFullyPushedDown();

        // date_trunc
        assertThat(query("SELECT * FROM test_day_transform_timestamp WHERE date_trunc('day', d) = DATE '2015-05-15'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_day_transform_timestamp WHERE date_trunc('month', d) = DATE '2015-05-01'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_day_transform_timestamp WHERE date_trunc('year', d) = DATE '2015-01-01'"))
                .isFullyPushedDown();

        assertUpdate("DROP TABLE test_day_transform_timestamp");
    }

    @Test
    public void testDayTransformTimestampWithTimeZone()
    {
        assertUpdate("CREATE TABLE test_day_transform_timestamptz (d timestamp(6) with time zone, b integer) WITH (partitioning = ARRAY['day(d)'])");

        String values = "VALUES " +
                "(NULL, 101)," +
                "(TIMESTAMP '1969-12-25 15:13:12.876543 UTC', 8)," +
                "(TIMESTAMP '1969-12-30 18:47:33.345678 UTC', 9)," +
                "(TIMESTAMP '1969-12-31 00:00:00.000000 UTC', 10)," +
                "(TIMESTAMP '1969-12-31 05:06:07.234567 UTC', 11)," +
                "(TIMESTAMP '1970-01-01 12:03:08.456789 UTC', 12)," +
                "(TIMESTAMP '2015-01-01 10:01:23.123456 UTC', 1)," +
                "(TIMESTAMP '2015-01-01 11:10:02.987654 UTC', 2)," +
                "(TIMESTAMP '2015-01-01 12:55:00.456789 UTC', 3)," +
                "(TIMESTAMP '2015-05-15 13:05:01.234567 UTC', 4)," +
                "(TIMESTAMP '2015-05-15 14:21:02.345678 UTC', 5)," +
                "(TIMESTAMP '2020-02-21 15:11:11.876543 UTC', 6)," +
                "(TIMESTAMP '2020-02-21 16:12:12.654321 UTC', 7)";
        assertUpdate("INSERT INTO test_day_transform_timestamptz " + values, 13);
        assertThat(query("SELECT * FROM test_day_transform_timestamptz"))
                .matches(values);

        String expected = "VALUES " +
                "(NULL, BIGINT '1', NULL, NULL, 101, 101), " +
                "(DATE '1969-12-25', 1, TIMESTAMP '1969-12-25 15:13:12.876543 UTC', TIMESTAMP '1969-12-25 15:13:12.876543 UTC', 8, 8), " +
                "(DATE '1969-12-30', 1, TIMESTAMP '1969-12-30 18:47:33.345678 UTC', TIMESTAMP '1969-12-30 18:47:33.345678 UTC', 9, 9), " +
                "(DATE '1969-12-31', 2, TIMESTAMP '1969-12-31 00:00:00.000000 UTC', TIMESTAMP '1969-12-31 05:06:07.234567 UTC', 10, 11), " +
                "(DATE '1970-01-01', 1, TIMESTAMP '1970-01-01 12:03:08.456789 UTC', TIMESTAMP '1970-01-01 12:03:08.456789 UTC', 12, 12), " +
                "(DATE '2015-01-01', 3, TIMESTAMP '2015-01-01 10:01:23.123456 UTC', TIMESTAMP '2015-01-01 12:55:00.456789 UTC', 1, 3), " +
                "(DATE '2015-05-15', 2, TIMESTAMP '2015-05-15 13:05:01.234567 UTC', TIMESTAMP '2015-05-15 14:21:02.345678 UTC', 4, 5), " +
                "(DATE '2020-02-21', 2, TIMESTAMP '2020-02-21 15:11:11.876543 UTC', TIMESTAMP '2020-02-21 16:12:12.654321 UTC', 6, 7)";
        String expectedTimestampStats = "NULL, 12e0, 0.0769231e0, NULL, '1969-12-25 15:13:12.876 UTC', '2020-02-21 16:12:12.654 UTC'";
        String expectedIntegerStats = "NULL, 13e0, 0e0, NULL, '1', '101'";
        if (format == ORC) {
            expected = "VALUES " +
                    "(NULL, BIGINT '1', NULL, NULL, 101, 101), " +
                    "(DATE '1969-12-25', 1, TIMESTAMP '1969-12-25 15:13:12.876000 UTC', TIMESTAMP '1969-12-25 15:13:12.876999 UTC', 8, 8), " +
                    "(DATE '1969-12-30', 1, TIMESTAMP '1969-12-30 18:47:33.345000 UTC', TIMESTAMP '1969-12-30 18:47:33.345999 UTC', 9, 9), " +
                    "(DATE '1969-12-31', 2, TIMESTAMP '1969-12-31 00:00:00.000000 UTC', TIMESTAMP '1969-12-31 05:06:07.234999 UTC', 10, 11), " +
                    "(DATE '1970-01-01', 1, TIMESTAMP '1970-01-01 12:03:08.456000 UTC', TIMESTAMP '1970-01-01 12:03:08.456999 UTC', 12, 12), " +
                    "(DATE '2015-01-01', 3, TIMESTAMP '2015-01-01 10:01:23.123000 UTC', TIMESTAMP '2015-01-01 12:55:00.456999 UTC', 1, 3), " +
                    "(DATE '2015-05-15', 2, TIMESTAMP '2015-05-15 13:05:01.234000 UTC', TIMESTAMP '2015-05-15 14:21:02.345999 UTC', 4, 5), " +
                    "(DATE '2020-02-21', 2, TIMESTAMP '2020-02-21 15:11:11.876000 UTC', TIMESTAMP '2020-02-21 16:12:12.654999 UTC', 6, 7)";
        }
        else if (format == AVRO) {
            expected = "VALUES " +
                    "(NULL, BIGINT '1', NULL, NULL, NULL, NULL), " +
                    "(DATE '1969-12-25', 1, NULL, NULL, NULL, NULL), " +
                    "(DATE '1969-12-30', 1, NULL, NULL, NULL, NULL), " +
                    "(DATE '1969-12-31', 2, NULL, NULL, NULL, NULL), " +
                    "(DATE '1970-01-01', 1, NULL, NULL, NULL, NULL), " +
                    "(DATE '2015-01-01', 3, NULL, NULL, NULL, NULL), " +
                    "(DATE '2015-05-15', 2, NULL, NULL, NULL, NULL), " +
                    "(DATE '2020-02-21', 2, NULL, NULL, NULL, NULL)";
            expectedTimestampStats = "NULL, 12e0, 0.0769231e0, NULL, NULL, NULL";
            expectedIntegerStats = "NULL, 13e0, 0e0, NULL, NULL, NULL";
        }

        assertThat(query("SELECT partition.d_day, record_count, data.d.min, data.d.max, data.b.min, data.b.max FROM \"test_day_transform_timestamptz$partitions\""))
                .skippingTypesCheck()
                .matches(expected);

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertThat(query("SELECT * FROM test_day_transform_timestamptz WHERE day_of_week(d) = 3 AND b % 7 = 3"))
                .matches("VALUES (TIMESTAMP '1969-12-31 00:00:00.000000 UTC', 10)");

        assertThat(query("SHOW STATS FOR test_day_transform_timestamptz"))
                .skippingTypesCheck()
                .matches("VALUES " +
                        "  ('d', " + expectedTimestampStats + "), " +
                        "  ('b', " + expectedIntegerStats + "), " +
                        "  (NULL, NULL, NULL, NULL, 13e0, NULL, NULL)");

        assertThat(query("SELECT * FROM test_day_transform_timestamptz WHERE d IS NOT NULL"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_day_transform_timestamptz WHERE d IS NULL"))
                .isFullyPushedDown();

        // Tests run with non-UTC session, so timestamp_tz > a_date will not align with partition boundaries. Use with_timezone to align it.
        assertThat(query("SELECT * FROM test_day_transform_timestamptz WHERE d >= with_timezone(DATE '2015-05-15', 'UTC')"))
                .isFullyPushedDown();

        assertThat(query("SELECT * FROM test_day_transform_timestamptz WHERE CAST(d AS date) >= DATE '2015-05-15'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_day_transform_timestamptz WHERE CAST(d AS date) >= DATE '2015-05-15' AND d < TIMESTAMP '2015-05-15 02:00:00 Europe/Warsaw'"))
                // Engine can eliminate the table scan after connector accepts the filter pushdown
                .hasPlan(node(OutputNode.class, node(ValuesNode.class)))
                .returnsEmptyResult();

        assertThat(query("SELECT * FROM test_day_transform_timestamptz WHERE d >= TIMESTAMP '2015-05-15 00:00:00 UTC'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_day_transform_timestamptz WHERE d >= TIMESTAMP '2015-05-15 00:00:00.000001 UTC'"))
                .isNotFullyPushedDown(FilterNode.class);

        // date()
        assertThat(query("SELECT * FROM test_day_transform_timestamptz WHERE date(d) = DATE '2015-05-15'"))
                .isFullyPushedDown();

        // year()
        assertThat(query("SELECT * FROM test_day_transform_timestamptz WHERE year(d) = 2015"))
                .isFullyPushedDown();

        // date_trunc
        assertThat(query("SELECT * FROM test_day_transform_timestamptz WHERE date_trunc('day', d) = TIMESTAMP '2015-05-15 00:00:00.000000 UTC'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_day_transform_timestamptz WHERE date_trunc('month', d) = TIMESTAMP '2015-05-01 00:00:00.000000 UTC'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_day_transform_timestamptz WHERE date_trunc('year', d) = TIMESTAMP '2015-01-01 00:00:00.000000 UTC'"))
                .isFullyPushedDown();

        assertUpdate("DROP TABLE test_day_transform_timestamptz");
    }

    @Test
    public void testMonthTransformDate()
    {
        assertUpdate("CREATE TABLE test_month_transform_date (d DATE, b BIGINT) WITH (partitioning = ARRAY['month(d)'])");

        @Language("SQL") String values = "VALUES " +
                "(NULL, 101)," +
                "(DATE '1969-11-13', 1)," +
                "(DATE '1969-12-01', 2)," +
                "(DATE '1969-12-02', 3)," +
                "(DATE '1969-12-31', 4)," +
                "(DATE '1970-01-01', 5), " +
                "(DATE '1970-05-13', 6), " +
                "(DATE '1970-12-31', 7), " +
                "(DATE '2020-01-01', 8), " +
                "(DATE '2020-06-16', 9), " +
                "(DATE '2020-06-28', 10), " +
                "(DATE '2020-06-06', 11), " +
                "(DATE '2020-07-18', 12), " +
                "(DATE '2020-07-28', 13), " +
                "(DATE '2020-12-31', 14)";
        assertUpdate("INSERT INTO test_month_transform_date " + values, 15);
        assertQuery("SELECT * FROM test_month_transform_date", values);

        String expectedDateStats = "NULL, 14e0, 0.0666667e0, NULL, '1969-11-13', '2020-12-31'";
        String expectedBigIntStats = "NULL, 15e0, 0e0, NULL, '1', '101'";
        if (format != AVRO) {
            assertQuery(
                    "SELECT partition.d_month, record_count, data.d.min, data.d.max, data.b.min, data.b.max FROM \"test_month_transform_date$partitions\"",
                    "VALUES " +
                            "(NULL, 1, NULL, NULL, 101, 101), " +
                            "(-2, 1, DATE '1969-11-13', DATE '1969-11-13', 1, 1), " +
                            "(-1, 3, DATE '1969-12-01', DATE '1969-12-31', 2, 4), " +
                            "(0, 1, DATE '1970-01-01', DATE '1970-01-01', 5, 5), " +
                            "(4, 1, DATE '1970-05-13', DATE '1970-05-13', 6, 6), " +
                            "(11, 1, DATE '1970-12-31', DATE '1970-12-31', 7, 7), " +
                            "(600, 1, DATE '2020-01-01', DATE '2020-01-01', 8, 8), " +
                            "(605, 3, DATE '2020-06-06', DATE '2020-06-28', 9, 11), " +
                            "(606, 2, DATE '2020-07-18', DATE '2020-07-28', 12, 13), " +
                            "(611, 1, DATE '2020-12-31', DATE '2020-12-31', 14, 14)");
        }
        else {
            assertQuery(
                    "SELECT partition.d_month, record_count, data.d.min, data.d.max, data.b.min, data.b.max FROM \"test_month_transform_date$partitions\"",
                    "VALUES " +
                            "(NULL, 1, NULL, NULL, NULL, NULL), " +
                            "(-2, 1, NULL, NULL, NULL, NULL), " +
                            "(-1, 3, NULL, NULL, NULL, NULL), " +
                            "(0, 1, NULL, NULL, NULL, NULL), " +
                            "(4, 1, NULL, NULL, NULL, NULL), " +
                            "(11, 1, NULL, NULL, NULL, NULL), " +
                            "(600, 1, NULL, NULL, NULL, NULL), " +
                            "(605, 3, NULL, NULL, NULL, NULL), " +
                            "(606, 2, NULL, NULL, NULL, NULL), " +
                            "(611, 1, NULL, NULL, NULL, NULL)");
            expectedDateStats = "NULL, 14e0, 0.0666667e0, NULL, NULL, NULL";
            expectedBigIntStats = "NULL, 15e0, 0e0, NULL, NULL, NULL";
        }

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertQuery(
                "SELECT * FROM test_month_transform_date WHERE day_of_week(d) = 7 AND b % 7 = 3",
                "VALUES (DATE '2020-06-28', 10)");

        assertThat(query("SHOW STATS FOR test_month_transform_date"))
                .skippingTypesCheck()
                .matches("VALUES " +
                        "  ('d', " + expectedDateStats + "), " +
                        "  ('b', " + expectedBigIntStats + "), " +
                        "  (NULL, NULL, NULL, NULL, 15e0, NULL, NULL)");

        assertThat(query("SELECT * FROM test_month_transform_date WHERE d IS NOT NULL"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_month_transform_date WHERE d IS NULL"))
                .isFullyPushedDown();

        assertThat(query("SELECT * FROM test_month_transform_date WHERE d >= DATE '2020-06-01'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_month_transform_date WHERE d >= DATE '2020-06-02'"))
                .isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT * FROM test_month_transform_date WHERE CAST(d AS date) >= DATE '2020-06-01'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_month_transform_date WHERE CAST(d AS date) >= DATE '2020-06-02'"))
                .isNotFullyPushedDown(FilterNode.class);

        // d comparison with TIMESTAMP can be unwrapped
        assertThat(query("SELECT * FROM test_month_transform_date WHERE d >= TIMESTAMP '2015-06-01 00:00:00'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_month_transform_date WHERE d >= TIMESTAMP '2015-05-01 00:00:00.000001'"))
                .isNotFullyPushedDown(FilterNode.class);

        // year()
        assertThat(query("SELECT * FROM test_month_transform_date WHERE year(d) = 2015"))
                .isFullyPushedDown();

        // date_trunc
        assertThat(query("SELECT * FROM test_month_transform_date WHERE date_trunc('month', d) = DATE '2015-01-01'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_month_transform_date WHERE date_trunc('year', d) = DATE '2015-01-01'"))
                .isFullyPushedDown();

        if (format != AVRO) {
            assertThat(query("SHOW STATS FOR test_month_transform_date"))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "  ('d', NULL, 14e0, 0.0666667e0, NULL, '1969-11-13', '2020-12-31'), " +
                            "  ('b', NULL, 15e0, 0e0, NULL, '1', '101'), " +
                            "  (NULL, NULL, NULL, NULL, 15e0, NULL, NULL)");
        }
        else {
            assertThat(query("SHOW STATS FOR test_month_transform_date"))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "  ('d', NULL, 14e0, 0.0666667e0, NULL, NULL, NULL), " +
                            "  ('b', NULL, 15e0, 0e0, NULL, NULL, NULL), " +
                            "  (NULL, NULL, NULL, NULL, 15e0, NULL, NULL)");
        }

        assertUpdate("DROP TABLE test_month_transform_date");
    }

    @Test
    public void testMonthTransformTimestamp()
    {
        assertUpdate("CREATE TABLE test_month_transform_timestamp (d TIMESTAMP(6), b BIGINT) WITH (partitioning = ARRAY['month(d)'])");

        @Language("SQL") String values = "VALUES " +
                "(NULL, 101)," +
                "(TIMESTAMP '1969-11-15 15:13:12.876543', 8)," +
                "(TIMESTAMP '1969-11-19 18:47:33.345678', 9)," +
                "(TIMESTAMP '1969-12-01 00:00:00.000000', 10)," +
                "(TIMESTAMP '1969-12-01 05:06:07.234567', 11)," +
                "(TIMESTAMP '1970-01-01 12:03:08.456789', 12)," +
                "(TIMESTAMP '2015-01-01 10:01:23.123456', 1)," +
                "(TIMESTAMP '2015-01-01 11:10:02.987654', 2)," +
                "(TIMESTAMP '2015-01-01 12:55:00.456789', 3)," +
                "(TIMESTAMP '2015-05-15 13:05:01.234567', 4)," +
                "(TIMESTAMP '2015-05-15 14:21:02.345678', 5)," +
                "(TIMESTAMP '2020-02-21 15:11:11.876543', 6)," +
                "(TIMESTAMP '2020-02-21 16:12:12.654321', 7)";
        assertUpdate("INSERT INTO test_month_transform_timestamp " + values, 13);
        assertQuery("SELECT * FROM test_month_transform_timestamp", values);

        @Language("SQL") String expected = "VALUES " +
                "(NULL, 1, NULL, NULL, 101, 101), " +
                "(-2, 2, TIMESTAMP '1969-11-15 15:13:12.876543', TIMESTAMP '1969-11-19 18:47:33.345678', 8, 9), " +
                "(-1, 2, TIMESTAMP '1969-12-01 00:00:00.000000', TIMESTAMP '1969-12-01 05:06:07.234567', 10, 11), " +
                "(0, 1, TIMESTAMP '1970-01-01 12:03:08.456789', TIMESTAMP '1970-01-01 12:03:08.456789', 12, 12), " +
                "(540, 3, TIMESTAMP '2015-01-01 10:01:23.123456', TIMESTAMP '2015-01-01 12:55:00.456789', 1, 3), " +
                "(544, 2, TIMESTAMP '2015-05-15 13:05:01.234567', TIMESTAMP '2015-05-15 14:21:02.345678', 4, 5), " +
                "(601, 2, TIMESTAMP '2020-02-21 15:11:11.876543', TIMESTAMP '2020-02-21 16:12:12.654321', 6, 7)";
        String expectedTimestampStats = "VALUES " +
                "  ('d', NULL, 12e0, 0.0769231e0, NULL, '1969-11-15 15:13:12.876543', '2020-02-21 16:12:12.654321'), " +
                "  ('b', NULL, 13e0, 0e0, NULL, '1', '101'), " +
                "  (NULL, NULL, NULL, NULL, 13e0, NULL, NULL)";

        if (format == ORC) {
            expected = "VALUES " +
                    "(NULL, 1, NULL, NULL, 101, 101), " +
                    "(-2, 2, TIMESTAMP '1969-11-15 15:13:12.876000', TIMESTAMP '1969-11-19 18:47:33.345999', 8, 9), " +
                    "(-1, 2, TIMESTAMP '1969-12-01 00:00:00.000000', TIMESTAMP '1969-12-01 05:06:07.234999', 10, 11), " +
                    "(0, 1, TIMESTAMP '1970-01-01 12:03:08.456000', TIMESTAMP '1970-01-01 12:03:08.456999', 12, 12), " +
                    "(540, 3, TIMESTAMP '2015-01-01 10:01:23.123000', TIMESTAMP '2015-01-01 12:55:00.456999', 1, 3), " +
                    "(544, 2, TIMESTAMP '2015-05-15 13:05:01.234000', TIMESTAMP '2015-05-15 14:21:02.345999', 4, 5), " +
                    "(601, 2, TIMESTAMP '2020-02-21 15:11:11.876000', TIMESTAMP '2020-02-21 16:12:12.654999', 6, 7)";
            expectedTimestampStats = "VALUES " +
                    "  ('d', NULL, 12e0, 0.0769231e0, NULL, '1969-11-15 15:13:12.876000', '2020-02-21 16:12:12.654999'), " +
                    "  ('b', NULL, 13e0, 0e0, NULL, '1', '101'), " +
                    "  (NULL, NULL, NULL, NULL, 13e0, NULL, NULL)";
        }
        else if (format == AVRO) {
            expected = "VALUES " +
                    "(NULL, 1, NULL, NULL, NULL, NULL), " +
                    "(-2, 2, NULL, NULL, NULL, NULL), " +
                    "(-1, 2, NULL, NULL, NULL, NULL), " +
                    "(0, 1, NULL, NULL, NULL, NULL), " +
                    "(540, 3, NULL, NULL, NULL, NULL), " +
                    "(544, 2, NULL, NULL, NULL, NULL), " +
                    "(601, 2, NULL, NULL, NULL, NULL)";
            expectedTimestampStats = "VALUES " +
                    "  ('d', NULL, 12e0, 0.0769231e0, NULL, NULL, NULL), " +
                    "  ('b', NULL, 13e0, 0e0, NULL, NULL, NULL), " +
                    "  (NULL, NULL, NULL, NULL, 13e0, NULL, NULL)";
        }

        assertQuery("SELECT partition.d_month, record_count, data.d.min, data.d.max, data.b.min, data.b.max FROM \"test_month_transform_timestamp$partitions\"", expected);

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertQuery(
                "SELECT * FROM test_month_transform_timestamp WHERE day_of_week(d) = 1 AND b % 7 = 3",
                "VALUES (TIMESTAMP '1969-12-01 00:00:00.000000', 10)");

        assertThat(query("SHOW STATS FOR test_month_transform_timestamp"))
                .skippingTypesCheck()
                .matches(expectedTimestampStats);

        assertThat(query("SELECT * FROM test_month_transform_timestamp WHERE d IS NOT NULL"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_month_transform_timestamp WHERE d IS NULL"))
                .isFullyPushedDown();

        assertThat(query("SELECT * FROM test_month_transform_timestamp WHERE d >= DATE '2015-05-01'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_month_transform_timestamp WHERE d >= DATE '2015-05-02'"))
                .isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT * FROM test_month_transform_timestamp WHERE CAST(d AS date) >= DATE '2015-05-01'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_month_transform_timestamp WHERE CAST(d AS date) >= DATE '2015-05-02'"))
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query("SELECT * FROM test_month_transform_timestamp WHERE d >= TIMESTAMP '2015-05-01 00:00:00'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_month_transform_timestamp WHERE d >= TIMESTAMP '2015-05-01 00:00:00.000001'"))
                .isNotFullyPushedDown(FilterNode.class);

        // year()
        assertThat(query("SELECT * FROM test_month_transform_timestamp WHERE year(d) = 2015"))
                .isFullyPushedDown();

        // date_trunc
        assertThat(query("SELECT * FROM test_month_transform_timestamp WHERE date_trunc('month', d) = DATE '2015-05-01'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_month_transform_timestamp WHERE date_trunc('year', d) = DATE '2015-01-01'"))
                .isFullyPushedDown();

        assertUpdate("DROP TABLE test_month_transform_timestamp");
    }

    @Test
    public void testMonthTransformTimestampWithTimeZone()
    {
        assertUpdate("CREATE TABLE test_month_transform_timestamptz (d timestamp(6) with time zone, b integer) WITH (partitioning = ARRAY['month(d)'])");

        String values = "VALUES " +
                "(NULL, 101)," +
                "(TIMESTAMP '1969-11-15 15:13:12.876543 UTC', 8)," +
                "(TIMESTAMP '1969-11-19 18:47:33.345678 UTC', 9)," +
                "(TIMESTAMP '1969-12-01 00:00:00.000000 UTC', 10)," +
                "(TIMESTAMP '1969-12-01 05:06:07.234567 UTC', 11)," +
                "(TIMESTAMP '1970-01-01 12:03:08.456789 UTC', 12)," +
                "(TIMESTAMP '2015-01-01 10:01:23.123456 UTC', 1)," +
                "(TIMESTAMP '2015-01-01 11:10:02.987654 UTC', 2)," +
                "(TIMESTAMP '2015-01-01 12:55:00.456789 UTC', 3)," +
                "(TIMESTAMP '2015-05-15 13:05:01.234567 UTC', 4)," +
                "(TIMESTAMP '2015-05-15 14:21:02.345678 UTC', 5)," +
                "(TIMESTAMP '2020-02-21 15:11:11.876543 UTC', 6)," +
                "(TIMESTAMP '2020-02-21 16:12:12.654321 UTC', 7)";
        assertUpdate("INSERT INTO test_month_transform_timestamptz " + values, 13);
        assertThat(query("SELECT * FROM test_month_transform_timestamptz"))
                .matches(values);

        String expected = "VALUES " +
                "(NULL, BIGINT '1', NULL, NULL, 101, 101), " +
                "(-2, 2, TIMESTAMP '1969-11-15 15:13:12.876543 UTC', TIMESTAMP '1969-11-19 18:47:33.345678 UTC', 8, 9), " +
                "(-1, 2, TIMESTAMP '1969-12-01 00:00:00.000000 UTC', TIMESTAMP '1969-12-01 05:06:07.234567 UTC', 10, 11), " +
                "(0, 1, TIMESTAMP '1970-01-01 12:03:08.456789 UTC', TIMESTAMP '1970-01-01 12:03:08.456789 UTC', 12, 12), " +
                "(540, 3, TIMESTAMP '2015-01-01 10:01:23.123456 UTC', TIMESTAMP '2015-01-01 12:55:00.456789 UTC', 1, 3), " +
                "(544, 2, TIMESTAMP '2015-05-15 13:05:01.234567 UTC', TIMESTAMP '2015-05-15 14:21:02.345678 UTC', 4, 5), " +
                "(601, 2, TIMESTAMP '2020-02-21 15:11:11.876543 UTC', TIMESTAMP '2020-02-21 16:12:12.654321 UTC', 6, 7)";
        String expectedTimestampStats = "NULL, 12e0, 0.0769231e0, NULL, '1969-11-15 15:13:12.876 UTC', '2020-02-21 16:12:12.654 UTC'";
        String expectedIntegerStats = "NULL, 13e0, 0e0, NULL, '1', '101'";
        if (format == ORC) {
            expected = "VALUES " +
                    "(NULL, BIGINT '1', NULL, NULL, 101, 101), " +
                    "(-2, 2, TIMESTAMP '1969-11-15 15:13:12.876000 UTC', TIMESTAMP '1969-11-19 18:47:33.345999 UTC', 8, 9), " +
                    "(-1, 2, TIMESTAMP '1969-12-01 00:00:00.000000 UTC', TIMESTAMP '1969-12-01 05:06:07.234999 UTC', 10, 11), " +
                    "(0, 1, TIMESTAMP '1970-01-01 12:03:08.456000 UTC', TIMESTAMP '1970-01-01 12:03:08.456999 UTC', 12, 12), " +
                    "(540, 3, TIMESTAMP '2015-01-01 10:01:23.123000 UTC', TIMESTAMP '2015-01-01 12:55:00.456999 UTC', 1, 3), " +
                    "(544, 2, TIMESTAMP '2015-05-15 13:05:01.234000 UTC', TIMESTAMP '2015-05-15 14:21:02.345999 UTC', 4, 5), " +
                    "(601, 2, TIMESTAMP '2020-02-21 15:11:11.876000 UTC', TIMESTAMP '2020-02-21 16:12:12.654999 UTC', 6, 7)";
        }
        else if (format == AVRO) {
            expected = "VALUES " +
                    "(NULL, BIGINT '1', NULL, NULL, NULL, NULL), " +
                    "(-2, 2, NULL, NULL, NULL, NULL), " +
                    "(-1, 2, NULL, NULL, NULL, NULL), " +
                    "(0, 1, NULL, NULL, NULL, NULL), " +
                    "(540, 3, NULL, NULL, NULL, NULL), " +
                    "(544, 2, NULL, NULL, NULL, NULL), " +
                    "(601, 2, NULL, NULL, NULL, NULL)";
            expectedTimestampStats = "NULL, 12e0, 0.0769231e0, NULL, NULL, NULL";
            expectedIntegerStats = "NULL, 13e0, 0e0, NULL, NULL, NULL";
        }

        assertThat(query("SELECT partition.d_month, record_count, data.d.min, data.d.max, data.b.min, data.b.max FROM \"test_month_transform_timestamptz$partitions\""))
                .skippingTypesCheck()
                .matches(expected);

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertThat(query("SELECT * FROM test_month_transform_timestamptz WHERE day_of_week(d) = 1 AND b % 7 = 3"))
                .matches("VALUES (TIMESTAMP '1969-12-01 00:00:00.000000 UTC', 10)");

        assertThat(query("SHOW STATS FOR test_month_transform_timestamptz"))
                .skippingTypesCheck()
                .matches("VALUES " +
                        "  ('d', " + expectedTimestampStats + "), " +
                        "  ('b', " + expectedIntegerStats + "), " +
                        "  (NULL, NULL, NULL, NULL, 13e0, NULL, NULL)");

        assertThat(query("SELECT * FROM test_month_transform_timestamptz WHERE d IS NOT NULL"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_month_transform_timestamptz WHERE d IS NULL"))
                .isFullyPushedDown();

        // Tests run with non-UTC session, so timestamp_tz > a_date will not align with partition boundaries. Use with_timezone to align it.
        assertThat(query("SELECT * FROM test_month_transform_timestamptz WHERE d >= with_timezone(DATE '2015-05-01', 'UTC')"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_month_transform_timestamptz WHERE d >= with_timezone(DATE '2015-05-02', 'UTC')"))
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query("SELECT * FROM test_month_transform_timestamptz WHERE CAST(d AS date) >= DATE '2015-05-01'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_month_transform_timestamptz WHERE CAST(d AS date) >= DATE '2015-05-02'"))
                .isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT * FROM test_month_transform_timestamptz WHERE CAST(d AS date) >= DATE '2015-05-01' AND d < TIMESTAMP '2015-05-01 02:00:00 Europe/Warsaw'"))
                // Engine can eliminate the table scan after connector accepts the filter pushdown
                .hasPlan(node(OutputNode.class, node(ValuesNode.class)))
                .returnsEmptyResult();

        assertThat(query("SELECT * FROM test_month_transform_timestamptz WHERE d >= TIMESTAMP '2015-05-01 00:00:00 UTC'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_month_transform_timestamptz WHERE d >= TIMESTAMP '2015-05-01 00:00:00.000001 UTC'"))
                .isNotFullyPushedDown(FilterNode.class);

        // year()
        assertThat(query("SELECT * FROM test_month_transform_timestamptz WHERE year(d) = 2015"))
                .isFullyPushedDown();

        // date_trunc
        assertThat(query("SELECT * FROM test_month_transform_timestamptz WHERE date_trunc('month', d) = TIMESTAMP '2015-05-01 00:00:00.000000 UTC'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_month_transform_timestamptz WHERE date_trunc('year', d) = TIMESTAMP '2015-01-01 00:00:00.000000 UTC'"))
                .isFullyPushedDown();

        assertUpdate("DROP TABLE test_month_transform_timestamptz");
    }

    @Test
    public void testYearTransformDate()
    {
        assertUpdate("CREATE TABLE test_year_transform_date (d DATE, b BIGINT) WITH (partitioning = ARRAY['year(d)'])");

        @Language("SQL") String values = "VALUES " +
                "(NULL, 101)," +
                "(DATE '1968-10-13', 1), " +
                "(DATE '1969-01-01', 2), " +
                "(DATE '1969-03-15', 3), " +
                "(DATE '1970-01-01', 4), " +
                "(DATE '1970-03-05', 5), " +
                "(DATE '2015-01-01', 6), " +
                "(DATE '2015-06-16', 7), " +
                "(DATE '2015-07-28', 8), " +
                "(DATE '2016-05-15', 9), " +
                "(DATE '2016-06-06', 10), " +
                "(DATE '2020-02-21', 11), " +
                "(DATE '2020-11-10', 12)";
        assertUpdate("INSERT INTO test_year_transform_date " + values, 13);
        assertQuery("SELECT * FROM test_year_transform_date", values);

        if (format != AVRO) {
            assertQuery(
                    "SELECT partition.d_year, record_count, data.d.min, data.d.max, data.b.min, data.b.max FROM \"test_year_transform_date$partitions\"",
                    "VALUES " +
                            "(NULL, 1, NULL, NULL, 101, 101), " +
                            "(-2, 1, DATE '1968-10-13', DATE '1968-10-13', 1, 1), " +
                            "(-1, 2, DATE '1969-01-01', DATE '1969-03-15', 2, 3), " +
                            "(0, 2, DATE '1970-01-01', DATE '1970-03-05', 4, 5), " +
                            "(45, 3, DATE '2015-01-01', DATE '2015-07-28', 6, 8), " +
                            "(46, 2, DATE '2016-05-15', DATE '2016-06-06', 9, 10), " +
                            "(50, 2, DATE '2020-02-21', DATE '2020-11-10', 11, 12)");
        }
        else {
            assertQuery(
                    "SELECT partition.d_year, record_count, data.d.min, data.d.max, data.b.min, data.b.max FROM \"test_year_transform_date$partitions\"",
                    "VALUES " +
                            "(NULL, 1, NULL, NULL, NULL, NULL), " +
                            "(-2, 1, NULL, NULL, NULL, NULL), " +
                            "(-1, 2, NULL, NULL, NULL, NULL), " +
                            "(0, 2, NULL, NULL, NULL, NULl), " +
                            "(45, 3, NULL, NULL, NULL, NULL), " +
                            "(46, 2, NULL, NULL, NULL, NULL), " +
                            "(50, 2, NULL, NULL, NULL, NULL)");
        }

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertQuery(
                "SELECT * FROM test_year_transform_date WHERE day_of_week(d) = 1 AND b % 7 = 3",
                "VALUES (DATE '2016-06-06', 10)");

        if (format != AVRO) {
            assertThat(query("SHOW STATS FOR test_year_transform_date"))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "  ('d', NULL, 12e0, 0.0769231e0, NULL, '1968-10-13', '2020-11-10'), " +
                            "  ('b', NULL, 13e0, 0e0, NULL, '1', '101'), " +
                            "  (NULL, NULL, NULL, NULL, 13e0, NULL, NULL)");
        }
        else {
            assertThat(query("SHOW STATS FOR test_year_transform_date"))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "  ('d', NULL, 12e0, 0.0769231e0, NULL, NULL, NULL), " +
                            "  ('b', NULL, 13e0, 0e0, NULL, NULL, NULL), " +
                            "  (NULL, NULL, NULL, NULL, 13e0, NULL, NULL)");
        }

        assertThat(query("SELECT * FROM test_year_transform_date WHERE d IS NOT NULL"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_year_transform_date WHERE d IS NULL"))
                .isFullyPushedDown();

        assertThat(query("SELECT * FROM test_year_transform_date WHERE d >= DATE '2015-01-01'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_year_transform_date WHERE d >= DATE '2015-01-02'"))
                .isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT * FROM test_year_transform_date WHERE CAST(d AS date) >= DATE '2015-01-01'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_year_transform_date WHERE CAST(d AS date) >= DATE '2015-01-02'"))
                .isNotFullyPushedDown(FilterNode.class);

        // d comparison with TIMESTAMP can be unwrapped
        assertThat(query("SELECT * FROM test_year_transform_date WHERE d >= TIMESTAMP '2015-01-01 00:00:00'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_year_transform_date WHERE d >= TIMESTAMP '2015-01-01 00:00:00.000001'"))
                .isNotFullyPushedDown(FilterNode.class);

        // year()
        assertThat(query("SELECT * FROM test_year_transform_date WHERE year(d) = 2015"))
                .isFullyPushedDown();

        // date_trunc
        assertThat(query("SELECT * FROM test_year_transform_date WHERE date_trunc('year', d) = DATE '2015-01-01'"))
                .isFullyPushedDown();

        if (format != AVRO) {
            assertThat(query("SHOW STATS FOR test_year_transform_date"))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "  ('d', NULL, 12e0, 0.0769231e0, NULL, '1968-10-13', '2020-11-10'), " +
                            "  ('b', NULL, 13e0, 0e0, NULL, '1', '101'), " +
                            "  (NULL, NULL, NULL, NULL, 13e0, NULL, NULL)");
        }
        else {
            assertThat(query("SHOW STATS FOR test_year_transform_date"))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "  ('d', NULL, 12e0, 0.0769231e0, NULL, NULL, NULL), " +
                            "  ('b', NULL, 13e0, 0e0, NULL, NULL, NULL), " +
                            "  (NULL, NULL, NULL, NULL, 13e0, NULL, NULL)");
        }

        assertUpdate("DROP TABLE test_year_transform_date");
    }

    @Test
    public void testYearTransformTimestamp()
    {
        assertUpdate("CREATE TABLE test_year_transform_timestamp (d TIMESTAMP(6), b BIGINT) WITH (partitioning = ARRAY['year(d)'])");

        @Language("SQL") String values = "VALUES " +
                "(NULL, 101)," +
                "(TIMESTAMP '1968-03-15 15:13:12.876543', 1)," +
                "(TIMESTAMP '1968-11-19 18:47:33.345678', 2)," +
                "(TIMESTAMP '1969-01-01 00:00:00.000000', 3)," +
                "(TIMESTAMP '1969-01-01 05:06:07.234567', 4)," +
                "(TIMESTAMP '1970-01-18 12:03:08.456789', 5)," +
                "(TIMESTAMP '1970-03-14 10:01:23.123456', 6)," +
                "(TIMESTAMP '1970-08-19 11:10:02.987654', 7)," +
                "(TIMESTAMP '1970-12-31 12:55:00.456789', 8)," +
                "(TIMESTAMP '2015-05-15 13:05:01.234567', 9)," +
                "(TIMESTAMP '2015-09-15 14:21:02.345678', 10)," +
                "(TIMESTAMP '2020-02-21 15:11:11.876543', 11)," +
                "(TIMESTAMP '2020-08-21 16:12:12.654321', 12)";
        assertUpdate("INSERT INTO test_year_transform_timestamp " + values, 13);
        assertQuery("SELECT * FROM test_year_transform_timestamp", values);

        @Language("SQL") String expected = "VALUES " +
                "(NULL, 1, NULL, NULL, 101, 101), " +
                "(-2, 2, TIMESTAMP '1968-03-15 15:13:12.876543', TIMESTAMP '1968-11-19 18:47:33.345678', 1, 2), " +
                "(-1, 2, TIMESTAMP '1969-01-01 00:00:00.000000', TIMESTAMP '1969-01-01 05:06:07.234567', 3, 4), " +
                "(0, 4, TIMESTAMP '1970-01-18 12:03:08.456789', TIMESTAMP '1970-12-31 12:55:00.456789', 5, 8), " +
                "(45, 2, TIMESTAMP '2015-05-15 13:05:01.234567', TIMESTAMP '2015-09-15 14:21:02.345678', 9, 10), " +
                "(50, 2, TIMESTAMP '2020-02-21 15:11:11.876543', TIMESTAMP '2020-08-21 16:12:12.654321', 11, 12)";
        String expectedTimestampStats = "VALUES " +
                "  ('d', NULL, 12e0, 0.0769231e0, NULL, '1968-03-15 15:13:12.876543', '2020-08-21 16:12:12.654321'), " +
                "  ('b', NULL, 13e0, 0e0, NULL, '1', '101'), " +
                "  (NULL, NULL, NULL, NULL, 13e0, NULL, NULL)";

        if (format == ORC) {
            expected = "VALUES " +
                    "(NULL, 1, NULL, NULL, 101, 101), " +
                    "(-2, 2, TIMESTAMP '1968-03-15 15:13:12.876000', TIMESTAMP '1968-11-19 18:47:33.345999', 1, 2), " +
                    "(-1, 2, TIMESTAMP '1969-01-01 00:00:00.000000', TIMESTAMP '1969-01-01 05:06:07.234999', 3, 4), " +
                    "(0, 4, TIMESTAMP '1970-01-18 12:03:08.456000', TIMESTAMP '1970-12-31 12:55:00.456999', 5, 8), " +
                    "(45, 2, TIMESTAMP '2015-05-15 13:05:01.234000', TIMESTAMP '2015-09-15 14:21:02.345999', 9, 10), " +
                    "(50, 2, TIMESTAMP '2020-02-21 15:11:11.876000', TIMESTAMP '2020-08-21 16:12:12.654999', 11, 12)";
            expectedTimestampStats = "VALUES " +
                    "  ('d', NULL, 12e0, 0.0769231e0, NULL, '1968-03-15 15:13:12.876000', '2020-08-21 16:12:12.654999'), " +
                    "  ('b', NULL, 13e0, 0e0, NULL, '1', '101'), " +
                    "  (NULL, NULL, NULL, NULL, 13e0, NULL, NULL)";
        }
        else if (format == AVRO) {
            expected = "VALUES " +
                    "(NULL, 1, NULL, NULL, NULL, NULL), " +
                    "(-2, 2, NULL, NULL, NULL, NULL), " +
                    "(-1, 2, NULL, NULL, NULL, NULL), " +
                    "(0, 4, NULL, NULL, NULL, NULL), " +
                    "(45, 2, NULL, NULL, NULL, NULL), " +
                    "(50, 2, NULL, NULL, NULL, NULL)";
            expectedTimestampStats = "VALUES " +
                    "  ('d', NULL, 12e0, 0.0769231e0, NULL, NULL, NULL), " +
                    "  ('b', NULL, 13e0, 0e0, NULL, NULL, NULL), " +
                    "  (NULL, NULL, NULL, NULL, 13e0, NULL, NULL)";
        }

        assertQuery("SELECT partition.d_year, record_count, data.d.min, data.d.max, data.b.min, data.b.max FROM \"test_year_transform_timestamp$partitions\"", expected);

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertQuery(
                "SELECT * FROM test_year_transform_timestamp WHERE day_of_week(d) = 2 AND b % 7 = 3",
                "VALUES (TIMESTAMP '2015-09-15 14:21:02.345678', 10)");

        assertThat(query("SHOW STATS FOR test_year_transform_timestamp"))
                .skippingTypesCheck()
                .matches(expectedTimestampStats);

        assertThat(query("SELECT * FROM test_year_transform_timestamp WHERE d IS NOT NULL"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_year_transform_timestamp WHERE d IS NULL"))
                .isFullyPushedDown();

        assertThat(query("SELECT * FROM test_year_transform_timestamp WHERE d >= DATE '2015-01-01'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_year_transform_timestamp WHERE d >= DATE '2015-01-02'"))
                .isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT * FROM test_year_transform_timestamp WHERE CAST(d AS date) >= DATE '2015-01-01'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_year_transform_timestamp WHERE CAST(d AS date) >= DATE '2015-01-02'"))
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query("SELECT * FROM test_year_transform_timestamp WHERE d >= TIMESTAMP '2015-01-01 00:00:00'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_year_transform_timestamp WHERE d >= TIMESTAMP '2015-01-01 00:00:00.000001'"))
                .isNotFullyPushedDown(FilterNode.class);

        // year()
        assertThat(query("SELECT * FROM test_year_transform_timestamp WHERE year(d) = 2015"))
                .isFullyPushedDown();

        // date_trunc
        assertThat(query("SELECT * FROM test_year_transform_timestamp WHERE date_trunc('year', d) = DATE '2015-01-01'"))
                .isFullyPushedDown();

        assertUpdate("DROP TABLE test_year_transform_timestamp");
    }

    @Test
    public void testYearTransformTimestampWithTimeZone()
    {
        assertUpdate("CREATE TABLE test_year_transform_timestamptz (d timestamp(6) with time zone, b integer) WITH (partitioning = ARRAY['year(d)'])");

        String values = "VALUES " +
                "(NULL, 101)," +
                "(TIMESTAMP '1968-03-15 15:13:12.876543 UTC', 1)," +
                "(TIMESTAMP '1968-11-19 18:47:33.345678 UTC', 2)," +
                "(TIMESTAMP '1969-01-01 00:00:00.000000 UTC', 3)," +
                "(TIMESTAMP '1969-01-01 05:06:07.234567 UTC', 4)," +
                "(TIMESTAMP '1970-01-18 12:03:08.456789 UTC', 5)," +
                "(TIMESTAMP '1970-03-14 10:01:23.123456 UTC', 6)," +
                "(TIMESTAMP '1970-08-19 11:10:02.987654 UTC', 7)," +
                "(TIMESTAMP '1970-12-31 12:55:00.456789 UTC', 8)," +
                "(TIMESTAMP '2015-05-15 13:05:01.234567 UTC', 9)," +
                "(TIMESTAMP '2015-09-15 14:21:02.345678 UTC', 10)," +
                "(TIMESTAMP '2020-02-21 15:11:11.876543 UTC', 11)," +
                "(TIMESTAMP '2020-08-21 16:12:12.654321 UTC', 12)";
        assertUpdate("INSERT INTO test_year_transform_timestamptz " + values, 13);
        assertThat(query("SELECT * FROM test_year_transform_timestamptz"))
                .matches(values);

        String expected = "VALUES " +
                "(NULL, BIGINT '1', NULL, NULL, 101, 101), " +
                "(-2, 2, TIMESTAMP '1968-03-15 15:13:12.876543 UTC', TIMESTAMP '1968-11-19 18:47:33.345678 UTC', 1, 2), " +
                "(-1, 2, TIMESTAMP '1969-01-01 00:00:00.000000 UTC', TIMESTAMP '1969-01-01 05:06:07.234567 UTC', 3, 4), " +
                "(0, 4, TIMESTAMP '1970-01-18 12:03:08.456789 UTC', TIMESTAMP '1970-12-31 12:55:00.456789 UTC', 5, 8), " +
                "(45, 2, TIMESTAMP '2015-05-15 13:05:01.234567 UTC', TIMESTAMP '2015-09-15 14:21:02.345678 UTC', 9, 10), " +
                "(50, 2, TIMESTAMP '2020-02-21 15:11:11.876543 UTC', TIMESTAMP '2020-08-21 16:12:12.654321 UTC', 11, 12)";
        String expectedTimestampStats = "NULL, 12e0, 0.0769231e0, NULL, '1968-03-15 15:13:12.876 UTC', '2020-08-21 16:12:12.654 UTC'";
        String expectedIntegerStats = "NULL, 13e0, 0e0, NULL, '1', '101'";
        if (format == ORC) {
            expected = "VALUES " +
                    "(NULL, BIGINT '1', NULL, NULL, 101, 101), " +
                    "(-2, 2, TIMESTAMP '1968-03-15 15:13:12.876000 UTC', TIMESTAMP '1968-11-19 18:47:33.345999 UTC', 1, 2), " +
                    "(-1, 2, TIMESTAMP '1969-01-01 00:00:00.000000 UTC', TIMESTAMP '1969-01-01 05:06:07.234999 UTC', 3, 4), " +
                    "(0, 4, TIMESTAMP '1970-01-18 12:03:08.456000 UTC', TIMESTAMP '1970-12-31 12:55:00.456999 UTC', 5, 8), " +
                    "(45, 2, TIMESTAMP '2015-05-15 13:05:01.234000 UTC', TIMESTAMP '2015-09-15 14:21:02.345999 UTC', 9, 10), " +
                    "(50, 2, TIMESTAMP '2020-02-21 15:11:11.876000 UTC', TIMESTAMP '2020-08-21 16:12:12.654999 UTC', 11, 12)";
        }
        else if (format == AVRO) {
            expected = "VALUES " +
                    "(NULL, BIGINT '1', NULL, NULL, NULL, NULL), " +
                    "(-2, 2, NULL, NULL, NULL, NULL), " +
                    "(-1, 2, NULL, NULL, NULL, NULL), " +
                    "(0, 4, NULL, NULL, NULL, NULL), " +
                    "(45, 2, NULL, NULL, NULL, NULL), " +
                    "(50, 2, NULL, NULL, NULL, NULL)";
            expectedTimestampStats = "NULL, 12e0, 0.0769231e0, NULL, NULL, NULL";
            expectedIntegerStats = "NULL, 13e0, 0e0, NULL, NULL, NULL";
        }

        assertThat(query("SELECT partition.d_year, record_count, data.d.min, data.d.max, data.b.min, data.b.max FROM \"test_year_transform_timestamptz$partitions\""))
                .skippingTypesCheck()
                .matches(expected);

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertThat(query("SELECT * FROM test_year_transform_timestamptz WHERE day_of_week(d) = 2 AND b % 7 = 3"))
                .matches("VALUES (TIMESTAMP '2015-09-15 14:21:02.345678 UTC', 10)");

        assertThat(query("SHOW STATS FOR test_year_transform_timestamptz"))
                .skippingTypesCheck()
                .matches("VALUES " +
                        "  ('d', " + expectedTimestampStats + "), " +
                        "  ('b', " + expectedIntegerStats + "), " +
                        "  (NULL, NULL, NULL, NULL, 13e0, NULL, NULL)");

        assertThat(query("SELECT * FROM test_year_transform_timestamptz WHERE d IS NOT NULL"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_year_transform_timestamptz WHERE d IS NULL"))
                .isFullyPushedDown();

        // Tests run with non-UTC session, so timestamp_tz > a_date will not align with partition boundaries. Use with_timezone to align it.
        assertThat(query("SELECT * FROM test_year_transform_timestamptz WHERE d >= with_timezone(DATE '2015-01-01', 'UTC')"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_year_transform_timestamptz WHERE d >= with_timezone(DATE '2015-01-02', 'UTC')"))
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query("SELECT * FROM test_year_transform_timestamptz WHERE CAST(d AS date) >= DATE '2015-01-01'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_year_transform_timestamptz WHERE CAST(d AS date) >= DATE '2015-01-02'"))
                .isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT * FROM test_year_transform_timestamptz WHERE CAST(d AS date) >= DATE '2015-01-01' AND d < TIMESTAMP '2015-01-01 01:00:00 Europe/Warsaw'"))
                // Engine can eliminate the table scan after connector accepts the filter pushdown
                .hasPlan(node(OutputNode.class, node(ValuesNode.class)))
                .returnsEmptyResult();

        assertThat(query("SELECT * FROM test_year_transform_timestamptz WHERE d >= TIMESTAMP '2015-01-01 00:00:00 UTC'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_year_transform_timestamptz WHERE d >= TIMESTAMP '2015-01-01 00:00:00.000001 UTC'"))
                .isNotFullyPushedDown(FilterNode.class);

        // year()
        assertThat(query("SELECT * FROM test_year_transform_timestamptz WHERE year(d) = 2015"))
                .isFullyPushedDown();

        // date_trunc
        assertThat(query("SELECT * FROM test_year_transform_timestamptz WHERE date_trunc('year', d) = TIMESTAMP '2015-01-01 00:00:00.000000 UTC'"))
                .isFullyPushedDown();

        assertUpdate("DROP TABLE test_year_transform_timestamptz");
    }

    @Test
    public void testTruncateTextTransform()
    {
        assertUpdate("CREATE TABLE test_truncate_text_transform (d VARCHAR, b BIGINT) WITH (partitioning = ARRAY['truncate(d, 2)'])");
        String select = "SELECT partition.d_trunc, record_count, data.d.min AS d_min, data.d.max AS d_max, data.b.min AS b_min, data.b.max AS b_max FROM \"test_truncate_text_transform$partitions\"";

        assertUpdate("INSERT INTO test_truncate_text_transform VALUES" +
                "(NULL, 101)," +
                "('abcd', 1)," +
                "('abxy', 2)," +
                "('ab598', 3)," +
                "('Kielce', 4)," +
                "('Kiev', 5)," +
                "('Greece', 6)," +
                "('Grozny', 7)", 8);

        assertQuery("SELECT partition.d_trunc FROM \"test_truncate_text_transform$partitions\"", "VALUES NULL, 'ab', 'Ki', 'Gr'");

        assertQuery("SELECT b FROM test_truncate_text_transform WHERE substring(d, 1, 2) = 'ab'", "VALUES 1, 2, 3");
        assertQuery(
                select + " WHERE partition.d_trunc = 'ab'",
                format == AVRO ? "VALUES ('ab', 3, NULL, NULL, NULL, NULL)" : "VALUES ('ab', 3, 'ab598', 'abxy', 1, 3)");

        assertQuery("SELECT b FROM test_truncate_text_transform WHERE substring(d, 1, 2) = 'Ki'", "VALUES 4, 5");
        assertQuery(
                select + " WHERE partition.d_trunc = 'Ki'",
                format == AVRO ? "VALUES ('Ki', 2, NULL, NULL, NULL, NULL)" : "VALUES ('Ki', 2, 'Kielce', 'Kiev', 4, 5)");

        assertQuery("SELECT b FROM test_truncate_text_transform WHERE substring(d, 1, 2) = 'Gr'", "VALUES 6, 7");
        assertQuery(
                select + " WHERE partition.d_trunc = 'Gr'",
                format == AVRO ? "VALUES ('Gr', 2, NULL, NULL, NULL, NULL)" : "VALUES ('Gr', 2, 'Greece', 'Grozny', 6, 7)");

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertQuery(
                "SELECT * FROM test_truncate_text_transform WHERE length(d) = 4 AND b % 7 = 2",
                "VALUES ('abxy', 2)");

        assertThat(query("SHOW STATS FOR test_truncate_text_transform"))
                .skippingTypesCheck()
                .matches("VALUES " +
                        "  ('d', " + (format == PARQUET ? "507e0" : "NULL") + ", 7e0, " + (format == AVRO ? "0.1e0" : "0.125e0") + ", NULL, NULL, NULL), " +
                        "  ('b', NULL, 8e0, 0e0, NULL, " + (format == AVRO ? "NULL, NULL" : "'1', '101'") + "), " +
                        "  (NULL, NULL, NULL, NULL, 8e0, NULL, NULL)");

        assertThat(query("SELECT * FROM test_truncate_text_transform WHERE d IS NOT NULL"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_truncate_text_transform WHERE d IS NULL"))
                .isFullyPushedDown();

        assertThat(query("SELECT * FROM test_truncate_text_transform WHERE d >= 'ab'"))
                .isNotFullyPushedDown(FilterNode.class); // TODO subsume partition boundary filters on varchar
        // Currently, prefix-checking LIKE -> range conversion is part of DomainTranslator and doesn't allow for filter elimination. TODO subsume prefix-checking LIKE with truncate().
        assertThat(query("SELECT * FROM test_truncate_text_transform WHERE d LIKE 'ab%'"))
                .isNotFullyPushedDown(FilterNode.class);
        // condition to long to subsume, we use truncate(2)
        assertThat(query("SELECT * FROM test_truncate_text_transform WHERE d >= 'abc'"))
                .isNotFullyPushedDown(FilterNode.class);
        // condition to long to subsume, we use truncate(2)
        assertThat(query("SELECT * FROM test_truncate_text_transform WHERE d LIKE 'abc%'"))
                .isNotFullyPushedDown(FilterNode.class);

        assertUpdate("DROP TABLE test_truncate_text_transform");
    }

    @Test
    public void testTruncateIntegerTransform()
    {
        testTruncateIntegerTransform("integer");
        testTruncateIntegerTransform("bigint");
    }

    public void testTruncateIntegerTransform(String dataType)
    {
        String table = format("test_truncate_%s_transform", dataType);
        assertUpdate(format("CREATE TABLE " + table + " (d %s, b BIGINT) WITH (partitioning = ARRAY['truncate(d, 10)'])", dataType));
        String select = "SELECT partition.d_trunc, record_count, data.d.min AS d_min, data.d.max AS d_max, data.b.min AS b_min, data.b.max AS b_max FROM \"" + table + "$partitions\"";

        assertUpdate("INSERT INTO " + table + " VALUES" +
                "(NULL, 101)," +
                "(0, 1)," +
                "(1, 2)," +
                "(5, 3)," +
                "(9, 4)," +
                "(10, 5)," +
                "(11, 6)," +
                "(120, 7)," +
                "(121, 8)," +
                "(123, 9)," +
                "(-1, 10)," +
                "(-5, 11)," +
                "(-10, 12)," +
                "(-11, 13)," +
                "(-123, 14)," +
                "(-130, 15)", 16);

        assertQuery("SELECT partition.d_trunc FROM \"" + table + "$partitions\"", "VALUES NULL, 0, 10, 120, -10, -20, -130");

        assertQuery("SELECT b FROM " + table + " WHERE d IN (0, 1, 5, 9)", "VALUES 1, 2, 3, 4");
        assertQuery(
                select + " WHERE partition.d_trunc = 0",
                format == AVRO ? "VALUES (0, 4, NULL, NULL,NULL, NULL)" : "VALUES (0, 4, 0, 9, 1, 4)");

        assertQuery("SELECT b FROM " + table + " WHERE d IN (10, 11)", "VALUES 5, 6");
        assertQuery(
                select + " WHERE partition.d_trunc = 10",
                format == AVRO ? "VALUES (10, 2, NULL, NULL,NULL, NULL)" : "VALUES (10, 2, 10, 11, 5, 6)");

        assertQuery("SELECT b FROM " + table + " WHERE d IN (120, 121, 123)", "VALUES 7, 8, 9");
        assertQuery(
                select + " WHERE partition.d_trunc = 120",
                format == AVRO ? "VALUES (120, 3, NULL, NULL, NULL, NULL)" : "VALUES (120, 3, 120, 123, 7, 9)");

        assertQuery("SELECT b FROM " + table + " WHERE d IN (-1, -5, -10)", "VALUES 10, 11, 12");
        assertQuery(
                select + " WHERE partition.d_trunc = -10",
                format == AVRO ? "VALUES (-10, 3, NULL, NULL, NULL, NULL)" : "VALUES (-10, 3, -10, -1, 10, 12)");

        assertQuery("SELECT b FROM " + table + " WHERE d = -11", "VALUES 13");
        assertQuery(
                select + " WHERE partition.d_trunc = -20",
                format == AVRO ? "VALUES (-20, 1, NULL, NULL, NULL, NULL)" : "VALUES (-20, 1, -11, -11, 13, 13)");

        assertQuery("SELECT b FROM " + table + " WHERE d IN (-123, -130)", "VALUES 14, 15");
        assertQuery(
                select + " WHERE partition.d_trunc = -130",
                format == AVRO ? "VALUES (-130, 2, NULL, NULL, NULL, NULL)" : "VALUES (-130, 2, -130, -123, 14, 15)");

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertQuery(
                "SELECT * FROM " + table + " WHERE d % 10 = -1 AND b % 7 = 3",
                "VALUES (-1, 10)");

        if (format != AVRO) {
            assertThat(query("SHOW STATS FOR " + table))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "  ('d', NULL, 15e0, 0.0625e0, NULL, '-130', '123'), " +
                            "  ('b', NULL, 16e0, 0e0, NULL, '1', '101'), " +
                            "  (NULL, NULL, NULL, NULL, 16e0, NULL, NULL)");
        }
        else {
            assertThat(query("SHOW STATS FOR " + table))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "  ('d', NULL, 15e0, 0.0625e0, NULL, NULL, NULL), " +
                            "  ('b', NULL, 16e0, 0e0, NULL, NULL, NULL), " +
                            "  (NULL, NULL, NULL, NULL, 16e0, NULL, NULL)");
        }

        assertThat(query("SELECT * FROM " + table + " WHERE d IS NOT NULL"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM " + table + " WHERE d IS NULL"))
                .isFullyPushedDown();

        assertThat(query("SELECT * FROM " + table + " WHERE d >= 10"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM " + table + " WHERE d > 10"))
                .isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT * FROM " + table + " WHERE d >= 11"))
                .isNotFullyPushedDown(FilterNode.class);

        assertUpdate("DROP TABLE " + table);
    }

    @Test
    public void testTruncateDecimalTransform()
    {
        assertUpdate("CREATE TABLE test_truncate_decimal_transform (d DECIMAL(9, 2), b BIGINT) WITH (partitioning = ARRAY['truncate(d, 10)'])");
        String select = "SELECT partition.d_trunc, record_count, data.d.min AS d_min, data.d.max AS d_max, data.b.min AS b_min, data.b.max AS b_max FROM \"test_truncate_decimal_transform$partitions\"";

        assertUpdate("INSERT INTO test_truncate_decimal_transform VALUES" +
                "(NULL, 101)," +
                "(12.34, 1)," +
                "(12.30, 2)," +
                "(12.29, 3)," +
                "(0.05, 4)," +
                "(-0.05, 5)", 6);

        assertQuery("SELECT partition.d_trunc FROM \"test_truncate_decimal_transform$partitions\"", "VALUES NULL, 12.30, 12.20, 0.00, -0.10");

        assertQuery("SELECT b FROM test_truncate_decimal_transform WHERE d IN (12.34, 12.30)", "VALUES 1, 2");
        assertQuery(
                select + " WHERE partition.d_trunc = 12.30",
                format == AVRO ? "VALUES (12.30, 2, NULL, NULL, NULL, NULL)" : "VALUES (12.30, 2, 12.30, 12.34, 1, 2)");

        assertQuery("SELECT b FROM test_truncate_decimal_transform WHERE d = 12.29", "VALUES 3");
        assertQuery(
                select + " WHERE partition.d_trunc = 12.20",
                format == AVRO ? "VALUES (12.20, 1, NULL, NULL, NULL, NULL)" : "VALUES (12.20, 1, 12.29, 12.29, 3, 3)");

        assertQuery("SELECT b FROM test_truncate_decimal_transform WHERE d = 0.05", "VALUES 4");
        assertQuery(
                select + " WHERE partition.d_trunc = 0.00",
                format == AVRO ? "VALUES (0.00, 1, NULL, NULL, NULL, NULL)" : "VALUES (0.00, 1, 0.05, 0.05, 4, 4)");

        assertQuery("SELECT b FROM test_truncate_decimal_transform WHERE d = -0.05", "VALUES 5");
        assertQuery(
                select + " WHERE partition.d_trunc = -0.10",
                format == AVRO ? "VALUES (-0.10, 1, NULL, NULL, NULL, NULL)" : "VALUES (-0.10, 1, -0.05, -0.05, 5, 5)");

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertQuery(
                "SELECT * FROM test_truncate_decimal_transform WHERE d * 100 % 10 = 9 AND b % 7 = 3",
                "VALUES (12.29, 3)");

        if (format == ORC || format == PARQUET) {
            assertThat(query("SHOW STATS FOR test_truncate_decimal_transform"))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "  ('d', NULL, 5e0, 0.166667e0, NULL, '-0.05', '12.34'), " +
                            "  ('b', NULL, 6e0, 0e0, NULL, '1', '101'), " +
                            "  (NULL, NULL, NULL, NULL, 6e0, NULL, NULL)");
        }
        else if (format == AVRO) {
            assertThat(query("SHOW STATS FOR test_truncate_decimal_transform"))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "  ('d', NULL, 5e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('b', NULL, 6e0, 0e0, NULL, NULL, NULL), " +
                            "  (NULL, NULL, NULL, NULL, 6e0, NULL, NULL)");
        }

        assertThat(query("SELECT * FROM test_truncate_decimal_transform WHERE d IS NOT NULL"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_truncate_decimal_transform WHERE d IS NULL"))
                .isFullyPushedDown();

        assertThat(query("SELECT * FROM test_truncate_decimal_transform WHERE d >= 12.20"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_truncate_decimal_transform WHERE d > 12.19"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM test_truncate_decimal_transform WHERE d > 12.20"))
                .isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT * FROM test_truncate_decimal_transform WHERE d >= 12.21"))
                .isNotFullyPushedDown(FilterNode.class);

        assertUpdate("DROP TABLE test_truncate_decimal_transform");
    }

    @Test
    public void testBucketTransform()
    {
        testBucketTransformForType("DATE", "DATE '2020-05-19'", "DATE '2020-08-19'", "DATE '2020-11-19'");
        testBucketTransformForType("VARCHAR", "CAST('abcd' AS VARCHAR)", "CAST('mommy' AS VARCHAR)", "CAST('abxy' AS VARCHAR)");
        testBucketTransformForType("INTEGER", "10", "12", "20");
        testBucketTransformForType("BIGINT", "CAST(100000000 AS BIGINT)", "CAST(200000002 AS BIGINT)", "CAST(400000001 AS BIGINT)");
        testBucketTransformForType(
                "UUID",
                "CAST('206caec7-68b9-4778-81b2-a12ece70c8b1' AS UUID)",
                "CAST('906caec7-68b9-4778-81b2-a12ece70c8b1' AS UUID)",
                "CAST('406caec7-68b9-4778-81b2-a12ece70c8b1' AS UUID)");
        testBucketTransformForType("VARBINARY", "x'04'", "x'21'", "x'02'");
    }

    protected void testBucketTransformForType(
            String type,
            String value,
            String greaterValueInSameBucket,
            String valueInOtherBucket)
    {
        String tableName = format("test_bucket_transform%s", type.toLowerCase(ENGLISH));

        assertUpdate(format("CREATE TABLE %s (d %s) WITH (partitioning = ARRAY['bucket(d, 2)'])", tableName, type));
        assertUpdate(format("INSERT INTO %s VALUES (NULL), (%s), (%s), (%s)", tableName, value, greaterValueInSameBucket, valueInOtherBucket), 4);
        assertThat(query(format("SELECT * FROM %s", tableName))).matches(format("VALUES (NULL), (%s), (%s), (%s)", value, greaterValueInSameBucket, valueInOtherBucket));
        assertThat(query(format("SELECT * FROM %s WHERE d <= %s AND (rand() = 42 OR d != %s)", tableName, value, valueInOtherBucket)))
                .matches("VALUES " + value);
        assertThat(query(format("SELECT * FROM %s WHERE d >= %s AND (rand() = 42 OR d != %s)", tableName, greaterValueInSameBucket, valueInOtherBucket)))
                .matches("VALUES " + greaterValueInSameBucket);

        String selectFromPartitions = format("SELECT partition.d_bucket, record_count, data.d.min AS d_min, data.d.max AS d_max FROM \"%s$partitions\"", tableName);

        if (supportsIcebergFileStatistics(type)) {
            assertQuery(selectFromPartitions + " WHERE partition.d_bucket = 0", format("VALUES(0, %d, %s, %s)", 2, value, greaterValueInSameBucket));
            assertQuery(selectFromPartitions + " WHERE partition.d_bucket = 1", format("VALUES(1, %d, %s, %s)", 1, valueInOtherBucket, valueInOtherBucket));
        }
        else {
            assertQuery(selectFromPartitions + " WHERE partition.d_bucket = 0", format("VALUES(0, %d, null, null)", 2));
            assertQuery(selectFromPartitions + " WHERE partition.d_bucket = 1", format("VALUES(1, %d, null, null)", 1));
        }

        assertThat(query("SHOW STATS FOR " + tableName))
                .result()
                .exceptColumns("data_size", "low_value", "high_value") // these may vary between types
                .skippingTypesCheck()
                .matches("VALUES " +
                        "  ('d', 3e0, " + (format == AVRO ? "0.1e0" : "0.25e0") + ", NULL), " +
                        "  (NULL, NULL, NULL, 4e0)");

        assertThat(query("SELECT * FROM " + tableName + " WHERE d IS NULL"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM " + tableName + " WHERE d IS NOT NULL"))
                .isNotFullyPushedDown(FilterNode.class); // this could be subsumed

        // Bucketing transform doesn't allow comparison filter elimination
        assertThat(query("SELECT * FROM " + tableName + " WHERE d >= " + value))
                .isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT * FROM " + tableName + " WHERE d >= " + greaterValueInSameBucket))
                .isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT * FROM " + tableName + " WHERE d >= " + valueInOtherBucket))
                .isNotFullyPushedDown(FilterNode.class);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testApplyFilterWithNonEmptyConstraintPredicate()
    {
        assertUpdate("CREATE TABLE test_apply_functional_constraint (d VARCHAR, b BIGINT) WITH (partitioning = ARRAY['bucket(d, 2)'])");
        assertUpdate(
                "INSERT INTO test_apply_functional_constraint VALUES" +
                        "('abcd', 1)," +
                        "('abxy', 2)," +
                        "('ab598', 3)," +
                        "('Kielce', 4)," +
                        "('Kiev', 5)," +
                        "('Greece', 6)," +
                        "('Grozny', 7)",
                7);

        assertQuery(
                "SELECT * FROM test_apply_functional_constraint WHERE length(d) = 4 AND b % 7 = 2",
                "VALUES ('abxy', 2)");

        String expected = switch (format) {
            case ORC -> "VALUES " +
                    "  ('d', NULL, 7e0, 0e0, NULL, NULL, NULL), " +
                    "  ('b', NULL, 7e0, 0e0, NULL, '1', '7'), " +
                    "  (NULL, NULL, NULL, NULL, 7e0, NULL, NULL)";
            case PARQUET -> "VALUES " +
                    "  ('d', 342e0, 7e0, 0e0, NULL, NULL, NULL), " +
                    "  ('b', NULL, 7e0, 0e0, NULL, '1', '7'), " +
                    "  (NULL, NULL, NULL, NULL, 7e0, NULL, NULL)";
            case AVRO -> "VALUES " +
                    "  ('d', NULL, 7e0, 0e0, NULL, NULL, NULL), " +
                    "  ('b', NULL, 7e0, 0e0, NULL, NULL, NULL), " +
                    "  (NULL, NULL, NULL, NULL, 7e0, NULL, NULL)";
        };
        assertThat(query("SHOW STATS FOR test_apply_functional_constraint"))
                .skippingTypesCheck()
                .matches(expected);

        assertUpdate("DROP TABLE test_apply_functional_constraint");
    }

    @Test
    public void testVoidTransform()
    {
        assertUpdate("CREATE TABLE test_void_transform (d VARCHAR, b BIGINT) WITH (partitioning = ARRAY['void(d)'])");
        String values = "VALUES " +
                "('abcd', 1)," +
                "('abxy', 2)," +
                "('ab598', 3)," +
                "('mommy', 4)," +
                "('Warsaw', 5)," +
                "(NULL, 6)," +
                "(NULL, 7)";
        assertUpdate("INSERT INTO test_void_transform " + values, 7);
        assertQuery("SELECT * FROM test_void_transform", values);

        assertQuery("SELECT COUNT(*) FROM \"test_void_transform$partitions\"", "SELECT 1");
        if (format != AVRO) {
            assertQuery(
                    "SELECT partition.d_null, record_count, file_count, data.d.min, data.d.max, data.d.null_count, data.d.nan_count, data.b.min, data.b.max, data.b.null_count, data.b.nan_count FROM \"test_void_transform$partitions\"",
                    "VALUES (NULL, 7, 1, 'Warsaw', 'mommy', 2, NULL, 1, 7, 0, NULL)");
        }
        else {
            assertQuery(
                    "SELECT partition.d_null, record_count, file_count, data.d.min, data.d.max, data.d.null_count, data.d.nan_count, data.b.min, data.b.max, data.b.null_count, data.b.nan_count FROM \"test_void_transform$partitions\"",
                    "VALUES (NULL, 7, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
        }

        assertQuery(
                "SELECT d, b FROM test_void_transform WHERE d IS NOT NULL",
                "VALUES " +
                        "('abcd', 1)," +
                        "('abxy', 2)," +
                        "('ab598', 3)," +
                        "('mommy', 4)," +
                        "('Warsaw', 5)");

        assertQuery("SELECT b FROM test_void_transform WHERE d IS NULL", "VALUES 6, 7");

        if (format != AVRO) {
            assertThat(query("SHOW STATS FOR test_void_transform"))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "  ('d', " + (format == PARQUET ? "194e0" : "NULL") + ", 5e0, 0.2857142857142857, NULL, NULL, NULL), " +
                            "  ('b', NULL, 7e0, 0e0, NULL, '1', '7'), " +
                            "  (NULL, NULL, NULL, NULL, 7e0, NULL, NULL)");
        }
        else {
            assertThat(query("SHOW STATS FOR test_void_transform"))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "  ('d', NULL, 5e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('b', NULL, 7e0, 0e0, NULL, NULL, NULL), " +
                            "  (NULL, NULL, NULL, NULL, 7e0, NULL, NULL)");
        }

        // Void transform doesn't allow filter elimination
        assertThat(query("SELECT * FROM test_void_transform WHERE d IS NULL"))
                .isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT * FROM test_void_transform WHERE d IS NOT NULL"))
                .isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT * FROM test_void_transform WHERE d >= 'abc'"))
                .isNotFullyPushedDown(FilterNode.class);

        assertUpdate("DROP TABLE " + "test_void_transform");
    }

    @Test
    public void testMetadataDeleteSimple()
    {
        assertUpdate("CREATE TABLE test_metadata_delete_simple (col1 BIGINT, col2 BIGINT) WITH (partitioning = ARRAY['col1'])");
        assertUpdate("INSERT INTO test_metadata_delete_simple VALUES(1, 100), (1, 101), (1, 102), (2, 200), (2, 201), (3, 300)", 6);
        assertQuery("SELECT sum(col2) FROM test_metadata_delete_simple", "SELECT 1004");
        assertQuery("SELECT count(*) FROM \"test_metadata_delete_simple$partitions\"", "SELECT 3");
        assertUpdate("DELETE FROM test_metadata_delete_simple WHERE col1 = 1", 3);
        assertQuery("SELECT sum(col2) FROM test_metadata_delete_simple", "SELECT 701");
        assertQuery("SELECT count(*) FROM \"test_metadata_delete_simple$partitions\"", "SELECT 2");
        assertUpdate("DROP TABLE test_metadata_delete_simple");
    }

    @Test
    public void testMetadataDelete()
    {
        assertUpdate("CREATE TABLE test_metadata_delete (" +
                "  orderkey BIGINT," +
                "  linenumber INTEGER," +
                "  linestatus VARCHAR" +
                ") " +
                "WITH (" +
                "  partitioning = ARRAY[ 'linenumber', 'linestatus' ]" +
                ")");

        assertUpdate(
                "" +
                        "INSERT INTO test_metadata_delete " +
                        "SELECT orderkey, linenumber, linestatus " +
                        "FROM tpch.tiny.lineitem",
                "SELECT count(*) FROM lineitem");

        assertQuery("SELECT COUNT(*) FROM \"test_metadata_delete$partitions\"", "SELECT 14");

        assertUpdate("DELETE FROM test_metadata_delete WHERE linestatus = 'F' AND linenumber = 3", 5378);
        assertQuery("SELECT * FROM test_metadata_delete", "SELECT orderkey, linenumber, linestatus FROM lineitem WHERE linestatus <> 'F' or linenumber <> 3");
        assertQuery("SELECT count(*) FROM \"test_metadata_delete$partitions\"", "SELECT 13");

        assertUpdate("DELETE FROM test_metadata_delete WHERE linestatus='O'", 30049);
        assertQuery("SELECT count(*) FROM \"test_metadata_delete$partitions\"", "SELECT 6");
        assertQuery("SELECT * FROM test_metadata_delete", "SELECT orderkey, linenumber, linestatus FROM lineitem WHERE linestatus <> 'O' AND linenumber <> 3");

        assertUpdate("DROP TABLE test_metadata_delete");
    }

    @Test
    public void testInSet()
    {
        testInSet(31);
        testInSet(35);
    }

    private void testInSet(int inCount)
    {
        String values = range(1, inCount + 1)
                .mapToObj(n -> format("(%s, %s)", n, n + 10))
                .collect(joining(", "));
        String inList = range(1, inCount + 1)
                .mapToObj(Integer::toString)
                .collect(joining(", "));

        assertUpdate("CREATE TABLE test_in_set (col1 INTEGER, col2 BIGINT)");
        assertUpdate(format("INSERT INTO test_in_set VALUES %s", values), inCount);
        // This proves that SELECTs with large IN phrases work correctly
        computeActual(format("SELECT col1 FROM test_in_set WHERE col1 IN (%s)", inList));
        assertUpdate("DROP TABLE test_in_set");
    }

    @Test
    public void testBasicTableStatistics()
    {
        String tableName = "test_basic_table_statistics";
        assertUpdate(format("CREATE TABLE %s (col REAL)", tableName));

        assertThat(query("SHOW STATS FOR " + tableName))
                .skippingTypesCheck()
                .matches("VALUES " +
                        "  ('col', 0e0, 0e0, 1e0, NULL, NULL, NULL), " +
                        "  (NULL, NULL, NULL, NULL, 0e0, NULL, NULL)");

        assertUpdate("INSERT INTO " + tableName + " VALUES -10", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES 100", 1);

        MaterializedResult result = computeActual("SHOW STATS FOR " + tableName);
        MaterializedResult expectedStatistics =
                resultBuilder(getSession(), VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("col", null, 2.0, 0.0, null, "-10.0", "100.0")
                        .row(null, null, null, null, 2.0, null, null)
                        .build();
        if (format == AVRO) {
            expectedStatistics =
                    resultBuilder(getSession(), VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                            .row("col", null, 2.0, 0.0, null, null, null)
                            .row(null, null, null, null, 2.0, null, null)
                            .build();
        }
        assertThat(result).containsExactlyElementsOf(expectedStatistics);

        assertUpdate("INSERT INTO " + tableName + " VALUES 200", 1);

        result = computeActual("SHOW STATS FOR " + tableName);
        expectedStatistics =
                resultBuilder(getSession(), VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("col", null, 3.0, 0.0, null, "-10.0", "200.0")
                        .row(null, null, null, null, 3.0, null, null)
                        .build();
        if (format == AVRO) {
            expectedStatistics =
                    resultBuilder(getSession(), VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                            .row("col", null, 3.0, 0.0, null, null, null)
                            .row(null, null, null, null, 3.0, null, null)
                            .build();
        }
        assertThat(result).containsExactlyElementsOf(expectedStatistics);

        assertUpdate("DROP TABLE " + tableName);
    }

    /**
     * @see TestIcebergStatistics
     */
    @Test
    public void testBasicAnalyze()
    {
        Session defaultSession = getSession();
        String catalog = defaultSession.getCatalog().orElseThrow();
        Session extendedStatisticsDisabled = Session.builder(defaultSession)
                .setCatalogSessionProperty(catalog, EXTENDED_STATISTICS_ENABLED, "false")
                .build();
        String tableName = "test_basic_analyze";

        assertUpdate(defaultSession, "CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.region", 5);

        String statsWithoutNdv = format == AVRO
                ? ("VALUES " +
                "  ('regionkey', NULL, NULL, NULL, NULL, NULL, NULL), " +
                "  ('name', NULL, NULL, NULL, NULL, NULL, NULL), " +
                "  ('comment', NULL, NULL, NULL, NULL, NULL, NULL), " +
                "  (NULL, NULL, NULL, NULL, 5e0, NULL, NULL)")
                : ("VALUES " +
                "  ('regionkey', NULL, NULL, 0e0, NULL, '0', '4'), " +
                "  ('name', " + (format == PARQUET ? "224e0" : "NULL") + ", NULL, 0e0, NULL, NULL, NULL), " +
                "  ('comment', " + (format == PARQUET ? "626e0" : "NULL") + ", NULL, 0e0, NULL, NULL, NULL), " +
                "  (NULL, NULL, NULL, NULL, 5e0, NULL, NULL)");

        String statsWithNdv = format == AVRO
                ? ("VALUES " +
                "  ('regionkey', NULL, 5e0, 0e0, NULL, NULL, NULL), " +
                "  ('name', NULL, 5e0, 0e0, NULL, NULL, NULL), " +
                "  ('comment', NULL, 5e0, 0e0, NULL, NULL, NULL), " +
                "  (NULL, NULL, NULL, NULL, 5e0, NULL, NULL)")
                : ("VALUES " +
                "  ('regionkey', NULL, 5e0, 0e0, NULL, '0', '4'), " +
                "  ('name', " + (format == PARQUET ? "224e0" : "NULL") + ", 5e0, 0e0, NULL, NULL, NULL), " +
                "  ('comment', " + (format == PARQUET ? "626e0" : "NULL") + ", 5e0, 0e0, NULL, NULL, NULL), " +
                "  (NULL, NULL, NULL, NULL, 5e0, NULL, NULL)");

        assertThat(query(defaultSession, "SHOW STATS FOR " + tableName)).skippingTypesCheck().matches(statsWithNdv);
        assertThat(query(extendedStatisticsDisabled, "SHOW STATS FOR " + tableName)).skippingTypesCheck().matches(statsWithoutNdv);

        // ANALYZE can be disabled.
        assertQueryFails(
                extendedStatisticsDisabled,
                "ANALYZE " + tableName,
                "\\QAnalyze is not enabled. You can enable analyze using iceberg.extended-statistics.enabled config or extended_statistics_enabled catalog session property");

        // ANALYZE the table
        assertUpdate(defaultSession, "ANALYZE " + tableName);
        // After ANALYZE, NDV information present
        assertThat(query(defaultSession, "SHOW STATS FOR " + tableName))
                .skippingTypesCheck()
                .matches(statsWithNdv);
        // NDV information is not present in a session with extended statistics disabled
        assertThat(query(extendedStatisticsDisabled, "SHOW STATS FOR " + tableName))
                .skippingTypesCheck()
                .matches(statsWithoutNdv);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testMultipleColumnTableStatistics()
    {
        String tableName = "test_multiple_table_statistics";
        assertUpdate(format("CREATE TABLE %s (col1 REAL, col2 INTEGER, col3 DATE)", tableName));
        assertUpdate("INSERT INTO " + tableName + " VALUES (-10, -1, DATE '2019-06-28')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (100, 10, DATE '2020-01-01')", 1);

        MaterializedResult result = computeActual("SHOW STATS FOR " + tableName);

        MaterializedResult expectedStatistics =
                resultBuilder(getSession(), VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("col1", null, 2.0, 0.0, null, "-10.0", "100.0")
                        .row("col2", null, 2.0, 0.0, null, "-1", "10")
                        .row("col3", null, 2.0, 0.0, null, "2019-06-28", "2020-01-01")
                        .row(null, null, null, null, 2.0, null, null)
                        .build();
        if (format == AVRO) {
            expectedStatistics = resultBuilder(getSession(), VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                    .row("col1", null, 2.0, 0.0, null, null, null)
                    .row("col2", null, 2.0, 0.0, null, null, null)
                    .row("col3", null, 2.0, 0.0, null, null, null)
                    .row(null, null, null, null, 2.0, null, null)
                    .build();
        }
        assertThat(result).containsExactlyElementsOf(expectedStatistics);

        assertUpdate("INSERT INTO " + tableName + " VALUES (200, 20, DATE '2020-06-28')", 1);
        result = computeActual("SHOW STATS FOR " + tableName);
        expectedStatistics =
                resultBuilder(getSession(), VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("col1", null, 3.0, 0.0, null, "-10.0", "200.0")
                        .row("col2", null, 3.0, 0.0, null, "-1", "20")
                        .row("col3", null, 3.0, 0.0, null, "2019-06-28", "2020-06-28")
                        .row(null, null, null, null, 3.0, null, null)
                        .build();
        if (format == AVRO) {
            expectedStatistics =
                    resultBuilder(getSession(), VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                            .row("col1", null, 3.0, 0.0, null, null, null)
                            .row("col2", null, 3.0, 0.0, null, null, null)
                            .row("col3", null, 3.0, 0.0, null, null, null)
                            .row(null, null, null, null, 3.0, null, null)
                            .build();
        }
        assertThat(result).containsExactlyElementsOf(expectedStatistics);

        assertUpdate("INSERT INTO " + tableName + " VALUES " + IntStream.rangeClosed(21, 25)
                .mapToObj(i -> format("(200, %d, DATE '2020-07-%d')", i, i))
                .collect(joining(", ")), 5);

        assertUpdate("INSERT INTO " + tableName + " VALUES " + IntStream.rangeClosed(26, 30)
                .mapToObj(i -> format("(NULL, %d, DATE '2020-06-%d')", i, i))
                .collect(joining(", ")), 5);

        result = computeActual("SHOW STATS FOR " + tableName);

        expectedStatistics =
                resultBuilder(getSession(), VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("col1", null, 3.0, 5.0 / 13.0, null, "-10.0", "200.0")
                        .row("col2", null, 13.0, 0.0, null, "-1", "30")
                        .row("col3", null, 12.0, 0.0, null, "2019-06-28", "2020-07-25")
                        .row(null, null, null, null, 13.0, null, null)
                        .build();
        if (format == AVRO) {
            expectedStatistics =
                    resultBuilder(getSession(), VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                            .row("col1", null, 3.0, 0.1, null, null, null)
                            .row("col2", null, 13.0, 0.0, null, null, null)
                            .row("col3", null, 12.0, 1.0 / 13.0, null, null, null)
                            .row(null, null, null, null, 13.0, null, null)
                            .build();
        }
        assertThat(result).containsExactlyElementsOf(expectedStatistics);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testPartitionedTableStatistics()
    {
        assertUpdate("CREATE TABLE iceberg.tpch.test_partitioned_table_statistics (col1 REAL, col2 BIGINT) WITH (partitioning = ARRAY['col2'])");

        assertUpdate("INSERT INTO test_partitioned_table_statistics VALUES (-10, -1)", 1);
        assertUpdate("INSERT INTO test_partitioned_table_statistics VALUES (100, 10)", 1);

        MaterializedResult result = computeActual("SHOW STATS FOR iceberg.tpch.test_partitioned_table_statistics");
        assertThat(result.getRowCount()).isEqualTo(3);

        MaterializedRow row0 = result.getMaterializedRows().get(0);
        assertThat(row0.getField(0)).isEqualTo("col1");
        assertThat(row0.getField(3)).isEqualTo(0.0);
        if (format != AVRO) {
            assertThat(row0.getField(5)).isEqualTo("-10.0");
            assertThat(row0.getField(6)).isEqualTo("100.0");
        }
        else {
            assertThat(row0.getField(5)).isNull();
            assertThat(row0.getField(6)).isNull();
        }

        MaterializedRow row1 = result.getMaterializedRows().get(1);
        assertThat(row1.getField(0)).isEqualTo("col2");
        assertThat(row1.getField(3)).isEqualTo(0.0);
        if (format != AVRO) {
            assertThat(row1.getField(5)).isEqualTo("-1");
            assertThat(row1.getField(6)).isEqualTo("10");
        }
        else {
            assertThat(row0.getField(5)).isNull();
            assertThat(row0.getField(6)).isNull();
        }

        MaterializedRow row2 = result.getMaterializedRows().get(2);
        assertThat(row2.getField(4)).isEqualTo(2.0);

        assertUpdate("INSERT INTO test_partitioned_table_statistics VALUES " + IntStream.rangeClosed(1, 5)
                .mapToObj(i -> format("(%d, 10)", i + 100))
                .collect(joining(", ")), 5);

        assertUpdate("INSERT INTO test_partitioned_table_statistics VALUES " + IntStream.rangeClosed(6, 10)
                .mapToObj(i -> "(NULL, 10)")
                .collect(joining(", ")), 5);

        result = computeActual("SHOW STATS FOR iceberg.tpch.test_partitioned_table_statistics");
        assertThat(result.getRowCount()).isEqualTo(3);
        row0 = result.getMaterializedRows().get(0);
        assertThat(row0.getField(0)).isEqualTo("col1");
        if (format != AVRO) {
            assertThat((double) row0.getField(3)).isCloseTo(5.0 / 12.0, offset(1e-10));
            assertThat(row0.getField(5)).isEqualTo("-10.0");
            assertThat(row0.getField(6)).isEqualTo("105.0");
        }
        else {
            assertThat(row0.getField(3)).isEqualTo(0.1);
            assertThat(row0.getField(5)).isNull();
            assertThat(row0.getField(6)).isNull();
        }

        row1 = result.getMaterializedRows().get(1);
        assertThat(row1.getField(0)).isEqualTo("col2");
        if (format != AVRO) {
            assertThat(row1.getField(3)).isEqualTo(0.0);
            assertThat(row1.getField(5)).isEqualTo("-1");
            assertThat(row1.getField(6)).isEqualTo("10");
        }
        else {
            assertThat(row0.getField(3)).isEqualTo(0.1);
            assertThat(row0.getField(5)).isNull();
            assertThat(row0.getField(6)).isNull();
        }

        row2 = result.getMaterializedRows().get(2);
        assertThat(row2.getField(4)).isEqualTo(12.0);

        assertUpdate("INSERT INTO test_partitioned_table_statistics VALUES " + IntStream.rangeClosed(6, 10)
                .mapToObj(i -> "(100, NULL)")
                .collect(joining(", ")), 5);

        result = computeActual("SHOW STATS FOR iceberg.tpch.test_partitioned_table_statistics");
        row0 = result.getMaterializedRows().get(0);
        assertThat(row0.getField(0)).isEqualTo("col1");
        if (format != AVRO) {
            assertThat(row0.getField(3)).isEqualTo(5.0 / 17.0);
            assertThat(row0.getField(5)).isEqualTo("-10.0");
            assertThat(row0.getField(6)).isEqualTo("105.0");
        }
        else {
            assertThat(row0.getField(3)).isEqualTo(0.1);
            assertThat(row0.getField(5)).isNull();
            assertThat(row0.getField(6)).isNull();
        }

        row1 = result.getMaterializedRows().get(1);
        assertThat(row1.getField(0)).isEqualTo("col2");
        if (format != AVRO) {
            assertThat(row1.getField(3)).isEqualTo(5.0 / 17.0);
            assertThat(row1.getField(5)).isEqualTo("-1");
            assertThat(row1.getField(6)).isEqualTo("10");
        }
        else {
            assertThat(row0.getField(3)).isEqualTo(0.1);
            assertThat(row0.getField(5)).isNull();
            assertThat(row0.getField(6)).isNull();
        }

        row2 = result.getMaterializedRows().get(2);
        assertThat(row2.getField(4)).isEqualTo(17.0);

        assertUpdate("DROP TABLE iceberg.tpch.test_partitioned_table_statistics");
    }

    @Test
    public void testPredicatePushdown()
    {
        QualifiedObjectName tableName = new QualifiedObjectName("iceberg", "tpch", "test_predicate");
        assertUpdate(format("CREATE TABLE %s (col1 BIGINT, col2 BIGINT, col3 BIGINT) WITH (partitioning = ARRAY['col2', 'col3'])", tableName));
        assertUpdate(format("INSERT INTO %s VALUES (1, 10, 100)", tableName), 1L);
        assertUpdate(format("INSERT INTO %s VALUES (2, 20, 200)", tableName), 1L);

        assertQuery(format("SELECT * FROM %s WHERE col1 = 1", tableName), "VALUES (1, 10, 100)");
        assertFilterPushdown(
                tableName,
                ImmutableMap.of("col1", singleValue(BIGINT, 1L)),
                ImmutableMap.of(),
                ImmutableMap.of("col1", singleValue(BIGINT, 1L)));

        assertQuery(format("SELECT * FROM %s WHERE col2 = 10", tableName), "VALUES (1, 10, 100)");
        assertFilterPushdown(
                tableName,
                ImmutableMap.of("col2", singleValue(BIGINT, 10L)),
                ImmutableMap.of("col2", singleValue(BIGINT, 10L)),
                ImmutableMap.of());

        assertQuery(format("SELECT * FROM %s WHERE col1 = 1 AND col2 = 10", tableName), "VALUES (1, 10, 100)");
        assertFilterPushdown(
                tableName,
                ImmutableMap.of("col1", singleValue(BIGINT, 1L), "col2", singleValue(BIGINT, 10L)),
                ImmutableMap.of("col2", singleValue(BIGINT, 10L)),
                ImmutableMap.of("col1", singleValue(BIGINT, 1L)));

        // Assert pushdown for an IN predicate with value count above the default compaction threshold
        List<Long> values = LongStream.range(1L, 1010L).boxed()
                .filter(index -> index != 20L)
                .collect(toImmutableList());
        assertThat(values).hasSizeGreaterThan(ICEBERG_DOMAIN_COMPACTION_THRESHOLD);
        String valuesString = join(",", values.stream().map(Object::toString).collect(toImmutableList()));
        String inPredicate = "%s IN (" + valuesString + ")";
        assertQuery(
                format("SELECT * FROM %s WHERE %s AND %s", tableName, format(inPredicate, "col1"), format(inPredicate, "col2")),
                "VALUES (1, 10, 100)");

        assertFilterPushdown(
                tableName,
                ImmutableMap.of("col1", multipleValues(BIGINT, values), "col2", multipleValues(BIGINT, values)),
                ImmutableMap.of("col2", multipleValues(BIGINT, values)),
                // Unenforced predicate is simplified during split generation, but not reflected here
                ImmutableMap.of("col1", multipleValues(BIGINT, values)));

        assertUpdate("DROP TABLE " + tableName.objectName());
    }

    @Test
    public void testPredicateOnDataColumnIsNotPushedDown()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_predicate_on_data_column_is_not_pushed_down",
                "(a integer)")) {
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE a = 10"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertUpdate("INSERT INTO " + testTable.getName() + " VALUES 10", 1);
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE a = 10"))
                    .isNotFullyPushedDown(FilterNode.class);
        }
    }

    @Test
    public void testPredicatesWithStructuralTypes()
    {
        String tableName = "test_predicate_with_structural_types";
        assertUpdate("CREATE TABLE " + tableName + " (id INT, array_t ARRAY(BIGINT), map_t MAP(BIGINT, BIGINT), struct_t ROW(f1 BIGINT, f2 BIGINT))");
        assertUpdate("INSERT INTO " + tableName + " VALUES " +
                        "(1, ARRAY[1, 2, 3], MAP(ARRAY[1,3], ARRAY[2,4]), ROW(1, 2)), " +
                        "(11, ARRAY[11, 12, 13], MAP(ARRAY[11, 13], ARRAY[12, 14]), ROW(11, 12)), " +
                        "(11, ARRAY[111, 112, 113], MAP(ARRAY[111, 13], ARRAY[112, 114]), ROW(111, 112)), " +
                        "(21, ARRAY[21, 22, 23], MAP(ARRAY[21, 23], ARRAY[22, 24]), ROW(21, 22))",
                4);

        assertQuery("SELECT id FROM " + tableName + " WHERE array_t = ARRAY[1, 2, 3]", "VALUES 1");
        assertQuery("SELECT id FROM " + tableName + " WHERE map_t = MAP(ARRAY[11, 13], ARRAY[12, 14])", "VALUES 11");
        assertQuery("SELECT id FROM " + tableName + " WHERE struct_t = ROW(21, 22)", "VALUES 21");
        assertQuery("SELECT struct_t.f1  FROM " + tableName + " WHERE id = 11 AND map_t = MAP(ARRAY[11, 13], ARRAY[12, 14])", "VALUES 11");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testPartitionsTableWithColumnNameConflict()
    {
        testPartitionsTableWithColumnNameConflict(true);
        testPartitionsTableWithColumnNameConflict(false);
    }

    private void testPartitionsTableWithColumnNameConflict(boolean partitioned)
    {
        assertUpdate("DROP TABLE IF EXISTS test_partitions_with_conflict");
        assertUpdate("CREATE TABLE test_partitions_with_conflict (" +
                " p integer, " +
                " row_count integer, " +
                " record_count integer, " +
                " file_count integer, " +
                " total_size integer " +
                ") " +
                (partitioned ? "WITH(partitioning = ARRAY['p'])" : ""));

        assertUpdate("INSERT INTO test_partitions_with_conflict VALUES (11, 12, 13, 14, 15)", 1);

        // sanity check
        assertThat(query("SELECT * FROM test_partitions_with_conflict"))
                .matches("VALUES (11, 12, 13, 14, 15)");

        // test $partitions
        if (format != AVRO) {
            assertThat(query("SELECT * FROM \"test_partitions_with_conflict$partitions\""))
                    .matches("SELECT " +
                            (partitioned ? "CAST(ROW(11) AS row(p integer)), " : "") +
                            "BIGINT '1', " +
                            "BIGINT '1', " +
                            // total_size is not exactly deterministic, so grab whatever value there is
                            "(SELECT total_size FROM \"test_partitions_with_conflict$partitions\"), " +
                            "CAST(" +
                            "  ROW (" +
                            (partitioned ? "" : "  ROW(11, 11, 0, NULL), ") +
                            "    ROW(12, 12, 0, NULL), " +
                            "    ROW(13, 13, 0, NULL), " +
                            "    ROW(14, 14, 0, NULL), " +
                            "    ROW(15, 15, 0, NULL) " +
                            "  ) " +
                            "  AS row(" +
                            (partitioned ? "" : "    p row(min integer, max integer, null_count bigint, nan_count bigint), ") +
                            "    row_count row(min integer, max integer, null_count bigint, nan_count bigint), " +
                            "    record_count row(min integer, max integer, null_count bigint, nan_count bigint), " +
                            "    file_count row(min integer, max integer, null_count bigint, nan_count bigint), " +
                            "    total_size row(min integer, max integer, null_count bigint, nan_count bigint) " +
                            "  )" +
                            ")");
        }
        else {
            assertThat(query("SELECT * FROM \"test_partitions_with_conflict$partitions\""))
                    .matches("SELECT " +
                            (partitioned ? "CAST(ROW(11) AS row(p integer)), " : "") +
                            "BIGINT '1', " +
                            "BIGINT '1', " +
                            // total_size is not exactly deterministic, so grab whatever value there is
                            "(SELECT total_size FROM \"test_partitions_with_conflict$partitions\"), " +
                            "CAST(" +
                            " NULL AS row(" +
                            (partitioned ? "" : "    p row(min integer, max integer, null_count bigint, nan_count bigint), ") +
                            "    row_count row(min integer, max integer, null_count bigint, nan_count bigint), " +
                            "    record_count row(min integer, max integer, null_count bigint, nan_count bigint), " +
                            "    file_count row(min integer, max integer, null_count bigint, nan_count bigint), " +
                            "    total_size row(min integer, max integer, null_count bigint, nan_count bigint) " +
                            "  )" +
                            ")");
        }

        assertUpdate("DROP TABLE test_partitions_with_conflict");
    }

    private void assertFilterPushdown(
            QualifiedObjectName tableName,
            Map<String, Domain> filter,
            Map<String, Domain> expectedEnforcedPredicate,
            Map<String, Domain> expectedUnenforcedPredicate)
    {
        Metadata metadata = getQueryRunner().getPlannerContext().getMetadata();

        newTransaction().execute(getSession(), session -> {
            TableHandle table = metadata.getTableHandle(session, tableName)
                    .orElseThrow(() -> new TableNotFoundException(tableName.asSchemaTableName()));

            Map<String, ColumnHandle> columns = metadata.getColumnHandles(session, table);
            TupleDomain<ColumnHandle> domains = TupleDomain.withColumnDomains(
                    filter.entrySet().stream()
                            .collect(toImmutableMap(entry -> columns.get(entry.getKey()), Map.Entry::getValue)));

            Optional<ConstraintApplicationResult<TableHandle>> result = metadata.applyFilter(session, table, new Constraint(domains));

            assertThat((expectedUnenforcedPredicate == null && expectedEnforcedPredicate == null)).isEqualTo(result.isEmpty());

            if (result.isPresent()) {
                IcebergTableHandle newTable = (IcebergTableHandle) result.get().getHandle().connectorHandle();

                assertThat(newTable.getEnforcedPredicate()).isEqualTo(TupleDomain.withColumnDomains(expectedEnforcedPredicate.entrySet().stream()
                        .collect(toImmutableMap(entry -> columns.get(entry.getKey()), Map.Entry::getValue))));

                assertThat(newTable.getUnenforcedPredicate()).isEqualTo(TupleDomain.withColumnDomains(expectedUnenforcedPredicate.entrySet().stream()
                        .collect(toImmutableMap(entry -> columns.get(entry.getKey()), Map.Entry::getValue))));
            }
        });
    }

    @Test
    public void testCreateExternalTableWithNonExistingSchemaLocation()
            throws Exception
    {
        String schemaName = "test_schema_without_location" + randomNameSuffix();
        String schemaLocation = "/tmp/" + schemaName;

        fileSystem.createDirectory(Location.of(schemaLocation));
        assertUpdate("CREATE SCHEMA iceberg." + schemaName + " WITH (location = '" + schemaLocation + "')");
        fileSystem.deleteDirectory(Location.of(schemaLocation));

        String tableName = "test_create_external" + randomNameSuffix();
        String tableLocation = "/tmp/" + tableName;

        String schemaAndTableName = format("%s.%s", schemaName, tableName);
        assertUpdate("CREATE TABLE " + schemaAndTableName + " (a bigint, b varchar) WITH (location = '" + tableLocation + "')");

        assertUpdate(
                "INSERT INTO " + schemaAndTableName + "(a, b) VALUES" +
                        "(NULL, NULL)," +
                        "(-42, 'abc')," +
                        "(9223372036854775807, 'abcdefghijklmnopqrstuvwxyz')",
                3);
        assertThat(query("SELECT * FROM " + schemaAndTableName))
                .skippingTypesCheck()
                .matches("VALUES" +
                        "(NULL, NULL)," +
                        "(-42, 'abc')," +
                        "(9223372036854775807, 'abcdefghijklmnopqrstuvwxyz')");

        assertUpdate("DROP TABLE " + schemaAndTableName);
        assertUpdate("DROP SCHEMA " + schemaName);
    }

    @Test
    public void testCreateNestedPartitionedTable()
    {
        assertUpdate("CREATE TABLE test_nested_table_1 (" +
                " bool BOOLEAN" +
                ", int INTEGER" +
                ", arr ARRAY(VARCHAR)" +
                ", big BIGINT" +
                ", rl REAL" +
                ", dbl DOUBLE" +
                ", mp MAP(INTEGER, VARCHAR)" +
                ", dec DECIMAL(5,2)" +
                ", vc VARCHAR" +
                ", vb VARBINARY" +
                ", ts TIMESTAMP(6)" +
                ", tstz TIMESTAMP(6) WITH TIME ZONE" +
                ", str ROW(id INTEGER, vc VARCHAR)" +
                ", dt DATE)" +
                " WITH (partitioning = ARRAY['int'])");

        assertUpdate(
                "INSERT INTO test_nested_table_1 " +
                        " select true, 1, array['uno', 'dos', 'tres'], BIGINT '1', REAL '1.0', DOUBLE '1.0', map(array[1,2,3,4], array['ek','don','teen','char'])," +
                        " CAST(1.0 as DECIMAL(5,2))," +
                        " 'one', VARBINARY 'binary0/1values',\n" +
                        " TIMESTAMP '2021-07-24 02:43:57.348000'," +
                        " TIMESTAMP '2021-07-24 02:43:57.348000 UTC'," +
                        " (CAST(ROW(null, 'this is a random value') AS ROW(int, varchar))), " +
                        " DATE '2021-07-24'",
                1);
        assertThat(computeActual("SELECT * from test_nested_table_1").getRowCount()).isEqualTo(1);

        if (format != AVRO) {
            assertThat(query("SHOW STATS FOR test_nested_table_1"))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "  ('bool', NULL, 1e0, 0e0, NULL, 'true', 'true'), " +
                            "  ('int', NULL, 1e0, 0e0, NULL, '1', '1'), " +
                            "  ('arr', NULL, NULL, " + (format == ORC ? "0e0" : "NULL") + ", NULL, NULL, NULL), " +
                            "  ('big', NULL, 1e0, 0e0, NULL, '1', '1'), " +
                            "  ('rl', NULL, 1e0, 0e0, NULL, '1.0', '1.0'), " +
                            "  ('dbl', NULL, 1e0, 0e0, NULL, '1.0', '1.0'), " +
                            "  ('mp', NULL, NULL, " + (format == ORC ? "0e0" : "NULL") + ", NULL, NULL, NULL), " +
                            "  ('dec', NULL, 1e0, 0e0, NULL, '1.0', '1.0'), " +
                            "  ('vc', " + (format == PARQUET ? "105e0" : "NULL") + ", 1e0, 0e0, NULL, NULL, NULL), " +
                            "  ('vb', " + (format == PARQUET ? "71e0" : "NULL") + ", 1e0, 0e0, NULL, NULL, NULL), " +
                            "  ('ts', NULL, 1e0, 0e0, NULL, '2021-07-24 02:43:57.348000', " + (format == ORC ? "'2021-07-24 02:43:57.348999'" : "'2021-07-24 02:43:57.348000'") + "), " +
                            "  ('tstz', NULL, 1e0, 0e0, NULL, '2021-07-24 02:43:57.348 UTC', '2021-07-24 02:43:57.348 UTC'), " +
                            "  ('str', NULL, NULL, " + (format == ORC ? "0e0" : "NULL") + ", NULL, NULL, NULL), " +
                            "  ('dt', NULL, 1e0, 0e0, NULL, '2021-07-24', '2021-07-24'), " +
                            "  (NULL, NULL, NULL, NULL, 1e0, NULL, NULL)");
        }
        else {
            assertThat(query("SHOW STATS FOR test_nested_table_1"))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "  ('bool', NULL, 1e0, 0e0, NULL, NULL, NULL), " +
                            "  ('int', NULL, 1e0, 0e0, NULL, '1', '1'), " +
                            "  ('arr', NULL, NULL, NULL, NULL, NULL, NULL), " +
                            "  ('big', NULL, 1e0, 0e0, NULL, NULL, NULL), " +
                            "  ('rl', NULL, 1e0, 0e0, NULL, NULL, NULL), " +
                            "  ('dbl', NULL, 1e0, 0e0, NULL, NULL, NULL), " +
                            "  ('mp', NULL, NULL, NULL, NULL, NULL, NULL), " +
                            "  ('dec', NULL, 1e0, 0e0, NULL, NULL, NULL), " +
                            "  ('vc', NULL, 1e0, 0e0, NULL, NULL, NULL), " +
                            "  ('vb', NULL, 1e0, 0e0, NULL, NULL, NULL), " +
                            "  ('ts', NULL, 1e0, 0e0, NULL, NULL, NULL), " +
                            "  ('tstz', NULL, 1e0, 0e0, NULL, NULL, NULL), " +
                            "  ('str', NULL, NULL, NULL, NULL, NULL, NULL), " +
                            "  ('dt', NULL, 1e0, 0e0, NULL, NULL, NULL), " +
                            "  (NULL, NULL, NULL, NULL, 1e0, NULL, NULL)");
        }

        assertUpdate("DROP TABLE test_nested_table_1");

        assertUpdate("" +
                "CREATE TABLE test_nested_table_2 (" +
                " int INTEGER" +
                ", arr ARRAY(ROW(id INTEGER, vc VARCHAR))" +
                ", big BIGINT" +
                ", rl REAL" +
                ", dbl DOUBLE" +
                ", mp MAP(INTEGER, ARRAY(VARCHAR))" +
                ", dec DECIMAL(5,2)" +
                ", str ROW(id INTEGER, vc VARCHAR, arr ARRAY(INTEGER))" +
                ", vc VARCHAR)" +
                " WITH (partitioning = ARRAY['int'])");

        assertUpdate(
                "INSERT INTO test_nested_table_2 " +
                        " select 1, array[cast(row(1, null) as row(int, varchar)), cast(row(2, 'dos') as row(int, varchar))], BIGINT '1', REAL '1.0', DOUBLE '1.0', " +
                        "map(array[1,2], array[array['ek', 'one'], array['don', 'do', 'two']]), CAST(1.0 as DECIMAL(5,2)), " +
                        "CAST(ROW(1, 'this is a random value', null) AS ROW(int, varchar, array(int))), 'one'",
                1);
        assertThat(computeActual("SELECT * from test_nested_table_2").getRowCount()).isEqualTo(1);

        if (format != AVRO) {
            assertThat(query("SHOW STATS FOR test_nested_table_2"))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "  ('int', NULL, 1e0, 0e0, NULL, '1', '1'), " +
                            "  ('arr', NULL, NULL, " + (format == ORC ? "0e0" : "NULL") + ", NULL, NULL, NULL), " +
                            "  ('big', NULL, 1e0, 0e0, NULL, '1', '1'), " +
                            "  ('rl', NULL, 1e0, 0e0, NULL, '1.0', '1.0'), " +
                            "  ('dbl', NULL, 1e0, 0e0, NULL, '1.0', '1.0'), " +
                            "  ('mp', NULL, NULL, " + (format == ORC ? "0e0" : "NULL") + ", NULL, NULL, NULL), " +
                            "  ('dec', NULL, 1e0, 0e0, NULL, '1.0', '1.0'), " +
                            "  ('vc', " + (format == PARQUET ? "105e0" : "NULL") + ", 1e0, 0e0, NULL, NULL, NULL), " +
                            "  ('str', NULL, NULL, " + (format == ORC ? "0e0" : "NULL") + ", NULL, NULL, NULL), " +
                            "  (NULL, NULL, NULL, NULL, 1e0, NULL, NULL)");
        }
        else {
            assertThat(query("SHOW STATS FOR test_nested_table_2"))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "  ('int', NULL, 1e0, 0e0, NULL, '1', '1'), " +
                            "  ('arr', NULL, NULL, NULL, NULL, NULL, NULL), " +
                            "  ('big', NULL, 1e0, 0e0, NULL, NULL, NULL), " +
                            "  ('rl', NULL, 1e0, 0e0, NULL, NULL, NULL), " +
                            "  ('dbl', NULL, 1e0, 0e0, NULL, NULL, NULL), " +
                            "  ('mp', NULL, NULL, NULL, NULL, NULL, NULL), " +
                            "  ('dec', NULL, 1e0, 0e0, NULL, NULL, NULL), " +
                            "  ('vc', NULL, 1e0, 0e0, NULL, NULL, NULL), " +
                            "  ('str', NULL, NULL, NULL, NULL, NULL, NULL), " +
                            "  (NULL, NULL, NULL, NULL, 1e0, NULL, NULL)");
        }

        assertUpdate("CREATE TABLE test_nested_table_3 WITH (partitioning = ARRAY['int']) AS SELECT * FROM test_nested_table_2", 1);

        assertThat(computeActual("SELECT * FROM test_nested_table_3").getRowCount()).isEqualTo(1);

        assertThat(query("SHOW STATS FOR test_nested_table_3"))
                .matches("SHOW STATS FOR test_nested_table_2");

        assertUpdate("DROP TABLE test_nested_table_2");
        assertUpdate("DROP TABLE test_nested_table_3");
    }

    @Test
    public void testSerializableReadIsolation()
    {
        assertUpdate("CREATE TABLE test_read_isolation (x int)");
        assertUpdate("INSERT INTO test_read_isolation VALUES 123, 456", 2);

        withTransaction(session -> {
            assertQuery(session, "SELECT * FROM test_read_isolation", "VALUES 123, 456");

            assertUpdate("INSERT INTO test_read_isolation VALUES 789", 1);
            assertQuery("SELECT * FROM test_read_isolation", "VALUES 123, 456, 789");

            assertQuery(session, "SELECT * FROM test_read_isolation", "VALUES 123, 456");
        });

        assertQuery("SELECT * FROM test_read_isolation", "VALUES 123, 456, 789");

        assertUpdate("DROP TABLE test_read_isolation");
    }

    private void withTransaction(Consumer<Session> consumer)
    {
        transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getPlannerContext().getMetadata(), getQueryRunner().getAccessControl())
                .readCommitted()
                .execute(getSession(), consumer);
    }

    @Test
    public void testOptimizedMetadataQueries()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty("optimize_metadata_queries", "true")
                .build();

        assertUpdate("CREATE TABLE test_metadata_optimization (a BIGINT, b BIGINT, c BIGINT) WITH (PARTITIONING = ARRAY['b', 'c'])");

        assertUpdate("INSERT INTO test_metadata_optimization VALUES (5, 6, 7), (8, 9, 10)", 2);

        assertQuery(session, "SELECT DISTINCT b FROM test_metadata_optimization", "VALUES (6), (9)");
        assertQuery(session, "SELECT DISTINCT b, c FROM test_metadata_optimization", "VALUES (6, 7), (9, 10)");
        assertQuery(session, "SELECT DISTINCT b FROM test_metadata_optimization WHERE b < 7", "VALUES (6)");
        assertQuery(session, "SELECT DISTINCT b FROM test_metadata_optimization WHERE c > 8", "VALUES (9)");

        // Assert behavior after metadata delete
        assertUpdate("DELETE FROM test_metadata_optimization WHERE b = 6", 1);
        assertQuery(session, "SELECT DISTINCT b FROM test_metadata_optimization", "VALUES (9)");

        // TODO: assert behavior after deleting the last row of a partition, once row-level deletes are supported.
        // i.e. a query like 'DELETE FROM test_metadata_optimization WHERE b = 6 AND a = 5'

        assertUpdate("DROP TABLE test_metadata_optimization");
    }

    @Test
    public void testFileSizeInManifest()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_file_size_in_manifest (" +
                "a_bigint bigint, " +
                "a_varchar varchar, " +
                "a_long_decimal decimal(38,20), " +
                "a_map map(varchar, integer))");

        assertUpdate(
                "INSERT INTO test_file_size_in_manifest VALUES " +
                        "(NULL, NULL, NULL, NULL), " +
                        "(42, 'some varchar value', DECIMAL '123456789123456789.123456789123456789', map(ARRAY['abc', 'def'], ARRAY[113, -237843832]))",
                2);

        MaterializedResult files = computeActual("SELECT file_path, record_count, file_size_in_bytes FROM \"test_file_size_in_manifest$files\"");
        long totalRecordCount = 0;
        for (MaterializedRow row : files.getMaterializedRows()) {
            String path = (String) row.getField(0);
            Long recordCount = (Long) row.getField(1);
            Long fileSizeInBytes = (Long) row.getField(2);

            totalRecordCount += recordCount;
            assertThat(fileSizeInBytes).isEqualTo(fileSize(path));
        }
        // Verify sum(record_count) to make sure we have all the files.
        assertThat(totalRecordCount).isEqualTo(2);
    }

    @Test
    public void testIncorrectIcebergFileSizes()
            throws Exception
    {
        // Create a table with a single insert
        assertUpdate("CREATE TABLE test_iceberg_file_size (x BIGINT)");
        assertUpdate("INSERT INTO test_iceberg_file_size VALUES (123), (456), (758)", 3);

        // Get manifest file
        MaterializedResult result = computeActual("SELECT path FROM \"test_iceberg_file_size$manifests\"");
        assertThat(result.getRowCount()).isEqualTo(1);
        String manifestFile = (String) result.getOnlyValue();

        // Read manifest file
        Schema schema;
        GenericData.Record entry = null;
        try (DataFileReader<GenericData.Record> dataFileReader = readManifestFile(manifestFile)) {
            schema = dataFileReader.getSchema();
            int recordCount = 0;
            while (dataFileReader.hasNext()) {
                entry = dataFileReader.next();
                recordCount++;
            }
            assertThat(recordCount).isEqualTo(1);
        }

        // Alter data file entry to store incorrect file size
        GenericData.Record dataFile = (GenericData.Record) entry.get("data_file");
        long alteredValue = 50L;
        assertThat(dataFile.get("file_size_in_bytes"))
                .isNotEqualTo(alteredValue);
        dataFile.put("file_size_in_bytes", alteredValue);

        // Write altered metadata
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DataFileWriter<GenericData.Record> dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            dataFileWriter.create(schema, out);
            dataFileWriter.append(entry);
        }
        fileSystem.newOutputFile(Location.of(manifestFile)).createOrOverwrite(out.toByteArray());

        // Ignoring Iceberg provided file size makes the query succeed
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", "use_file_size_from_metadata", "false")
                .build();
        assertQuery(session, "SELECT * FROM test_iceberg_file_size", "VALUES (123), (456), (758)");

        // Using Iceberg provided file size fails the query
        assertQueryFails(
                "SELECT * FROM test_iceberg_file_size",
                "(Malformed ORC file\\. Invalid file metadata.*)|(.*Malformed Parquet file.*)");

        assertUpdate("DROP TABLE test_iceberg_file_size");
    }

    protected DataFileReader<GenericData.Record> readManifestFile(String location)
            throws IOException
    {
        Path tempFile = getDistributedQueryRunner().getCoordinator().getBaseDataDir().resolve(randomUUID() + "-manifest-copy");
        try (InputStream inputStream = fileSystem.newInputFile(Location.of(location)).newStream()) {
            Files.copy(inputStream, tempFile);
        }
        return new DataFileReader<>(tempFile.toFile(), new GenericDatumReader<>());
    }

    @Test
    public void testSplitPruningForFilterOnPartitionColumn()
    {
        String tableName = "nation_partitioned_pruning";

        assertUpdate("DROP TABLE IF EXISTS " + tableName);

        // disable writes redistribution to have predictable number of files written per partition (one).
        Session noRedistributeWrites = Session.builder(getSession())
                .setSystemProperty("redistribute_writes", "false")
                .build();

        assertUpdate(noRedistributeWrites, "CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['regionkey']) AS SELECT * FROM nation", 25);

        // sanity check that table contains exactly 5 files
        assertThat(query("SELECT count(*) FROM \"" + tableName + "$files\"")).matches("VALUES CAST(5 AS BIGINT)");

        verifySplitCount("SELECT * FROM " + tableName, 5);
        verifySplitCount("SELECT * FROM " + tableName + " WHERE regionkey = 3", 1);
        verifySplitCount("SELECT * FROM " + tableName + " WHERE regionkey < 2", 2);
        verifySplitCount("SELECT * FROM " + tableName + " WHERE regionkey < 0", 0);
        verifySplitCount("SELECT * FROM " + tableName + " WHERE regionkey > 1 AND regionkey < 4", 2);
        verifySplitCount("SELECT * FROM " + tableName + " WHERE regionkey % 5 = 3", 1);

        assertUpdate("DROP TABLE " + tableName);

        // Partition by multiple columns
        assertUpdate(noRedistributeWrites, "CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['regionkey', 'nationkey']) AS SELECT * FROM nation", 25);
        // Create 2 files per partition
        assertUpdate(noRedistributeWrites, "INSERT INTO " + tableName + " SELECT * FROM nation", 25);
        // sanity check that table contains exactly 50 files
        assertThat(computeScalar("SELECT count(*) FROM \"" + tableName + "$files\"")).isEqualTo(50L);

        verifySplitCount("SELECT * FROM " + tableName + " WHERE regionkey % 5 = 3", 10);
        verifySplitCount("SELECT * FROM " + tableName + " WHERE (regionkey * 2) - nationkey = 0", 6);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAllAvailableTypes()
    {
        assertUpdate("CREATE TABLE test_all_types (" +
                "  a_boolean boolean, " +
                "  an_integer integer, " +
                "  a_bigint bigint, " +
                "  a_real real, " +
                "  a_double double, " +
                "  a_short_decimal decimal(5,2), " +
                "  a_long_decimal decimal(38,20), " +
                "  a_varchar varchar, " +
                "  a_varbinary varbinary, " +
                "  a_date date, " +
                "  a_time time(6), " +
                "  a_timestamp timestamp(6), " +
                "  a_timestamptz timestamp(6) with time zone, " +
                "  a_uuid uuid, " +
                "  a_row row(id integer, vc varchar), " +
                "  an_array array(varchar), " +
                "  a_map map(integer, varchar) " +
                ")");

        String values = "VALUES (" +
                "true, " +
                "1, " +
                "BIGINT '1', " +
                "REAL '1.0', " +
                "DOUBLE '1.0', " +
                "CAST(1.0 AS decimal(5,2)), " +
                "CAST(11.0 AS decimal(38,20)), " +
                "VARCHAR 'onefsadfdsf', " +
                "X'000102f0feff', " +
                "DATE '2021-07-24'," +
                "TIME '02:43:57.987654', " +
                "TIMESTAMP '2021-07-24 03:43:57.987654'," +
                "TIMESTAMP '2021-07-24 04:43:57.987654 UTC', " +
                "UUID '20050910-1330-11e9-ffff-2a86e4085a59', " +
                "CAST(ROW(42, 'this is a random value') AS ROW(id int, vc varchar)), " +
                "ARRAY[VARCHAR 'uno', 'dos', 'tres'], " +
                "map(ARRAY[1,2], ARRAY['ek', VARCHAR 'one'])) ";

        String nullValues = nCopies(17, "NULL").stream()
                .collect(joining(", ", "VALUES (", ")"));

        assertUpdate("INSERT INTO test_all_types " + values, 1);
        assertUpdate("INSERT INTO test_all_types " + nullValues, 1);

        // SELECT
        assertThat(query("SELECT * FROM test_all_types"))
                .matches(values + " UNION ALL " + nullValues);

        // SELECT with predicates
        assertThat(query("SELECT * FROM test_all_types WHERE " +
                "    a_boolean = true " +
                "AND an_integer = 1 " +
                "AND a_bigint = BIGINT '1' " +
                "AND a_real = REAL '1.0' " +
                "AND a_double = DOUBLE '1.0' " +
                "AND a_short_decimal = CAST(1.0 AS decimal(5,2)) " +
                "AND a_long_decimal = CAST(11.0 AS decimal(38,20)) " +
                "AND a_varchar = VARCHAR 'onefsadfdsf' " +
                "AND a_varbinary = X'000102f0feff' " +
                "AND a_date = DATE '2021-07-24' " +
                "AND a_time = TIME '02:43:57.987654' " +
                "AND a_timestamp = TIMESTAMP '2021-07-24 03:43:57.987654' " +
                "AND a_timestamptz = TIMESTAMP '2021-07-24 04:43:57.987654 UTC' " +
                "AND a_uuid = UUID '20050910-1330-11e9-ffff-2a86e4085a59' " +
                "AND a_row = CAST(ROW(42, 'this is a random value') AS ROW(id int, vc varchar)) " +
                "AND an_array = ARRAY[VARCHAR 'uno', 'dos', 'tres'] " +
                "AND a_map = map(ARRAY[1,2], ARRAY['ek', VARCHAR 'one']) " +
                ""))
                .matches(values);

        assertThat(query("SELECT * FROM test_all_types WHERE " +
                "    a_boolean IS NULL " +
                "AND an_integer IS NULL " +
                "AND a_bigint IS NULL " +
                "AND a_real IS NULL " +
                "AND a_double IS NULL " +
                "AND a_short_decimal IS NULL " +
                "AND a_long_decimal IS NULL " +
                "AND a_varchar IS NULL " +
                "AND a_varbinary IS NULL " +
                "AND a_date IS NULL " +
                "AND a_time IS NULL " +
                "AND a_timestamp IS NULL " +
                "AND a_timestamptz IS NULL " +
                "AND a_uuid IS NULL " +
                "AND a_row IS NULL " +
                "AND an_array IS NULL " +
                "AND a_map IS NULL " +
                ""))
                .skippingTypesCheck()
                .matches(nullValues);

        // SHOW STATS
        if (format != AVRO) {
            assertThat(query("SHOW STATS FOR test_all_types"))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "  ('a_boolean', NULL, 1e0, 0.5e0, NULL, 'true', 'true'), " +
                            "  ('an_integer', NULL, 1e0, 0.5e0, NULL, '1', '1'), " +
                            "  ('a_bigint', NULL, 1e0, 0.5e0, NULL, '1', '1'), " +
                            "  ('a_real', NULL, 1e0, 0.5e0, NULL, '1.0', '1.0'), " +
                            "  ('a_double', NULL, 1e0, 0.5e0, NULL, '1.0', '1.0'), " +
                            "  ('a_short_decimal', NULL, 1e0, 0.5e0, NULL, '1.0', '1.0'), " +
                            "  ('a_long_decimal', NULL, 1e0, 0.5e0, NULL, '11.0', '11.0'), " +
                            "  ('a_varchar', " + (format == PARQUET ? "213e0" : "NULL") + ", 1e0, 0.5e0, NULL, NULL, NULL), " +
                            "  ('a_varbinary', " + (format == PARQUET ? "103e0" : "NULL") + ", 1e0, 0.5e0, NULL, NULL, NULL), " +
                            "  ('a_date', NULL, 1e0, 0.5e0, NULL, '2021-07-24', '2021-07-24'), " +
                            "  ('a_time', NULL, 1e0, 0.5e0, NULL, NULL, NULL), " +
                            "  ('a_timestamp', NULL, 1e0, 0.5e0, NULL, " + (format == ORC ? "'2021-07-24 03:43:57.987000', '2021-07-24 03:43:57.987999'" : "'2021-07-24 03:43:57.987654', '2021-07-24 03:43:57.987654'") + "), " +
                            "  ('a_timestamptz', NULL, 1e0, 0.5e0, NULL, '2021-07-24 04:43:57.987 UTC', '2021-07-24 04:43:57.987 UTC'), " +
                            "  ('a_uuid', NULL, 1e0, 0.5e0, NULL, NULL, NULL), " +
                            "  ('a_row', NULL, NULL, " + (format == ORC ? "0.5" : "NULL") + ", NULL, NULL, NULL), " +
                            "  ('an_array', NULL, NULL, " + (format == ORC ? "0.5" : "NULL") + ", NULL, NULL, NULL), " +
                            "  ('a_map', NULL, NULL, " + (format == ORC ? "0.5" : "NULL") + ", NULL, NULL, NULL), " +
                            "  (NULL, NULL, NULL, NULL, 2e0, NULL, NULL)");
        }
        else {
            assertThat(query("SHOW STATS FOR test_all_types"))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "  ('a_boolean', NULL, 1e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('an_integer', NULL, 1e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('a_bigint', NULL, 1e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('a_real', NULL, 1e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('a_double', NULL, 1e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('a_short_decimal', NULL, 1e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('a_long_decimal', NULL, 1e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('a_varchar', NULL, 1e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('a_varbinary', NULL, 1e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('a_date', NULL, 1e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('a_time', NULL, 1e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('a_timestamp', NULL, 1e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('a_timestamptz', NULL, 1e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('a_uuid', NULL, 1e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('a_row', NULL, NULL, NULL, NULL, NULL, NULL), " +
                            "  ('an_array', NULL, NULL, NULL, NULL, NULL, NULL), " +
                            "  ('a_map', NULL, NULL, NULL, NULL, NULL, NULL), " +
                            "  (NULL, NULL, NULL, NULL, 2e0, NULL, NULL)");
        }

        // ANALYZE
        Session defaultSession = getSession();
        String catalog = defaultSession.getCatalog().orElseThrow();
        Session extendedStatisticsEnabled = Session.builder(defaultSession)
                .setCatalogSessionProperty(catalog, EXTENDED_STATISTICS_ENABLED, "true")
                .build();
        assertUpdate(extendedStatisticsEnabled, "ANALYZE test_all_types");
        if (format != AVRO) {
            assertThat(query(extendedStatisticsEnabled, "SHOW STATS FOR test_all_types"))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "  ('a_boolean', NULL, 1e0, 0.5e0, NULL, 'true', 'true'), " +
                            "  ('an_integer', NULL, 1e0, 0.5e0, NULL, '1', '1'), " +
                            "  ('a_bigint', NULL, 1e0, 0.5e0, NULL, '1', '1'), " +
                            "  ('a_real', NULL, 1e0, 0.5e0, NULL, '1.0', '1.0'), " +
                            "  ('a_double', NULL, 1e0, 0.5e0, NULL, '1.0', '1.0'), " +
                            "  ('a_short_decimal', NULL, 1e0, 0.5e0, NULL, '1.0', '1.0'), " +
                            "  ('a_long_decimal', NULL, 1e0, 0.5e0, NULL, '11.0', '11.0'), " +
                            "  ('a_varchar', " + (format == PARQUET ? "213e0" : "NULL") + ", 1e0, 0.5e0, NULL, NULL, NULL), " +
                            "  ('a_varbinary', " + (format == PARQUET ? "103e0" : "NULL") + ", 1e0, 0.5e0, NULL, NULL, NULL), " +
                            "  ('a_date', NULL, 1e0, 0.5e0, NULL, '2021-07-24', '2021-07-24'), " +
                            "  ('a_time', NULL, 1e0, 0.5e0, NULL, NULL, NULL), " +
                            "  ('a_timestamp', NULL, 1e0, 0.5e0, NULL, " + (format == ORC ? "'2021-07-24 03:43:57.987000', '2021-07-24 03:43:57.987999'" : "'2021-07-24 03:43:57.987654', '2021-07-24 03:43:57.987654'") + "), " +
                            "  ('a_timestamptz', NULL, 1e0, 0.5e0, NULL, '2021-07-24 04:43:57.987 UTC', '2021-07-24 04:43:57.987 UTC'), " +
                            "  ('a_uuid', NULL, 1e0, 0.5e0, NULL, NULL, NULL), " +
                            "  ('a_row', NULL, NULL, " + (format == ORC ? "0.5" : "NULL") + ", NULL, NULL, NULL), " +
                            "  ('an_array', NULL, NULL, " + (format == ORC ? "0.5" : "NULL") + ", NULL, NULL, NULL), " +
                            "  ('a_map', NULL, NULL, " + (format == ORC ? "0.5" : "NULL") + ", NULL, NULL, NULL), " +
                            "  (NULL, NULL, NULL, NULL, 2e0, NULL, NULL)");
        }
        else {
            assertThat(query(extendedStatisticsEnabled, "SHOW STATS FOR test_all_types"))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "  ('a_boolean', NULL, 1e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('an_integer', NULL, 1e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('a_bigint', NULL, 1e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('a_real', NULL, 1e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('a_double', NULL, 1e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('a_short_decimal', NULL, 1e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('a_long_decimal', NULL, 1e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('a_varchar', NULL, 1e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('a_varbinary', NULL, 1e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('a_date', NULL, 1e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('a_time', NULL, 1e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('a_timestamp', NULL, 1e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('a_timestamptz', NULL, 1e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('a_uuid', NULL, 1e0, 0.1e0, NULL, NULL, NULL), " +
                            "  ('a_row', NULL, NULL, NULL, NULL, NULL, NULL), " +
                            "  ('an_array', NULL, NULL, NULL, NULL, NULL, NULL), " +
                            "  ('a_map', NULL, NULL, NULL, NULL, NULL, NULL), " +
                            "  (NULL, NULL, NULL, NULL, 2e0, NULL, NULL)");
        }

        // $partitions
        String schema = getSession().getSchema().orElseThrow();
        assertThat(query("SELECT column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name = 'test_all_types$partitions' "))
                .skippingTypesCheck()
                .matches("VALUES 'record_count', 'file_count', 'total_size', 'data'");
        if (format != AVRO) {
            assertThat(query("SELECT " +
                    "  record_count," +
                    "  file_count, " +
                    "  data.a_boolean, " +
                    "  data.an_integer, " +
                    "  data.a_bigint, " +
                    "  data.a_real, " +
                    "  data.a_double, " +
                    "  data.a_short_decimal, " +
                    "  data.a_long_decimal, " +
                    "  data.a_varchar, " +
                    "  data.a_varbinary, " +
                    "  data.a_date, " +
                    "  data.a_time, " +
                    "  data.a_timestamp, " +
                    "  data.a_timestamptz, " +
                    "  data.a_uuid " +
                    " FROM \"test_all_types$partitions\" "))
                    .matches(
                            "VALUES (" +
                                    "  BIGINT '2', " +
                                    "  BIGINT '2', " +
                                    "  CAST(ROW(true, true, 1, NULL) AS ROW(min boolean, max boolean, null_count bigint, nan_count bigint)), " +
                                    "  CAST(ROW(1, 1, 1, NULL) AS ROW(min integer, max integer, null_count bigint, nan_count bigint)), " +
                                    "  CAST(ROW(1, 1, 1, NULL) AS ROW(min bigint, max bigint, null_count bigint, nan_count bigint)), " +
                                    "  CAST(ROW(1, 1, 1, NULL) AS ROW(min real, max real, null_count bigint, nan_count bigint)), " +
                                    "  CAST(ROW(1, 1, 1, NULL) AS ROW(min double, max double, null_count bigint, nan_count bigint)), " +
                                    "  CAST(ROW(1, 1, 1, NULL) AS ROW(min decimal(5,2), max decimal(5,2), null_count bigint, nan_count bigint)), " +
                                    "  CAST(ROW(11, 11, 1, NULL) AS ROW(min decimal(38,20), max decimal(38,20), null_count bigint, nan_count bigint)), " +
                                    "  CAST(ROW('onefsadfdsf', 'onefsadfdsf', 1, NULL) AS ROW(min varchar, max varchar, null_count bigint, nan_count bigint)), " +
                                    (format == ORC ?
                                            "  CAST(ROW(NULL, NULL, 1, NULL) AS ROW(min varbinary, max varbinary, null_count bigint, nan_count bigint)), " :
                                            "  CAST(ROW(X'000102f0feff', X'000102f0feff', 1, NULL) AS ROW(min varbinary, max varbinary, null_count bigint, nan_count bigint)), ") +
                                    "  CAST(ROW(DATE '2021-07-24', DATE '2021-07-24', 1, NULL) AS ROW(min date, max date, null_count bigint, nan_count bigint)), " +
                                    "  CAST(ROW(TIME '02:43:57.987654', TIME '02:43:57.987654', 1, NULL) AS ROW(min time(6), max time(6), null_count bigint, nan_count bigint)), " +
                                    (format == ORC ?
                                            "  CAST(ROW(TIMESTAMP '2021-07-24 03:43:57.987000', TIMESTAMP '2021-07-24 03:43:57.987999', 1, NULL) AS ROW(min timestamp(6), max timestamp(6), null_count bigint, nan_count bigint)), " :
                                            "  CAST(ROW(TIMESTAMP '2021-07-24 03:43:57.987654', TIMESTAMP '2021-07-24 03:43:57.987654', 1, NULL) AS ROW(min timestamp(6), max timestamp(6), null_count bigint, nan_count bigint)), ") +
                                    (format == ORC ?
                                            "  CAST(ROW(TIMESTAMP '2021-07-24 04:43:57.987000 UTC', TIMESTAMP '2021-07-24 04:43:57.987999 UTC', 1, NULL) AS ROW(min timestamp(6) with time zone, max timestamp(6) with time zone, null_count bigint, nan_count bigint)), " :
                                            "  CAST(ROW(TIMESTAMP '2021-07-24 04:43:57.987654 UTC', TIMESTAMP '2021-07-24 04:43:57.987654 UTC', 1, NULL) AS ROW(min timestamp(6) with time zone, max timestamp(6) with time zone, null_count bigint, nan_count bigint)), ") +
                                    (format == ORC ?
                                            "  CAST(ROW(NULL, NULL, 1, NULL) AS ROW(min uuid, max uuid, null_count bigint, nan_count bigint)) " :
                                            "  CAST(ROW(UUID '20050910-1330-11e9-ffff-2a86e4085a59', UUID '20050910-1330-11e9-ffff-2a86e4085a59', 1, NULL) AS ROW(min uuid, max uuid, null_count bigint, nan_count bigint)) "
                                    ) +
                                    ")");
        }
        else {
            assertThat(query("SELECT " +
                    "  record_count," +
                    "  file_count, " +
                    "  data.a_boolean, " +
                    "  data.an_integer, " +
                    "  data.a_bigint, " +
                    "  data.a_real, " +
                    "  data.a_double, " +
                    "  data.a_short_decimal, " +
                    "  data.a_long_decimal, " +
                    "  data.a_varchar, " +
                    "  data.a_varbinary, " +
                    "  data.a_date, " +
                    "  data.a_time, " +
                    "  data.a_timestamp, " +
                    "  data.a_timestamptz, " +
                    "  data.a_uuid " +
                    " FROM \"test_all_types$partitions\" "))
                    .matches(
                            "VALUES (" +
                                    "  BIGINT '2', " +
                                    "  BIGINT '2', " +
                                    "  CAST(NULL AS ROW(min boolean, max boolean, null_count bigint, nan_count bigint)), " +
                                    "  CAST(NULL AS ROW(min integer, max integer, null_count bigint, nan_count bigint)), " +
                                    "  CAST(NULL AS ROW(min bigint, max bigint, null_count bigint, nan_count bigint)), " +
                                    "  CAST(NULL AS ROW(min real, max real, null_count bigint, nan_count bigint)), " +
                                    "  CAST(NULL AS ROW(min double, max double, null_count bigint, nan_count bigint)), " +
                                    "  CAST(NULL AS ROW(min decimal(5,2), max decimal(5,2), null_count bigint, nan_count bigint)), " +
                                    "  CAST(NULL AS ROW(min decimal(38,20), max decimal(38,20), null_count bigint, nan_count bigint)), " +
                                    "  CAST(NULL AS ROW(min varchar, max varchar, null_count bigint, nan_count bigint)), " +
                                    "  CAST(NULL AS ROW(min varbinary, max varbinary, null_count bigint, nan_count bigint)), " +
                                    "  CAST(NULL AS ROW(min date, max date, null_count bigint, nan_count bigint)), " +
                                    "  CAST(NULL AS ROW(min time(6), max time(6), null_count bigint, nan_count bigint)), " +
                                    "  CAST(NULL AS ROW(min timestamp(6), max timestamp(6), null_count bigint, nan_count bigint)), " +
                                    "  CAST(NULL AS ROW(min timestamp(6) with time zone, max timestamp(6) with time zone, null_count bigint, nan_count bigint)), " +
                                    "  CAST(NULL AS ROW(min uuid, max uuid, null_count bigint, nan_count bigint)) " +
                                    ")");
        }

        assertUpdate("DROP TABLE test_all_types");
    }

    @Test
    public void testRepartitionDataOnCtas()
    {
        // identity partitioning column
        testRepartitionData(getSession(), "tpch.tiny.orders", true, "'orderstatus'", 3);
        // bucketing
        testRepartitionData(getSession(), "tpch.tiny.orders", true, "'bucket(custkey, 13)'", 13);
        // varchar-based
        testRepartitionData(getSession(), "tpch.tiny.orders", true, "'truncate(comment, 1)'", 35);
        // complex; would exceed 100 open writers limit in IcebergPageSink without write repartitioning
        testRepartitionData(getSession(), "tpch.tiny.orders", true, "'bucket(custkey, 4)', 'truncate(comment, 1)'", 131);
        // same column multiple times
        testRepartitionData(getSession(), "tpch.tiny.orders", true, "'truncate(comment, 1)', 'orderstatus', 'bucket(comment, 2)'", 180);
    }

    @Test
    public void testRepartitionDataOnInsert()
    {
        // identity partitioning column
        testRepartitionData(getSession(), "tpch.tiny.orders", false, "'orderstatus'", 3);
        // bucketing
        testRepartitionData(getSession(), "tpch.tiny.orders", false, "'bucket(custkey, 13)'", 13);
        // varchar-based
        testRepartitionData(getSession(), "tpch.tiny.orders", false, "'truncate(comment, 1)'", 35);
        // complex; would exceed 100 open writers limit in IcebergPageSink without write repartitioning
        testRepartitionData(getSession(), "tpch.tiny.orders", false, "'bucket(custkey, 4)', 'truncate(comment, 1)'", 131);
        // same column multiple times
        testRepartitionData(getSession(), "tpch.tiny.orders", false, "'truncate(comment, 1)', 'orderstatus', 'bucket(comment, 2)'", 180);
    }

    @Test
    public void testStatsBasedRepartitionDataOnCtas()
    {
        testStatsBasedRepartitionData(true);
    }

    @Test
    public void testStatsBasedRepartitionDataOnInsert()
    {
        testStatsBasedRepartitionData(false);
    }

    private void testStatsBasedRepartitionData(boolean ctas)
    {
        String catalog = getSession().getCatalog().orElseThrow();
        try (TestTable sourceTable = new TestTable(
                sql -> assertQuerySucceeds(
                        Session.builder(getSession())
                                .setCatalogSessionProperty(catalog, COLLECT_EXTENDED_STATISTICS_ON_WRITE, "true")
                                .build(),
                        sql),
                "temp_table_analyzed",
                "AS SELECT orderkey, custkey, orderstatus FROM tpch.\"sf0.03\".orders")) {
            Session sessionRepartitionMany = Session.builder(getSession())
                    .setSystemProperty(SCALE_WRITERS, "false")
                    .setSystemProperty(USE_PREFERRED_WRITE_PARTITIONING, "false")
                    .build();
            // Use DISTINCT to add data redistribution between source table and the writer. This makes it more likely that all writers get some data.
            String sourceRelation = "(SELECT DISTINCT orderkey, custkey, orderstatus FROM " + sourceTable.getName() + ")";
            testRepartitionData(
                    getSession(),
                    sourceRelation,
                    ctas,
                    "'orderstatus'",
                    3);
            // Test uses relatively small table (45K rows). When engine doesn't redistribute data for writes,
            // occasionally a worker node doesn't get any data and fewer files get created.
            assertEventually(new Duration(3, MINUTES), () -> {
                testRepartitionData(
                        sessionRepartitionMany,
                        sourceRelation,
                        ctas,
                        "'orderstatus'",
                        9);
            });
        }
    }

    private void testRepartitionData(Session session, String sourceRelation, boolean ctas, String partitioning, int expectedFiles)
    {
        String tableName = "repartition" +
                "_" + sourceRelation.replaceAll("[^a-zA-Z0-9]", "") +
                (ctas ? "ctas" : "insert") +
                "_" + partitioning.replaceAll("[^a-zA-Z0-9]", "") +
                "_" + randomNameSuffix();

        long rowCount = (long) computeScalar(session, "SELECT count(*) FROM " + sourceRelation);

        if (ctas) {
            assertUpdate(
                    session,
                    "CREATE TABLE " + tableName + " WITH (partitioning = ARRAY[" + partitioning + "]) " +
                            "AS SELECT * FROM " + sourceRelation,
                    rowCount);
        }
        else {
            assertUpdate(
                    session,
                    "CREATE TABLE " + tableName + " WITH (partitioning = ARRAY[" + partitioning + "]) " +
                            "AS SELECT * FROM " + sourceRelation + " WITH NO DATA",
                    0);
            // Use source table big enough so that there will be multiple pages being written.
            assertUpdate(session, "INSERT INTO " + tableName + " SELECT * FROM " + sourceRelation, rowCount);
        }

        // verify written data
        assertThat(query(session, "TABLE " + tableName))
                .skippingTypesCheck()
                .matches("SELECT * FROM " + sourceRelation);

        // verify data files, i.e. repartitioning took place
        assertThat(query(session, "SELECT count(*) FROM \"" + tableName + "$files\""))
                .matches("VALUES BIGINT '" + expectedFiles + "'");

        assertUpdate(session, "DROP TABLE " + tableName);
    }

    @Test
    public void testSplitPruningForFilterOnNonPartitionColumn()
    {
        for (DataMappingTestSetup testSetup : testDataMappingSmokeTestDataProvider()) {
            if (testSetup.isUnsupportedType()) {
                return;
            }
            try (TestTable table = new TestTable(getQueryRunner()::execute, "test_split_pruning_non_partitioned", "(row_id int, col " + testSetup.getTrinoTypeName() + ")")) {
                String tableName = table.getName();
                String sampleValue = testSetup.getSampleValueLiteral();
                String highValue = testSetup.getHighValueLiteral();
                // Insert separately to ensure two files with one value each
                assertUpdate("INSERT INTO " + tableName + " VALUES (1, " + sampleValue + ")", 1);
                assertUpdate("INSERT INTO " + tableName + " VALUES (2, " + highValue + ")", 1);
                assertQuery("select count(*) from \"" + tableName + "$files\"", "VALUES 2");

                int expectedSplitCount = supportsIcebergFileStatistics(testSetup.getTrinoTypeName()) ? 1 : 2;
                verifySplitCount("SELECT row_id FROM " + tableName, 2);
                verifySplitCount("SELECT row_id FROM " + tableName + " WHERE col = " + sampleValue, expectedSplitCount);
                verifySplitCount("SELECT row_id FROM " + tableName + " WHERE col = " + highValue, expectedSplitCount);

                // ORC max timestamp statistics are truncated to millisecond precision and then appended with 999 microseconds.
                // Therefore, sampleValue and highValue are within the max timestamp & there will be 2 splits.
                verifySplitCount("SELECT row_id FROM " + tableName + " WHERE col > " + sampleValue,
                        (format == ORC && testSetup.getTrinoTypeName().contains("timestamp") ? 2 : expectedSplitCount));
                verifySplitCount("SELECT row_id FROM " + tableName + " WHERE col < " + highValue,
                        (format == ORC && testSetup.getTrinoTypeName().contains("timestamp(6)") ? 2 : expectedSplitCount));
            }
        }
    }

    @Test
    public void testGetIcebergTableProperties()
    {
        assertUpdate("CREATE TABLE test_iceberg_get_table_props (x BIGINT)");
        verifyIcebergTableProperties(computeActual("SELECT * FROM \"test_iceberg_get_table_props$properties\""));
        assertUpdate("DROP TABLE test_iceberg_get_table_props");
    }

    protected void verifyIcebergTableProperties(MaterializedResult actual)
    {
        assertThat(actual).isNotNull();
        MaterializedResult expected = resultBuilder(getSession())
                .row("write.format.default", format.name())
                .row("write.parquet.compression-codec", "zstd").build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testGetIcebergTableWithLegacyOrcBloomFilterProperties()
            throws IOException
    {
        String tableName = "test_get_table_with_legacy_orc_bloom_filter_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 x, 'INDIA' y", 1);

        String tableLocation = getTableLocation(tableName);
        String metadataLocation = getLatestMetadataLocation(fileSystem, tableLocation);

        TableMetadata tableMetadata = TableMetadataParser.read(new ForwardingFileIo(fileSystem), metadataLocation);
        ImmutableMap<String, String> newProperties = ImmutableMap.<String, String>builder()
                .putAll(tableMetadata.properties())
                .put("orc.bloom.filter.columns", "x,y") // legacy incorrect property
                .put("orc.bloom.filter.fpp", "0.2") // legacy incorrect property
                .buildOrThrow();
        TableMetadata newTableMetadata = newTableMetadata(
                tableMetadata.schema(),
                tableMetadata.spec(),
                tableMetadata.sortOrder(),
                tableMetadata.location(),
                newProperties);
        byte[] metadataJson = TableMetadataParser.toJson(newTableMetadata).getBytes(UTF_8);
        fileSystem.newOutputFile(Location.of(metadataLocation)).createOrOverwrite(metadataJson);

        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .contains("orc_bloom_filter_columns", "orc_bloom_filter_fpp");
    }

    protected abstract boolean supportsIcebergFileStatistics(String typeName);

    @Test
    public void testSplitPruningFromDataFileStatistics()
    {
        for (DataMappingTestSetup testSetup : testDataMappingSmokeTestDataProvider()) {
            if (testSetup.isUnsupportedType()) {
                return;
            }
            try (TestTable table = new TestTable(
                    getQueryRunner()::execute,
                    "test_split_pruning_data_file_statistics",
                    // Random double is needed to make sure rows are different. Otherwise compression may deduplicate rows, resulting in only one row group
                    "(col " + testSetup.getTrinoTypeName() + ", r double)")) {
                String tableName = table.getName();
                String values =
                        Stream.concat(
                                        nCopies(100, testSetup.getSampleValueLiteral()).stream(),
                                        nCopies(100, testSetup.getHighValueLiteral()).stream())
                                .map(value -> "(" + value + ", rand())")
                                .collect(joining(", "));
                assertUpdate(withSmallRowGroups(getSession()), "INSERT INTO " + tableName + " VALUES " + values, 200);

                String query = "SELECT * FROM " + tableName + " WHERE col = " + testSetup.getSampleValueLiteral();
                verifyPredicatePushdownDataRead(query, supportsRowGroupStatistics(testSetup.getTrinoTypeName()));
            }
        }
    }

    protected abstract boolean supportsRowGroupStatistics(String typeName);

    private void verifySplitCount(String query, int expectedSplitCount)
    {
        MaterializedResultWithPlan selectAllPartitionsResult = getDistributedQueryRunner().executeWithPlan(getSession(), query);
        assertEqualsIgnoreOrder(selectAllPartitionsResult.result().getMaterializedRows(), computeActual(withoutPredicatePushdown(getSession()), query).getMaterializedRows());
        verifySplitCount(selectAllPartitionsResult.queryId(), expectedSplitCount);
    }

    private void verifyPredicatePushdownDataRead(@Language("SQL") String query, boolean supportsPushdown)
    {
        MaterializedResultWithPlan resultWithPredicatePushdown = getDistributedQueryRunner().executeWithPlan(getSession(), query);
        MaterializedResultWithPlan resultWithoutPredicatePushdown = getDistributedQueryRunner().executeWithPlan(
                withoutPredicatePushdown(getSession()),
                query);

        DataSize withPushdownDataSize = getOperatorStats(resultWithPredicatePushdown.queryId()).getInputDataSize();
        DataSize withoutPushdownDataSize = getOperatorStats(resultWithoutPredicatePushdown.queryId()).getInputDataSize();
        if (supportsPushdown) {
            assertThat(withPushdownDataSize).isLessThan(withoutPushdownDataSize);
        }
        else {
            assertThat(withPushdownDataSize).isEqualTo(withoutPushdownDataSize);
        }
    }

    private Session withoutPredicatePushdown(Session session)
    {
        return Session.builder(session)
                .setSystemProperty("allow_pushdown_into_connectors", "false")
                .build();
    }

    private void verifySplitCount(QueryId queryId, long expectedSplitCount)
    {
        checkArgument(expectedSplitCount >= 0);
        OperatorStats operatorStats = getOperatorStats(queryId);
        if (expectedSplitCount > 0) {
            assertThat(operatorStats.getTotalDrivers()).isEqualTo(expectedSplitCount);
            assertThat(operatorStats.getPhysicalInputPositions()).isGreaterThan(0);
            assertThat(operatorStats.getPhysicalInputReadTime().getValue()).isGreaterThan(0);
        }
        else {
            // expectedSplitCount == 0
            assertThat(operatorStats.getTotalDrivers()).isEqualTo(1);
            assertThat(operatorStats.getPhysicalInputPositions()).isEqualTo(0);
            assertThat(operatorStats.getPhysicalInputReadTime().toMillis()).isEqualTo(0);
        }
    }

    protected OperatorStats getOperatorStats(QueryId queryId)
    {
        try {
            return getDistributedQueryRunner().getCoordinator()
                    .getQueryManager()
                    .getFullQueryInfo(queryId)
                    .getQueryStats()
                    .getOperatorSummaries()
                    .stream()
                    .filter(summary -> summary.getOperatorType().startsWith("TableScan") || summary.getOperatorType().startsWith("Scan"))
                    .collect(onlyElement());
        }
        catch (NoSuchElementException e) {
            throw new RuntimeException("Couldn't find operator summary, probably due to query statistic collection error", e);
        }
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return abort("Iceberg connector does not support column default values");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("char(3)")) {
            // Use explicitly padded literal in char mapping test due to whitespace padding on coercion to varchar
            return Optional.of(new DataMappingTestSetup(typeName, "'ab '", dataMappingTestSetup.getHighValueLiteral()));
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Test
    public void testAmbiguousColumnsWithDots()
    {
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE ambiguous (\"a.cow\" BIGINT, a ROW(cow BIGINT))"))
                .hasMessage("Invalid schema: multiple fields for name a.cow: 1 and 3");

        assertUpdate("CREATE TABLE ambiguous (\"a.cow\" BIGINT, b ROW(cow BIGINT))");
        assertThatThrownBy(() -> assertUpdate("ALTER TABLE ambiguous RENAME COLUMN b TO a"))
                .hasMessage("Failed to rename column: Invalid schema: multiple fields for name a.cow: 1 and 3");
        assertUpdate("DROP TABLE ambiguous");

        assertUpdate("CREATE TABLE ambiguous (a ROW(cow BIGINT))");
        assertThatThrownBy(() -> assertUpdate("ALTER TABLE ambiguous ADD COLUMN \"a.cow\" BIGINT"))
                .hasMessage("Failed to add column: Cannot add column with ambiguous name: a.cow, use addColumn(parent, name, type)");
        assertUpdate("DROP TABLE ambiguous");
    }

    @Test
    public void testSchemaEvolutionWithDereferenceProjections()
    {
        // Fields are identified uniquely based on unique id's. If a column is dropped and recreated with the same name it should not return dropped data.
        String tableName = "evolve_test_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (dummy BIGINT, a row(b BIGINT, c VARCHAR))");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, ROW(1, 'abc'))", 1);
        assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN a");
        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN a ROW(b VARCHAR, c BIGINT)");
        assertQuery("SELECT a.b FROM " + tableName, "VALUES NULL");
        assertUpdate("DROP TABLE " + tableName);

        // Verify changing subfield ordering does not revive dropped data
        assertUpdate("CREATE TABLE " + tableName + " (dummy BIGINT, a ROW(b BIGINT, c VARCHAR), d BIGINT) with (partitioning = ARRAY['d'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, ROW(2, 'abc'), 3)", 1);
        assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN a");
        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN a ROW(c VARCHAR, b BIGINT)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (4, 5, ROW('def', 6))", 1);
        assertQuery("SELECT a.b FROM " + tableName + " WHERE d = 3", "VALUES NULL");
        assertQuery("SELECT a.b FROM " + tableName + " WHERE d = 5", "VALUES 6");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testProjectionPushdownAfterRename()
    {
        assertUpdate("CREATE TABLE projection_pushdown_after_rename (id INT, a ROW(b INT, c ROW (d INT)))");
        assertUpdate("INSERT INTO projection_pushdown_after_rename VALUES (1, ROW(2, ROW(3))), (11, ROW(12, ROW(13)))", 2);
        assertUpdate("INSERT INTO projection_pushdown_after_rename VALUES (21, ROW(22, ROW(23)))", 1);

        String expected = "VALUES (11, JSON '{\"b\":12,\"c\":{\"d\":13}}', 13)";
        assertQuery("SELECT id, CAST(a AS JSON), a.c.d FROM projection_pushdown_after_rename WHERE a.b = 12", expected);
        assertUpdate("ALTER TABLE projection_pushdown_after_rename RENAME COLUMN a TO row_t");
        assertQuery("SELECT id, CAST(row_t AS JSON), row_t.c.d FROM projection_pushdown_after_rename WHERE row_t.b = 12", expected);

        assertUpdate("DROP TABLE IF EXISTS projection_pushdown_after_rename");
    }

    @Test
    public void testProjectionPushdownOnPartitionedTables()
    {
        assertUpdate("CREATE TABLE table_with_partition_at_beginning (id BIGINT, root ROW(f1 BIGINT, f2 BIGINT)) WITH (partitioning = ARRAY['id'])");
        assertUpdate("INSERT INTO table_with_partition_at_beginning VALUES (1, ROW(1, 2)), (1, ROW(2, 3)), (1, ROW(3, 4))", 3);
        assertQuery("SELECT id, root.f2 FROM table_with_partition_at_beginning", "VALUES (1, 2), (1, 3), (1, 4)");
        assertUpdate("DROP TABLE table_with_partition_at_beginning");

        assertUpdate("CREATE TABLE table_with_partition_at_end (root ROW(f1 BIGINT, f2 BIGINT), id BIGINT) WITH (partitioning = ARRAY['id'])");
        assertUpdate("INSERT INTO table_with_partition_at_end VALUES (ROW(1, 2), 1), (ROW(2, 3), 1), (ROW(3, 4), 1)", 3);
        assertQuery("SELECT root.f2, id FROM table_with_partition_at_end", "VALUES (2, 1), (3, 1), (4, 1)");
        assertUpdate("DROP TABLE table_with_partition_at_end");
    }

    @Test
    public void testProjectionPushdownOnPartitionedTableWithComments()
    {
        assertUpdate("CREATE TABLE test_projection_pushdown_comments (id BIGINT COMMENT 'id', qid BIGINT COMMENT 'QID', root ROW(f1 BIGINT, f2 BIGINT) COMMENT 'root') WITH (partitioning = ARRAY['id'])");
        assertUpdate("INSERT INTO test_projection_pushdown_comments VALUES (1, 1, ROW(1, 2)), (1, 2, ROW(2, 3)), (1, 3, ROW(3, 4))", 3);
        assertQuery("SELECT id, root.f2 FROM test_projection_pushdown_comments", "VALUES (1, 2), (1, 3), (1, 4)");
        // Query with predicates on both nested and top-level columns (with partition column)
        assertQuery("SELECT id, root.f2 FROM test_projection_pushdown_comments WHERE id = 1 AND qid = 1 AND root.f1 = 1", "VALUES (1, 2)");
        // Query with predicates on both nested and top-level columns (no partition column)
        assertQuery("SELECT id, root.f2 FROM test_projection_pushdown_comments WHERE qid = 2 AND root.f1 = 2", "VALUES (1, 3)");
        // Query with predicates on top-level columns only
        assertQuery("SELECT id, root.f2 FROM test_projection_pushdown_comments WHERE id = 1 AND qid = 1", "VALUES (1, 2)");
        // Query with predicates on nested columns only
        assertQuery("SELECT id, root.f2 FROM test_projection_pushdown_comments WHERE root.f1 = 2", "VALUES (1, 3)");
        assertUpdate("DROP TABLE IF EXISTS test_projection_pushdown_comments");
    }

    @Test
    public void testOptimize()
            throws Exception
    {
        for (int formatVersion = IcebergConfig.FORMAT_VERSION_SUPPORT_MIN; formatVersion < IcebergConfig.FORMAT_VERSION_SUPPORT_MAX; formatVersion++) {
            String tableName = "test_optimize_" + randomNameSuffix();
            assertUpdate("CREATE TABLE " + tableName + " (key integer, value varchar) WITH (format_version = " + formatVersion + ")");

            // DistributedQueryRunner sets node-scheduler.include-coordinator by default, so include coordinator
            int workerCount = getQueryRunner().getNodeCount();

            // optimize an empty table
            assertQuerySucceeds(withSingleWriterPerTask(getSession()), "ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
            assertThat(getActiveFiles(tableName)).isEmpty();

            assertUpdate("INSERT INTO " + tableName + " VALUES (11, 'eleven')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (12, 'zwölf')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (13, 'trzynaście')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (14, 'quatorze')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (15, 'пʼятнадцять')", 1);

            List<String> initialFiles = getActiveFiles(tableName);
            assertThat(initialFiles)
                    .hasSize(5)
                    // Verify we have sufficiently many test rows with respect to worker count.
                    .hasSizeGreaterThan(workerCount);

            // For optimize we need to set task_min_writer_count to 1, otherwise it will create more than one file.
            computeActual(withSingleWriterPerTask(getSession()), "ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
            assertThat(query("SELECT sum(key), listagg(value, ' ') WITHIN GROUP (ORDER BY key) FROM " + tableName))
                    .matches("VALUES (BIGINT '65', VARCHAR 'eleven zwölf trzynaście quatorze пʼятнадцять')");
            List<String> updatedFiles = getActiveFiles(tableName);
            assertThat(updatedFiles)
                    .hasSizeBetween(1, workerCount)
                    .doesNotContainAnyElementsOf(initialFiles);
            // No files should be removed (this is expire_snapshots's job, when it exists)
            assertThat(getAllDataFilesFromTableDirectory(tableName))
                    .containsExactlyInAnyOrderElementsOf(concat(initialFiles, updatedFiles));

            // optimize with low retention threshold, nothing should change
            // For optimize we need to set task_min_writer_count to 1, otherwise it will create more than one file.
            computeActual(withSingleWriterPerTask(getSession()), "ALTER TABLE " + tableName + " EXECUTE OPTIMIZE (file_size_threshold => '33B')");
            assertThat(query("SELECT sum(key), listagg(value, ' ') WITHIN GROUP (ORDER BY key) FROM " + tableName))
                    .matches("VALUES (BIGINT '65', VARCHAR 'eleven zwölf trzynaście quatorze пʼятнадцять')");
            assertThat(getActiveFiles(tableName)).isEqualTo(updatedFiles);
            assertThat(getAllDataFilesFromTableDirectory(tableName))
                    .containsExactlyInAnyOrderElementsOf(concat(initialFiles, updatedFiles));

            // optimize with delimited procedure name
            assertQueryFails("ALTER TABLE " + tableName + " EXECUTE \"optimize\"", "Table procedure not registered: optimize");
            assertUpdate("ALTER TABLE " + tableName + " EXECUTE \"OPTIMIZE\"");
            // optimize with delimited parameter name (and procedure name)
            assertUpdate("ALTER TABLE " + tableName + " EXECUTE \"OPTIMIZE\" (\"file_size_threshold\" => '33B')"); // TODO (https://github.com/trinodb/trino/issues/11326) this should fail
            assertUpdate("ALTER TABLE " + tableName + " EXECUTE \"OPTIMIZE\" (\"FILE_SIZE_THRESHOLD\" => '33B')");
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testOptimizeForPartitionedTable()
            throws IOException
    {
        for (int formatVersion = IcebergConfig.FORMAT_VERSION_SUPPORT_MIN; formatVersion < IcebergConfig.FORMAT_VERSION_SUPPORT_MAX; formatVersion++) {
            // This test will have its own session to make sure partitioning is indeed forced and is not a result
            // of session configuration
            Session session = testSessionBuilder()
                    .setCatalog(getQueryRunner().getDefaultSession().getCatalog())
                    .setSchema(getQueryRunner().getDefaultSession().getSchema())
                    .setSystemProperty("use_preferred_write_partitioning", "true")
                    .build();
            String tableName = "test_repartitiong_during_optimize_" + randomNameSuffix();
            assertUpdate(session, "CREATE TABLE " + tableName + " (key varchar, value integer) WITH (format_version = " + formatVersion + ", partitioning = ARRAY['key'])");
            // optimize an empty table
            assertQuerySucceeds(withSingleWriterPerTask(session), "ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");

            assertUpdate(session, "INSERT INTO " + tableName + " VALUES ('one', 1)", 1);
            assertUpdate(session, "INSERT INTO " + tableName + " VALUES ('one', 2)", 1);
            assertUpdate(session, "INSERT INTO " + tableName + " VALUES ('one', 3)", 1);
            assertUpdate(session, "INSERT INTO " + tableName + " VALUES ('one', 4)", 1);
            assertUpdate(session, "INSERT INTO " + tableName + " VALUES ('one', 5)", 1);
            assertUpdate(session, "INSERT INTO " + tableName + " VALUES ('one', 6)", 1);
            assertUpdate(session, "INSERT INTO " + tableName + " VALUES ('one', 7)", 1);
            assertUpdate(session, "INSERT INTO " + tableName + " VALUES ('two', 8)", 1);
            assertUpdate(session, "INSERT INTO " + tableName + " VALUES ('two', 9)", 1);
            assertUpdate(session, "INSERT INTO " + tableName + " VALUES ('three', 10)", 1);

            List<String> initialFiles = getActiveFiles(tableName);
            assertThat(initialFiles).hasSize(10);

            // For optimize we need to set task_min_writer_count to 1, otherwise it will create more than one file.
            computeActual(withSingleWriterPerTask(session), "ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");

            assertThat(query(session, "SELECT sum(value), listagg(key, ' ') WITHIN GROUP (ORDER BY key) FROM " + tableName))
                    .matches("VALUES (BIGINT '55', VARCHAR 'one one one one one one one three two two')");

            List<String> updatedFiles = getActiveFiles(tableName);
            // as we force repartitioning there should be only 3 partitions
            assertThat(updatedFiles).hasSize(3);
            assertThat(getAllDataFilesFromTableDirectory(tableName)).containsExactlyInAnyOrderElementsOf(ImmutableSet.copyOf(concat(initialFiles, updatedFiles)));

            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test()
    public void testOptimizeTimePartitionedTable()
    {
        testOptimizeTimePartitionedTable("date", "%s", 15);
        testOptimizeTimePartitionedTable("date", "day(%s)", 15);
        testOptimizeTimePartitionedTable("date", "month(%s)", 3);
        testOptimizeTimePartitionedTable("timestamp(6)", "day(%s)", 15);
        testOptimizeTimePartitionedTable("timestamp(6)", "month(%s)", 3);
        testOptimizeTimePartitionedTable("timestamp(6) with time zone", "day(%s)", 15);
        testOptimizeTimePartitionedTable("timestamp(6) with time zone", "month(%s)", 3);
    }

    private void testOptimizeTimePartitionedTable(String dataType, String partitioningFormat, int expectedFilesAfterOptimize)
    {
        String tableName = "test_optimize_time_partitioned_" +
                (dataType + "_" + partitioningFormat).toLowerCase(ENGLISH).replaceAll("[^a-z0-9_]", "");
        assertUpdate(format("CREATE TABLE %s(p %s, val varchar) WITH (partitioning = ARRAY['%s'])", tableName, dataType, format(partitioningFormat, "p")));

        // Do several inserts so ensure more than one input file
        for (int hour = 0; hour < 5; hour++) {
            assertUpdate(
                    "" +
                            "INSERT INTO " + tableName + " " +
                            "SELECT CAST(t AS " + dataType + "), CAST(t AS varchar) " +
                            "FROM (" +
                            "    SELECT " +
                            "        TIMESTAMP '2022-01-16 10:05:06.123456 UTC'" +
                            "            + month * INTERVAL '1' MONTH " +
                            "            + day * INTERVAL '1' DAY " +
                            "            + " + hour + " * INTERVAL '1' HOUR " +
                            "            AS t" +
                            "    FROM UNNEST(sequence(1, 5)) AS _(month)" +
                            "    CROSS JOIN UNNEST(sequence(1, 5)) AS _(day)" +
                            ")",
                    25);
        }

        String optimizeDate = "DATE '2022-04-01'";
        assertThat((long) computeScalar("SELECT count(DISTINCT \"$path\") FROM " + tableName))
                .as("total file count")
                .isGreaterThanOrEqualTo(5);
        long filesBeforeOptimizeDate = (long) computeScalar("SELECT count(DISTINCT \"$path\") FROM " + tableName + " WHERE p < " + optimizeDate);
        assertThat(filesBeforeOptimizeDate)
                .as("file count before optimize date")
                .isGreaterThanOrEqualTo(5);
        assertThat((long) computeScalar("SELECT count(DISTINCT \"$path\") FROM " + tableName + " WHERE p >= " + optimizeDate))
                .as("file count after optimize date")
                .isGreaterThanOrEqualTo(5);

        assertUpdate(
                // For optimize we need to set task_min_writer_count to 1, otherwise it will create more than one file.
                // Use UTC zone so that DATE and TIMESTAMP WITH TIME ZONE comparisons align with partition boundaries.
                withSingleWriterPerTask(Session.builder(getSession())
                        .setTimeZoneKey(UTC_KEY)
                        .build()),
                "ALTER TABLE " + tableName + " EXECUTE optimize WHERE p >= " + optimizeDate);

        assertThat((long) computeScalar("SELECT count(DISTINCT \"$path\") FROM " + tableName + " WHERE p < " + optimizeDate))
                .as("file count before optimize date, after the optimize")
                .isEqualTo(filesBeforeOptimizeDate);
        assertThat((long) computeScalar("SELECT count(DISTINCT \"$path\") FROM " + tableName + " WHERE p >= " + optimizeDate))
                .as("file count after optimize date, after the optimize")
                .isEqualTo(expectedFilesAfterOptimize);

        // Verify that WHERE CAST(p AS date) ... form works in non-UTC zone
        assertUpdate(
                // For optimize we need to set task_min_writer_count to 1, otherwise it will create more than one file.
                withSingleWriterPerTask(Session.builder(getSession())
                        .setTimeZoneKey(getTimeZoneKey("Asia/Kathmandu"))
                        .build()),
                "ALTER TABLE " + tableName + " EXECUTE optimize WHERE CAST(p AS date) >= " + optimizeDate);

        // Table state shouldn't change substantially (but files may be rewritten)
        assertThat((long) computeScalar("SELECT count(DISTINCT \"$path\") FROM " + tableName + " WHERE p < " + optimizeDate))
                .as("file count before optimize date, after the second optimize")
                .isEqualTo(filesBeforeOptimizeDate);
        assertThat((long) computeScalar("SELECT count(DISTINCT \"$path\") FROM " + tableName + " WHERE p >= " + optimizeDate))
                .as("file count after optimize date, after the second optimize")
                .isEqualTo(expectedFilesAfterOptimize);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testOptimizeTableAfterDeleteWithFormatVersion2()
    {
        String tableName = "test_optimize_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM nation", 25);

        List<String> initialFiles = getActiveFiles(tableName);

        assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = 7", 1);

        // Verify that delete files exists
        assertQuery(
                "SELECT summary['total-delete-files'] FROM \"" + tableName + "$snapshots\" WHERE snapshot_id = " + getCurrentSnapshotId(tableName),
                "VALUES '1'");

        // For optimize we need to set task_min_writer_count to 1, otherwise it will create more than one file.
        computeActual(withSingleWriterPerTask(getSession()), "ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");

        List<String> updatedFiles = getActiveFiles(tableName);
        assertThat(updatedFiles)
                .hasSize(1)
                .isNotEqualTo(initialFiles);

        assertThat(query("SELECT * FROM " + tableName))
                .matches("SELECT * FROM nation WHERE nationkey != 7");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testOptimizeCleansUpDeleteFiles()
            throws IOException
    {
        String tableName = "test_optimize_" + randomNameSuffix();
        Session sessionWithShortRetentionUnlocked = prepareCleanUpSession();
        assertUpdate("CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['regionkey']) AS SELECT * FROM nation", 25);

        List<String> allDataFilesInitially = getAllDataFilesFromTableDirectory(tableName);
        assertThat(allDataFilesInitially).hasSize(5);

        assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = 7", 1);

        assertQuery(
                "SELECT summary['total-delete-files'] FROM \"" + tableName + "$snapshots\" WHERE snapshot_id = " + getCurrentSnapshotId(tableName),
                "VALUES '1'");

        List<String> allDataFilesAfterDelete = getAllDataFilesFromTableDirectory(tableName);
        assertThat(allDataFilesAfterDelete).hasSize(6);

        // For optimize we need to set task_min_writer_count to 1, otherwise it will create more than one file.
        computeActual(withSingleWriterPerTask(getSession()), "ALTER TABLE " + tableName + " EXECUTE OPTIMIZE WHERE regionkey = 3");
        computeActual(sessionWithShortRetentionUnlocked, "ALTER TABLE " + tableName + " EXECUTE EXPIRE_SNAPSHOTS (retention_threshold => '0s')");
        computeActual(sessionWithShortRetentionUnlocked, "ALTER TABLE " + tableName + " EXECUTE REMOVE_ORPHAN_FILES (retention_threshold => '0s')");

        assertQuery(
                "SELECT summary['total-delete-files'] FROM \"" + tableName + "$snapshots\" WHERE snapshot_id = " + getCurrentSnapshotId(tableName),
                "VALUES '0'");
        List<String> allDataFilesAfterOptimizeWithWhere = getAllDataFilesFromTableDirectory(tableName);
        assertThat(allDataFilesAfterOptimizeWithWhere)
                .hasSize(5)
                .doesNotContain(allDataFilesInitially.stream().filter(file -> file.contains("regionkey=3"))
                        .toArray(String[]::new))
                .contains(allDataFilesInitially.stream().filter(file -> !file.contains("regionkey=3"))
                        .toArray(String[]::new));

        assertThat(query("SELECT * FROM " + tableName))
                .matches("SELECT * FROM nation WHERE nationkey != 7");

        // For optimize we need to set task_min_writer_count to 1, otherwise it will create more than one file.
        computeActual(withSingleWriterPerTask(getSession()), "ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
        computeActual(sessionWithShortRetentionUnlocked, "ALTER TABLE " + tableName + " EXECUTE EXPIRE_SNAPSHOTS (retention_threshold => '0s')");
        computeActual(sessionWithShortRetentionUnlocked, "ALTER TABLE " + tableName + " EXECUTE REMOVE_ORPHAN_FILES (retention_threshold => '0s')");

        assertQuery(
                "SELECT summary['total-delete-files'] FROM \"" + tableName + "$snapshots\" WHERE snapshot_id = " + getCurrentSnapshotId(tableName),
                "VALUES '0'");
        List<String> allDataFilesAfterFullOptimize = getAllDataFilesFromTableDirectory(tableName);
        assertThat(allDataFilesAfterFullOptimize)
                .hasSize(5)
                // All files skipped from OPTIMIZE as they have no deletes and there's only one file per partition
                .contains(allDataFilesAfterOptimizeWithWhere.toArray(new String[0]));

        assertThat(query("SELECT * FROM " + tableName))
                .matches("SELECT * FROM nation WHERE nationkey != 7");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testOptimizeSnapshot()
    {
        String tableName = "test_optimize_snapshot_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a) AS VALUES 11", 1);
        long snapshotId = getCurrentSnapshotId(tableName);
        assertUpdate("INSERT INTO " + tableName + " VALUES 22", 1);
        assertThat(query("ALTER TABLE \"%s@%d\" EXECUTE OPTIMIZE".formatted(tableName, snapshotId)))
                .failure().hasMessage(format("line 1:7: Table 'iceberg.tpch.\"%s@%s\"' does not exist", tableName, snapshotId));
        assertThat(query("SELECT * FROM " + tableName))
                .matches("VALUES 11, 22");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testOptimizeSystemTable()
    {
        assertThat(query("ALTER TABLE \"nation$files\" EXECUTE OPTIMIZE"))
                .failure().hasMessage("This connector does not support table procedures");
        assertThat(query("ALTER TABLE \"nation$snapshots\" EXECUTE OPTIMIZE"))
                .failure().hasMessage("This connector does not support table procedures");
    }

    @Test
    void testOptimizeOnlyOneFileShouldHaveNoEffect()
    {
        String tableName = "test_optimize_one_file_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a integer)");
        assertUpdate("INSERT INTO " + tableName + " VALUES 1, 2", 2);

        List<String> initialFiles = getActiveFiles(tableName);
        assertThat(initialFiles).hasSize(1);

        computeActual("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
        assertThat(query("SELECT a FROM " + tableName))
                .matches("VALUES 1, 2");
        assertThat(getActiveFiles(tableName))
                .containsExactlyInAnyOrderElementsOf(initialFiles);

        assertUpdate("DELETE FROM " + tableName + " WHERE a = 1", 1);
        // Calling optimize after adding a DELETE should result in compaction
        computeActual("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
        assertThat(query("SELECT a FROM " + tableName))
                .matches("VALUES 2");
        assertThat(getActiveFiles(tableName))
                .hasSize(1)
                .doesNotContainAnyElementsOf(initialFiles);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testOptimizeAfterChangeInPartitioning()
    {
        String tableName = "test_optimize_after_change_in_partitioning_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['bucket(nationkey, 5)']) AS SELECT * FROM tpch.tiny.supplier", 100);
        List<String> initialFiles = getActiveFiles(tableName);
        assertThat(initialFiles).hasSize(5);

        // OPTIMIZE shouldn't have to rewrite files
        computeActual("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
        assertThat(query("SELECT COUNT(*) FROM " + tableName)).matches("VALUES BIGINT '100'");
        assertThat(getActiveFiles(tableName))
                .containsExactlyInAnyOrderElementsOf(initialFiles);

        // Change in partitioning should result in OPTIMIZE rewriting all files
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES partitioning = ARRAY['nationkey']");
        computeActual("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
        assertThat(query("SELECT COUNT(*) FROM " + tableName)).matches("VALUES BIGINT '100'");
        List<String> filesAfterPartioningChange = getActiveFiles(tableName);
        assertThat(filesAfterPartioningChange)
                .hasSize(25)
                .doesNotContainAnyElementsOf(initialFiles);

        // OPTIMIZE shouldn't have to rewrite files anymore
        computeActual("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
        assertThat(query("SELECT COUNT(*) FROM " + tableName)).matches("VALUES BIGINT '100'");
        assertThat(getActiveFiles(tableName))
                .hasSize(25)
                .containsExactlyInAnyOrderElementsOf(filesAfterPartioningChange);
    }

    private List<String> getActiveFiles(String tableName)
    {
        return computeActual(format("SELECT file_path FROM \"%s$files\"", tableName)).getOnlyColumn()
                .map(String.class::cast)
                .collect(toImmutableList());
    }

    protected String getTableLocation(String tableName)
    {
        Pattern locationPattern = Pattern.compile(".*location = '(.*?)'.*", Pattern.DOTALL);
        Matcher m = locationPattern.matcher((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue());
        if (m.find()) {
            String location = m.group(1);
            verify(!m.find(), "Unexpected second match");
            return location;
        }
        throw new IllegalStateException("Location not found in SHOW CREATE TABLE result");
    }

    protected List<String> getAllDataFilesFromTableDirectory(String tableName)
            throws IOException
    {
        return listFiles(getIcebergTableDataPath(getTableLocation(tableName)));
    }

    @Test
    public void testOptimizeParameterValidation()
    {
        assertQueryFails(
                "ALTER TABLE no_such_table_exists EXECUTE OPTIMIZE",
                "\\Qline 1:7: Table 'iceberg.tpch.no_such_table_exists' does not exist");
        assertQueryFails(
                "ALTER TABLE nation EXECUTE OPTIMIZE (file_size_threshold => '33')",
                "\\Qline 1:38: Unable to set catalog 'iceberg' table procedure 'OPTIMIZE' property 'file_size_threshold' to ['33']: size is not a valid data size string: 33");
        assertQueryFails(
                "ALTER TABLE nation EXECUTE OPTIMIZE (file_size_threshold => '33s')",
                "\\Qline 1:38: Unable to set catalog 'iceberg' table procedure 'OPTIMIZE' property 'file_size_threshold' to ['33s']: Unknown unit: s");
    }

    @Test
    public void testTargetMaxFileSize()
    {
        String tableName = "test_default_max_file_size" + randomNameSuffix();
        @Language("SQL") String createTableSql = format("CREATE TABLE %s AS SELECT * FROM tpch.sf1.lineitem LIMIT 100000", tableName);

        Session session = Session.builder(getSession())
                .setSystemProperty("task_min_writer_count", "1")
                // task scale writers should be disabled since we want to write with a single task writer
                .setSystemProperty("task_scale_writers_enabled", "false")
                .build();
        assertUpdate(session, createTableSql, 100000);
        List<String> initialFiles = getActiveFiles(tableName);
        assertThat(initialFiles.size()).isLessThanOrEqualTo(3);
        assertUpdate(format("DROP TABLE %s", tableName));

        DataSize maxSize = DataSize.of(40, DataSize.Unit.KILOBYTE);
        session = Session.builder(getSession())
                .setSystemProperty("task_min_writer_count", "1")
                // task scale writers should be disabled since we want to write with a single task writer
                .setSystemProperty("task_scale_writers_enabled", "false")
                .setCatalogSessionProperty("iceberg", "target_max_file_size", maxSize.toString())
                .build();

        assertUpdate(session, createTableSql, 100000);
        assertThat(query(format("SELECT count(*) FROM %s", tableName))).matches("VALUES BIGINT '100000'");
        List<String> updatedFiles = getActiveFiles(tableName);
        assertThat(updatedFiles.size()).isGreaterThan(10);

        computeActual(format("SELECT file_size_in_bytes FROM \"%s$files\"", tableName))
                .getMaterializedRows()
                // as target_max_file_size is set to quite low value it can happen that created files are bigger,
                // so just to be safe we check if it is not much bigger
                .forEach(row -> assertThat((Long) row.getField(0)).isBetween(1L, maxSize.toBytes() * 6));
    }

    @Test
    public void testTargetMaxFileSizeOnSortedTable()
    {
        String tableName = "test_default_max_file_size_sorted_" + randomNameSuffix();
        @Language("SQL") String createTableSql = format("CREATE TABLE %s WITH (sorted_by = ARRAY['shipdate']) AS SELECT * FROM tpch.sf1.lineitem LIMIT 100000", tableName);

        Session session = Session.builder(getSession())
                .setSystemProperty("task_min_writer_count", "1")
                // task scale writers should be disabled since we want to write with a single task writer
                .setSystemProperty("task_scale_writers_enabled", "false")
                .build();
        assertUpdate(session, createTableSql, 100000);
        List<String> initialFiles = getActiveFiles(tableName);
        assertThat(initialFiles.size()).isLessThanOrEqualTo(3);
        assertUpdate(format("DROP TABLE %s", tableName));

        DataSize maxSize = DataSize.of(40, DataSize.Unit.KILOBYTE);
        session = Session.builder(getSession())
                .setSystemProperty("task_min_writer_count", "1")
                // task scale writers should be disabled since we want to write with a single task writer
                .setSystemProperty("task_scale_writers_enabled", "false")
                .setCatalogSessionProperty("iceberg", "target_max_file_size", maxSize.toString())
                .build();

        assertUpdate(session, createTableSql, 100000);
        assertThat(query(format("SELECT count(*) FROM %s", tableName))).matches("VALUES BIGINT '100000'");
        List<String> updatedFiles = getActiveFiles(tableName);
        assertThat(updatedFiles.size()).isGreaterThan(5);

        computeActual(format("SELECT file_size_in_bytes FROM \"%s$files\"", tableName))
                .getMaterializedRows()
                // as target_max_file_size is set to quite low value it can happen that created files are bigger,
                // so just to be safe we check if it is not much bigger
                .forEach(row -> assertThat((Long) row.getField(0)).isBetween(1L, maxSize.toBytes() * 20));
    }

    @Test
    public void testDroppingIcebergAndCreatingANewTableWithTheSameNameShouldBePossible()
    {
        assertUpdate("CREATE TABLE test_iceberg_recreate (a_int) AS VALUES (1)", 1);
        assertThat(query("SELECT min(a_int) FROM test_iceberg_recreate")).matches("VALUES 1");
        assertUpdate("DROP TABLE test_iceberg_recreate");

        assertUpdate("CREATE TABLE test_iceberg_recreate (a_varchar) AS VALUES ('Trino')", 1);
        assertThat(query("SELECT min(a_varchar) FROM test_iceberg_recreate")).matches("VALUES CAST('Trino' AS varchar)");
        assertUpdate("DROP TABLE test_iceberg_recreate");
    }

    @Test
    public void testDropTableDeleteData()
    {
        String tableName = "test_drop_table_delete_data" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a_int) AS VALUES (1)", 1);
        String tableLocation = getTableLocation(tableName);
        assertUpdate("DROP TABLE " + tableName);

        // Create a new table with the same location to verify the data was deleted in the above DROP TABLE
        assertUpdate("CREATE TABLE " + tableName + "(a_int INTEGER) WITH (location = '" + tableLocation + "')");
        assertQueryReturnsEmptyResult("SELECT * FROM " + tableName);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testPathHiddenColumn()
    {
        String tableName = "test_path_" + randomNameSuffix();
        @Language("SQL") String createTable = "CREATE TABLE " + tableName + " " +
                "WITH ( partitioning = ARRAY['zip'] ) AS " +
                "SELECT * FROM (VALUES " +
                "(0, 0), (3, 0), (6, 0), " +
                "(1, 1), (4, 1), (7, 1), " +
                "(2, 2), (5, 2) " +
                " ) t(userid, zip)";
        assertUpdate(createTable, 8);

        // Describe output should not have the $path hidden column
        assertThat(query("DESCRIBE " + tableName))
                .skippingTypesCheck()
                .matches("VALUES ('userid', 'integer', '', ''), ('zip', 'integer', '', '')");

        assertThat(query("SELECT file_path FROM \"" + tableName + "$files\""))
                .matches("SELECT DISTINCT \"$path\" as file_path FROM " + tableName);

        String somePath = (String) computeScalar("SELECT \"$path\" FROM " + tableName + " WHERE userid = 2");
        String anotherPath = (String) computeScalar("SELECT \"$path\" FROM " + tableName + " WHERE userid = 3");
        assertThat(query("SELECT userid FROM " + tableName + " WHERE \"$path\" = '" + somePath + "'"))
                .matches("VALUES 2, 5")
                .isFullyPushedDown();
        assertThat(query("SELECT userid FROM " + tableName + " WHERE \"$path\" IN ('" + somePath + "', '" + anotherPath + "')"))
                .matches("VALUES 0, 2, 3, 5, 6")
                .isFullyPushedDown();
        assertThat(query("SELECT userid FROM " + tableName + " WHERE \"$path\" <> '" + somePath + "'"))
                .matches("VALUES 0, 1, 3, 4, 6, 7")
                .isFullyPushedDown();
        assertThat(query("SELECT userid FROM " + tableName + " WHERE \"$path\" = '" + somePath + "' AND userid > 0"))
                .matches("VALUES 2, 5");

        assertThat(query("SELECT userid FROM " + tableName + " WHERE \"$path\" IS NOT NULL"))
                .matches("VALUES 0, 1, 2, 3, 4, 5, 6, 7")
                .isFullyPushedDown();
        assertThat(query("SELECT userid FROM " + tableName + " WHERE \"$path\" IS NULL"))
                .returnsEmptyResult()
                .isFullyPushedDown();

        assertQuerySucceeds("SHOW STATS FOR (SELECT userid FROM " + tableName + " WHERE \"$path\" = '" + somePath + "')");
        // EXPLAIN triggers stats calculation and also rendering
        assertQuerySucceeds("EXPLAIN SELECT userid FROM " + tableName + " WHERE \"$path\" = '" + somePath + "'");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testOptimizeWithPathColumn()
    {
        String tableName = "test_optimize_with_path_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id integer)");

        assertUpdate("INSERT INTO " + tableName + " VALUES (1)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (3)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (4)", 1);

        String firstPath = (String) computeScalar("SELECT \"$path\" FROM " + tableName + " WHERE id = 1");
        String secondPath = (String) computeScalar("SELECT \"$path\" FROM " + tableName + " WHERE id = 2");
        String thirdPath = (String) computeScalar("SELECT \"$path\" FROM " + tableName + " WHERE id = 3");
        String fourthPath = (String) computeScalar("SELECT \"$path\" FROM " + tableName + " WHERE id = 4");

        List<String> initialFiles = getActiveFiles(tableName);
        assertThat(initialFiles).hasSize(4);

        // For optimize we need to set task_min_writer_count to 1, otherwise it will create more than one file.
        assertQuerySucceeds(withSingleWriterPerTask(getSession()), "ALTER TABLE " + tableName + " EXECUTE OPTIMIZE WHERE \"$path\" = '" + firstPath + "' OR \"$path\" = '" + secondPath + "'");
        assertQuerySucceeds(withSingleWriterPerTask(getSession()), "ALTER TABLE " + tableName + " EXECUTE OPTIMIZE WHERE \"$path\" = '" + thirdPath + "' OR \"$path\" = '" + fourthPath + "'");

        List<String> updatedFiles = getActiveFiles(tableName);
        assertThat(updatedFiles)
                .hasSize(2)
                .doesNotContainAnyElementsOf(initialFiles);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCollectingStatisticsWithPathColumnPredicate()
    {
        assertQuerySucceeds("EXPLAIN SELECT * FROM region WHERE \"$path\" = ''");

        Session collectingStatisticsSession = Session.builder(getSession())
                .setSystemProperty("collect_plan_statistics_for_all_queries", "true")
                .build();
        String tableName = "test_collect_statistics_with_path_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(id integer, value integer)");

        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 1)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 2)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (3, null)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (4, 4)", 1);

        // Make sure the whole table has stats
        MaterializedResult tableStatistics = computeActual(collectingStatisticsSession, "SHOW STATS FOR (SELECT * FROM %s WHERE \"$path\" IS NOT NULL)".formatted(tableName));
        MaterializedResult expectedTableStatistics =
                resultBuilder(collectingStatisticsSession, VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("id", null, 4.0, 0.0, null, "1", "4")
                        .row("value", null, 3.0, 0.25, null, "1", "4")
                        .row(null, null, null, null, 4.0, null, null)
                        .build();
        if (format == AVRO) {
            expectedTableStatistics =
                    resultBuilder(collectingStatisticsSession, VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                            .row("id", null, 4.0, 0.0, null, null, null)
                            .row("value", null, 3.0, 0.1, null, null, null)
                            .row(null, null, null, null, 4.0, null, null)
                            .build();
        }
        assertThat(tableStatistics).containsExactlyElementsOf(expectedTableStatistics);

        String firstPath = (String) computeScalar(collectingStatisticsSession, "SELECT \"$path\" FROM " + tableName + " WHERE id = 1");
        String secondPath = (String) computeScalar(collectingStatisticsSession, "SELECT \"$path\" FROM " + tableName + " WHERE id = 2");
        String thirdPath = (String) computeScalar(collectingStatisticsSession, "SELECT \"$path\" FROM " + tableName + " WHERE id = 3");
        String fourthPath = (String) computeScalar(collectingStatisticsSession, "SELECT \"$path\" FROM " + tableName + " WHERE id = 4");

        String pathPredicateSql = "SELECT * FROM " + tableName + " WHERE \"$path\" = '%s'";
        // Check the predicate with path
        assertQuery(collectingStatisticsSession, pathPredicateSql.formatted(firstPath), "VALUES (1, 1)");
        assertQuery(collectingStatisticsSession, pathPredicateSql.formatted(secondPath), "VALUES (2, 2)");
        assertQuery(collectingStatisticsSession, "SELECT COUNT(*) FROM %s WHERE \"$path\" = '%s' OR \"$path\" = '%s'".formatted(tableName, thirdPath, fourthPath), "VALUES 2");

        MaterializedResult firstPathStatistics = computeActual(collectingStatisticsSession, "SHOW STATS FOR (" + pathPredicateSql.formatted(firstPath) + ")");
        MaterializedResult expectedFirstPathStatistics =
                resultBuilder(collectingStatisticsSession, VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("id", null, 1.0, 0.0, null, "1", "1")
                        .row("value", null, 1.0, 0.0, null, "1", "1")
                        .row(null, null, null, null, 1.0, null, null)
                        .build();
        if (format == AVRO) {
            expectedFirstPathStatistics =
                    resultBuilder(collectingStatisticsSession, VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                            .row("id", null, 1.0, 0.0, null, null, null)
                            .row("value", null, 1.0, 0.0, null, null, null)
                            .row(null, null, null, null, 1.0, null, null)
                            .build();
        }
        assertThat(firstPathStatistics).containsExactlyElementsOf(expectedFirstPathStatistics);

        MaterializedResult secondThirdPathStatistics = computeActual(collectingStatisticsSession, "SHOW STATS FOR (SELECT * FROM %s WHERE \"$path\" IN ('%s', '%s'))".formatted(tableName, secondPath, thirdPath));
        MaterializedResult expectedSecondThirdPathStatistics =
                resultBuilder(collectingStatisticsSession, VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("id", null, 2.0, 0.0, null, "2", "3")
                        .row("value", null, 1.0, 0.5, null, "2", "2")
                        .row(null, null, null, null, 2.0, null, null)
                        .build();
        if (format == AVRO) {
            expectedSecondThirdPathStatistics =
                    resultBuilder(collectingStatisticsSession, VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                            .row("id", null, 2.0, 0.0, null, null, null)
                            .row("value", null, 2.0, 0.0, null, null, null)
                            .row(null, null, null, null, 2.0, null, null)
                            .build();
        }
        assertThat(secondThirdPathStatistics).containsExactlyElementsOf(expectedSecondThirdPathStatistics);

        MaterializedResult fourthPathStatistics = computeActual(collectingStatisticsSession, "SHOW STATS FOR (" + pathPredicateSql.formatted(fourthPath) + ")");
        MaterializedResult expectedFourthPathStatistics =
                resultBuilder(collectingStatisticsSession, VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("id", null, 1.0, 0.0, null, "4", "4")
                        .row("value", null, 1.0, 0.0, null, "4", "4")
                        .row(null, null, null, null, 1.0, null, null)
                        .build();
        if (format == AVRO) {
            expectedFourthPathStatistics =
                    resultBuilder(collectingStatisticsSession, VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                            .row("id", null, 1.0, 0.0, null, null, null)
                            .row("value", null, 1.0, 0.0, null, null, null)
                            .row(null, null, null, null, 1.0, null, null)
                            .build();
        }
        assertThat(fourthPathStatistics).containsExactlyElementsOf(expectedFourthPathStatistics);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCollectingStatisticsWithFileModifiedTimeColumnPredicate()
            throws InterruptedException
    {
        assertQuerySucceeds("EXPLAIN SELECT * FROM region WHERE \"$file_modified_time\" = TIMESTAMP '2001-08-22 03:04:05.321 UTC'");

        Session collectingStatisticsSession = Session.builder(getSession())
                .setSystemProperty("collect_plan_statistics_for_all_queries", "true")
                .build();
        String tableName = "test_collect_statistics_with_file_modified_time_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(id integer, value integer)");

        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 1)", 1);
        storageTimePrecision.sleep(1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 2)", 1);
        storageTimePrecision.sleep(1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (3, null)", 1);
        storageTimePrecision.sleep(1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (4, 4)", 1);

        // Make sure the whole table has stats
        MaterializedResult tableStatistics = computeActual(collectingStatisticsSession, "SHOW STATS FOR (SELECT * FROM %s WHERE \"$file_modified_time\" IS NOT NULL)".formatted(tableName));
        MaterializedResult expectedTableStatistics =
                resultBuilder(collectingStatisticsSession, VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("id", null, 4.0, 0.0, null, "1", "4")
                        .row("value", null, 3.0, 0.25, null, "1", "4")
                        .row(null, null, null, null, 4.0, null, null)
                        .build();
        if (format == AVRO) {
            expectedTableStatistics =
                    resultBuilder(collectingStatisticsSession, VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                            .row("id", null, 4.0, 0.0, null, null, null)
                            .row("value", null, 3.0, 0.1, null, null, null)
                            .row(null, null, null, null, 4.0, null, null)
                            .build();
        }
        assertThat(tableStatistics).containsExactlyElementsOf(expectedTableStatistics);

        ZonedDateTime firstFileModifiedTime = (ZonedDateTime) computeScalar(collectingStatisticsSession, "SELECT \"$file_modified_time\" FROM " + tableName + " WHERE id = 1");
        ZonedDateTime secondFileModifiedTime = (ZonedDateTime) computeScalar(collectingStatisticsSession, "SELECT \"$file_modified_time\" FROM " + tableName + " WHERE id = 2");
        ZonedDateTime thirdFileModifiedTime = (ZonedDateTime) computeScalar(collectingStatisticsSession, "SELECT \"$file_modified_time\" FROM " + tableName + " WHERE id = 3");
        ZonedDateTime fourthFileModifiedTime = (ZonedDateTime) computeScalar(collectingStatisticsSession, "SELECT \"$file_modified_time\" FROM " + tableName + " WHERE id = 4");

        String fileModifiedTimePredicateSql = "SELECT * FROM " + tableName + " WHERE \"$file_modified_time\" = from_iso8601_timestamp('%s')";
        // Check the predicate with fileModifiedTime
        assertQuery(collectingStatisticsSession, fileModifiedTimePredicateSql.formatted(firstFileModifiedTime.format(ISO_OFFSET_DATE_TIME)), "SELECT 1, 1");
        assertQuery(collectingStatisticsSession, fileModifiedTimePredicateSql.formatted(secondFileModifiedTime.format(ISO_OFFSET_DATE_TIME)), "SELECT 2, 2");
        assertQuery(collectingStatisticsSession, "SELECT COUNT(*) FROM %s WHERE \"$file_modified_time\" = from_iso8601_timestamp('%s') OR \"$file_modified_time\" = from_iso8601_timestamp('%s')".formatted(tableName, thirdFileModifiedTime.format(ISO_OFFSET_DATE_TIME), fourthFileModifiedTime.format(ISO_OFFSET_DATE_TIME)), "VALUES 2");

        MaterializedResult firstFileModifiedTimeStatistics = computeActual(collectingStatisticsSession, "SHOW STATS FOR (" + fileModifiedTimePredicateSql.formatted(firstFileModifiedTime.format(ISO_OFFSET_DATE_TIME)) + ")");
        MaterializedResult expectedFirstFileModifiedTimeStatistics =
                resultBuilder(collectingStatisticsSession, VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("id", null, 1.0, 0.0, null, "1", "1")
                        .row("value", null, 1.0, 0.0, null, "1", "1")
                        .row(null, null, null, null, 1.0, null, null)
                        .build();
        if (format == AVRO) {
            expectedFirstFileModifiedTimeStatistics =
                    resultBuilder(collectingStatisticsSession, VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                            .row("id", null, 1.0, 0.0, null, null, null)
                            .row("value", null, 1.0, 0.0, null, null, null)
                            .row(null, null, null, null, 1.0, null, null)
                            .build();
        }
        assertThat(firstFileModifiedTimeStatistics).containsExactlyElementsOf(expectedFirstFileModifiedTimeStatistics);

        MaterializedResult secondThirdFileModifiedTimeStatistics = computeActual(collectingStatisticsSession, "SHOW STATS FOR (SELECT * FROM %s WHERE \"$file_modified_time\" IN (from_iso8601_timestamp('%s'), from_iso8601_timestamp('%s')))".formatted(tableName, secondFileModifiedTime.format(ISO_OFFSET_DATE_TIME), thirdFileModifiedTime.format(ISO_OFFSET_DATE_TIME)));
        MaterializedResult expectedSecondThirdFileModifiedTimetatistics =
                resultBuilder(collectingStatisticsSession, VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("id", null, 2.0, 0.0, null, "2", "3")
                        .row("value", null, 1.0, 0.5, null, "2", "2")
                        .row(null, null, null, null, 2.0, null, null)
                        .build();
        if (format == AVRO) {
            expectedSecondThirdFileModifiedTimetatistics =
                    resultBuilder(collectingStatisticsSession, VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                            .row("id", null, 2.0, 0.0, null, null, null)
                            .row("value", null, 2.0, 0.0, null, null, null)
                            .row(null, null, null, null, 2.0, null, null)
                            .build();
        }
        assertThat(secondThirdFileModifiedTimeStatistics).containsExactlyElementsOf(expectedSecondThirdFileModifiedTimetatistics);

        MaterializedResult fourthFileModifiedTimeStatistics = computeActual(collectingStatisticsSession, "SHOW STATS FOR (" + fileModifiedTimePredicateSql.formatted(fourthFileModifiedTime.format(ISO_OFFSET_DATE_TIME)) + ")");
        MaterializedResult expectedFourthFileModifiedTimeStatistics =
                resultBuilder(collectingStatisticsSession, VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("id", null, 1.0, 0.0, null, "4", "4")
                        .row("value", null, 1.0, 0.0, null, "4", "4")
                        .row(null, null, null, null, 1.0, null, null)
                        .build();
        if (format == AVRO) {
            expectedFourthFileModifiedTimeStatistics =
                    resultBuilder(collectingStatisticsSession, VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                            .row("id", null, 1.0, 0.0, null, null, null)
                            .row("value", null, 1.0, 0.0, null, null, null)
                            .row(null, null, null, null, 1.0, null, null)
                            .build();
        }
        assertThat(fourthFileModifiedTimeStatistics).containsExactlyElementsOf(expectedFourthFileModifiedTimeStatistics);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDeleteWithPathColumn()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_with_path_", "(key int)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1)", 1);
            sleepUninterruptibly(1, MILLISECONDS);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (2)", 1);

            String firstFilePath = (String) computeScalar("SELECT \"$path\" FROM " + table.getName() + " WHERE key = 1");
            assertUpdate("DELETE FROM " + table.getName() + " WHERE \"$path\" = '" + firstFilePath + "'", 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES 2");
        }
    }

    @Test
    public void testFileModifiedTimeHiddenColumn()
            throws Exception
    {
        ZonedDateTime beforeTime = (ZonedDateTime) computeScalar("SELECT current_timestamp(3)");
        if (storageTimePrecision.toMillis(1) > 1) {
            storageTimePrecision.sleep(1);
        }
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_file_modified_time_", "(col) AS VALUES (1)")) {
            // Describe output should not have the $file_modified_time hidden column
            assertThat(query("DESCRIBE " + table.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES ('col', 'integer', '', '')");

            ZonedDateTime fileModifiedTime = (ZonedDateTime) computeScalar("SELECT \"$file_modified_time\" FROM " + table.getName());
            ZonedDateTime afterTime = (ZonedDateTime) computeScalar("SELECT current_timestamp(3)");
            assertThat(fileModifiedTime).isBetween(beforeTime, afterTime);

            storageTimePrecision.sleep(1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (2)", 1);
            ZonedDateTime anotherFileModifiedTime = (ZonedDateTime) computeScalar("SELECT max(\"$file_modified_time\") FROM " + table.getName());
            assertThat(fileModifiedTime)
                    .isNotEqualTo(anotherFileModifiedTime);
            assertThat(anotherFileModifiedTime).isAfter(fileModifiedTime); // to detect potential clock backward adjustment

            assertThat(query("SELECT col FROM " + table.getName() + " WHERE \"$file_modified_time\" = from_iso8601_timestamp('" + fileModifiedTime.format(ISO_OFFSET_DATE_TIME) + "')"))
                    .matches("VALUES 1")
                    .isFullyPushedDown();
            assertThat(query("SELECT col FROM " + table.getName() + " WHERE \"$file_modified_time\" IN (from_iso8601_timestamp('" + fileModifiedTime.format(ISO_OFFSET_DATE_TIME) + "'), from_iso8601_timestamp('" + anotherFileModifiedTime.format(ISO_OFFSET_DATE_TIME) + "'))"))
                    .matches("VALUES 1, 2")
                    .isFullyPushedDown();
            assertThat(query("SELECT col FROM " + table.getName() + " WHERE \"$file_modified_time\" <> from_iso8601_timestamp('" + fileModifiedTime.format(ISO_OFFSET_DATE_TIME) + "')"))
                    .matches("VALUES 2")
                    .isFullyPushedDown();
            assertThat(query("SELECT col FROM " + table.getName() + " WHERE \"$file_modified_time\" IS NOT NULL"))
                    .matches("VALUES 1, 2")
                    .isFullyPushedDown();
            assertThat(query("SELECT col FROM " + table.getName() + " WHERE \"$file_modified_time\" IS NULL"))
                    .returnsEmptyResult()
                    .isFullyPushedDown();

            assertQuerySucceeds("SHOW STATS FOR (SELECT col FROM " + table.getName() + " WHERE \"$file_modified_time\" = from_iso8601_timestamp('" + fileModifiedTime.format(ISO_OFFSET_DATE_TIME) + "'))");
            // EXPLAIN triggers stats calculation and also rendering
            assertQuerySucceeds("EXPLAIN SELECT col FROM " + table.getName() + " WHERE \"$file_modified_time\" = from_iso8601_timestamp('" + fileModifiedTime.format(ISO_OFFSET_DATE_TIME) + "')");
        }
    }

    @Test
    public void testOptimizeWithFileModifiedTimeColumn()
            throws Exception
    {
        String tableName = "test_optimize_with_file_modified_time_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id integer)");

        assertUpdate("INSERT INTO " + tableName + " VALUES (1)", 1);
        storageTimePrecision.sleep(1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2)", 1);
        storageTimePrecision.sleep(1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (3)", 1);
        storageTimePrecision.sleep(1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (4)", 1);

        ZonedDateTime firstFileModifiedTime = (ZonedDateTime) computeScalar("SELECT \"$file_modified_time\" FROM " + tableName + " WHERE id = 1");
        ZonedDateTime secondFileModifiedTime = (ZonedDateTime) computeScalar("SELECT \"$file_modified_time\" FROM " + tableName + " WHERE id = 2");
        ZonedDateTime thirdFileModifiedTime = (ZonedDateTime) computeScalar("SELECT \"$file_modified_time\" FROM " + tableName + " WHERE id = 3");
        ZonedDateTime fourthFileModifiedTime = (ZonedDateTime) computeScalar("SELECT \"$file_modified_time\" FROM " + tableName + " WHERE id = 4");
        // Sanity check
        assertThat(List.of(firstFileModifiedTime, secondFileModifiedTime, thirdFileModifiedTime, fourthFileModifiedTime))
                .doesNotHaveDuplicates();

        List<String> initialFiles = getActiveFiles(tableName);
        assertThat(initialFiles).hasSize(4);

        storageTimePrecision.sleep(1);
        // For optimize we need to set task_min_writer_count to 1, otherwise it will create more than one file.
        assertQuerySucceeds(withSingleWriterPerTask(getSession()), "ALTER TABLE " + tableName + " EXECUTE OPTIMIZE WHERE " +
                "\"$file_modified_time\" = from_iso8601_timestamp('" + firstFileModifiedTime.format(ISO_OFFSET_DATE_TIME) + "') OR " +
                "\"$file_modified_time\" = from_iso8601_timestamp('" + secondFileModifiedTime.format(ISO_OFFSET_DATE_TIME) + "')");
        assertQuerySucceeds(withSingleWriterPerTask(getSession()), "ALTER TABLE " + tableName + " EXECUTE OPTIMIZE WHERE " +
                "\"$file_modified_time\" = from_iso8601_timestamp('" + thirdFileModifiedTime.format(ISO_OFFSET_DATE_TIME) + "') OR " +
                "\"$file_modified_time\" = from_iso8601_timestamp('" + fourthFileModifiedTime.format(ISO_OFFSET_DATE_TIME) + "')");

        List<String> updatedFiles = getActiveFiles(tableName);
        assertThat(updatedFiles)
                .hasSize(2)
                .doesNotContainAnyElementsOf(initialFiles);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDeleteWithFileModifiedTimeColumn()
            throws Exception
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_with_file_modified_time_", "(key int)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1)", 1);
            storageTimePrecision.sleep(1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (2)", 1);

            ZonedDateTime oldModifiedTime = (ZonedDateTime) computeScalar("SELECT \"$file_modified_time\" FROM " + table.getName() + " WHERE key = 1");
            assertUpdate("DELETE FROM " + table.getName() + " WHERE \"$file_modified_time\" = from_iso8601_timestamp('" + oldModifiedTime.format(ISO_OFFSET_DATE_TIME) + "')", 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES 2");
        }
    }

    @Test
    public void testExpireSnapshots()
            throws Exception
    {
        String tableName = "test_expiring_snapshots_" + randomNameSuffix();
        Session sessionWithShortRetentionUnlocked = prepareCleanUpSession();
        assertUpdate("CREATE TABLE " + tableName + " (key varchar, value integer)");
        assertUpdate("INSERT INTO " + tableName + " VALUES ('one', 1)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('two', 2)", 1);
        assertThat(query("SELECT sum(value), listagg(key, ' ') WITHIN GROUP (ORDER BY key) FROM " + tableName))
                .matches("VALUES (BIGINT '3', VARCHAR 'one two')");

        List<Long> initialSnapshots = getSnapshotIds(tableName);
        String tableLocation = getTableLocation(tableName);
        List<String> initialFiles = getAllMetadataFilesFromTableDirectory(tableLocation);
        assertQuerySucceeds(sessionWithShortRetentionUnlocked, "ALTER TABLE " + tableName + " EXECUTE EXPIRE_SNAPSHOTS (retention_threshold => '0s')");

        assertThat(query("SELECT sum(value), listagg(key, ' ') WITHIN GROUP (ORDER BY key) FROM " + tableName))
                .matches("VALUES (BIGINT '3', VARCHAR 'one two')");
        List<String> updatedFiles = getAllMetadataFilesFromTableDirectory(tableLocation);
        List<Long> updatedSnapshots = getSnapshotIds(tableName);
        assertThat(updatedFiles).hasSize(initialFiles.size() - 2);
        assertThat(updatedSnapshots.size()).isLessThan(initialSnapshots.size());
        assertThat(updatedSnapshots).hasSize(1);
        assertThat(initialSnapshots).containsAll(updatedSnapshots);
    }

    @Test
    public void testExpireSnapshotsPartitionedTable()
            throws Exception
    {
        String tableName = "test_expiring_snapshots_partitioned_table" + randomNameSuffix();
        Session sessionWithShortRetentionUnlocked = prepareCleanUpSession();
        assertUpdate("CREATE TABLE " + tableName + " (col1 BIGINT, col2 BIGINT) WITH (partitioning = ARRAY['col1'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES(1, 100), (1, 101), (1, 102), (2, 200), (2, 201), (3, 300)", 6);
        assertUpdate("DELETE FROM " + tableName + " WHERE col1 = 1", 3);
        assertUpdate("INSERT INTO " + tableName + " VALUES(4, 400)", 1);
        assertQuery("SELECT sum(col2) FROM " + tableName, "SELECT 1101");
        List<String> initialDataFiles = getAllDataFilesFromTableDirectory(tableName);
        List<Long> initialSnapshots = getSnapshotIds(tableName);

        assertQuerySucceeds(sessionWithShortRetentionUnlocked, "ALTER TABLE " + tableName + " EXECUTE EXPIRE_SNAPSHOTS (retention_threshold => '0s')");

        List<String> updatedDataFiles = getAllDataFilesFromTableDirectory(tableName);
        List<Long> updatedSnapshots = getSnapshotIds(tableName);
        assertQuery("SELECT sum(col2) FROM " + tableName, "SELECT 1101");
        assertThat(updatedDataFiles.size()).isLessThan(initialDataFiles.size());
        assertThat(updatedSnapshots.size()).isLessThan(initialSnapshots.size());
    }

    @Test
    public void testExpireSnapshotsOnSnapshot()
    {
        String tableName = "test_expire_snapshots_on_snapshot_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a) AS VALUES 11", 1);
        long snapshotId = getCurrentSnapshotId(tableName);
        assertUpdate("INSERT INTO " + tableName + " VALUES 22", 1);
        assertThat(query("ALTER TABLE \"%s@%d\" EXECUTE EXPIRE_SNAPSHOTS".formatted(tableName, snapshotId)))
                .failure().hasMessage(format("line 1:7: Table 'iceberg.tpch.\"%s@%s\"' does not exist", tableName, snapshotId));
        assertThat(query("SELECT * FROM " + tableName))
                .matches("VALUES 11, 22");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testExpireSnapshotsSystemTable()
    {
        assertThat(query("ALTER TABLE \"nation$files\" EXECUTE EXPIRE_SNAPSHOTS"))
                .failure().hasMessage("This connector does not support table procedures");
        assertThat(query("ALTER TABLE \"nation$snapshots\" EXECUTE EXPIRE_SNAPSHOTS"))
                .failure().hasMessage("This connector does not support table procedures");
    }

    @Test
    public void testExplainExpireSnapshotOutput()
    {
        String tableName = "test_expiring_snapshots_output" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (key varchar, value integer) WITH (partitioning = ARRAY['key'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES ('one', 1)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('two', 2)", 1);

        assertExplain("EXPLAIN ALTER TABLE " + tableName + " EXECUTE EXPIRE_SNAPSHOTS (retention_threshold => '0s')",
                "SimpleTableExecute\\[table = iceberg:schemaTableName:tpch.test_expiring_snapshots.*\\[retentionThreshold=0\\.00s].*");
    }

    @Test
    public void testExpireSnapshotsParameterValidation()
    {
        assertQueryFails(
                "ALTER TABLE no_such_table_exists EXECUTE EXPIRE_SNAPSHOTS",
                "\\Qline 1:7: Table 'iceberg.tpch.no_such_table_exists' does not exist");
        assertQueryFails(
                "ALTER TABLE nation EXECUTE EXPIRE_SNAPSHOTS (retention_threshold => '33')",
                "\\Qline 1:46: Unable to set catalog 'iceberg' table procedure 'EXPIRE_SNAPSHOTS' property 'retention_threshold' to ['33']: duration is not a valid data duration string: 33");
        assertQueryFails(
                "ALTER TABLE nation EXECUTE EXPIRE_SNAPSHOTS (retention_threshold => '33mb')",
                "\\Qline 1:46: Unable to set catalog 'iceberg' table procedure 'EXPIRE_SNAPSHOTS' property 'retention_threshold' to ['33mb']: Unknown time unit: mb");
        assertQueryFails(
                "ALTER TABLE nation EXECUTE EXPIRE_SNAPSHOTS (retention_threshold => '33s')",
                "\\QRetention specified (33.00s) is shorter than the minimum retention configured in the system (7.00d). Minimum retention can be changed with iceberg.expire-snapshots.min-retention configuration property or iceberg.expire_snapshots_min_retention session property");
    }

    @Test
    public void testRemoveOrphanFiles()
            throws Exception
    {
        String tableName = "test_deleting_orphan_files_unnecessary_files" + randomNameSuffix();
        Session sessionWithShortRetentionUnlocked = prepareCleanUpSession();
        assertUpdate("CREATE TABLE " + tableName + " (key varchar, value integer)");
        assertUpdate("INSERT INTO " + tableName + " VALUES ('one', 1)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('two', 2), ('three', 3)", 2);
        assertUpdate("DELETE FROM " + tableName + " WHERE key = 'two'", 1);
        String location = getTableLocation(tableName);
        String orphanFile = getIcebergTableDataPath(location) + "/invalidData." + format;
        createFile(orphanFile);
        List<String> initialDataFiles = getAllDataFilesFromTableDirectory(tableName);
        assertThat(initialDataFiles).contains(orphanFile);

        assertQuerySucceeds(sessionWithShortRetentionUnlocked, "ALTER TABLE " + tableName + " EXECUTE REMOVE_ORPHAN_FILES (retention_threshold => '0s')");
        assertQuery("SELECT * FROM " + tableName, "VALUES ('one', 1), ('three', 3)");

        List<String> updatedDataFiles = getAllDataFilesFromTableDirectory(tableName);
        assertThat(updatedDataFiles.size()).isLessThan(initialDataFiles.size());
        assertThat(updatedDataFiles).doesNotContain(orphanFile);
    }

    @Test
    public void testIfRemoveOrphanFilesCleansUnnecessaryDataFilesInPartitionedTable()
            throws Exception
    {
        String tableName = "test_deleting_orphan_files_unnecessary_files" + randomNameSuffix();
        Session sessionWithShortRetentionUnlocked = prepareCleanUpSession();
        assertUpdate("CREATE TABLE " + tableName + " (key varchar, value integer) WITH (partitioning = ARRAY['key'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES ('one', 1)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('two', 2)", 1);
        String tableLocation = getTableLocation(tableName);
        String orphanFile = getIcebergTableDataPath(tableLocation) + "/key=one/invalidData." + format;
        createFile(orphanFile);
        List<String> initialDataFiles = getAllDataFilesFromTableDirectory(tableName);
        assertThat(initialDataFiles).contains(orphanFile);

        assertQuerySucceeds(sessionWithShortRetentionUnlocked, "ALTER TABLE " + tableName + " EXECUTE REMOVE_ORPHAN_FILES (retention_threshold => '0s')");

        List<String> updatedDataFiles = getAllDataFilesFromTableDirectory(tableName);
        assertThat(updatedDataFiles.size()).isLessThan(initialDataFiles.size());
        assertThat(updatedDataFiles).doesNotContain(orphanFile);
    }

    @Test
    public void testIfRemoveOrphanFilesCleansUnnecessaryMetadataFilesInPartitionedTable()
            throws Exception
    {
        String tableName = "test_deleting_orphan_files_unnecessary_files" + randomNameSuffix();
        Session sessionWithShortRetentionUnlocked = prepareCleanUpSession();
        assertUpdate("CREATE TABLE " + tableName + " (key varchar, value integer) WITH (partitioning = ARRAY['key'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES ('one', 1)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('two', 2)", 1);
        String tableLocation = getTableLocation(tableName);
        String orphanMetadataFile = getIcebergTableMetadataPath(tableLocation) + "/invalidData." + format;
        createFile(orphanMetadataFile);
        List<String> initialMetadataFiles = getAllMetadataFilesFromTableDirectory(tableLocation);
        assertThat(initialMetadataFiles).contains(orphanMetadataFile);

        assertQuerySucceeds(sessionWithShortRetentionUnlocked, "ALTER TABLE " + tableName + " EXECUTE REMOVE_ORPHAN_FILES (retention_threshold => '0s')");

        List<String> updatedMetadataFiles = getAllMetadataFilesFromTableDirectory(tableLocation);
        assertThat(updatedMetadataFiles.size()).isLessThan(initialMetadataFiles.size());
        assertThat(updatedMetadataFiles).doesNotContain(orphanMetadataFile);
    }

    @Test
    public void testCleaningUpWithTableWithSpecifiedLocation()
            throws IOException
    {
        testCleaningUpWithTableWithSpecifiedLocation("");
        testCleaningUpWithTableWithSpecifiedLocation("/");
        testCleaningUpWithTableWithSpecifiedLocation("//");
        testCleaningUpWithTableWithSpecifiedLocation("///");
    }

    private void testCleaningUpWithTableWithSpecifiedLocation(String suffix)
            throws IOException
    {
        String tableName = "test_table_cleaning_up_with_location" + randomNameSuffix();
        String tableLocation = getDistributedQueryRunner().getCoordinator().getBaseDataDir().toUri().toASCIIString() + randomNameSuffix() + suffix;
        String tableDirectory = new File(URI.create(tableLocation)).getPath(); // validates this is file:// URI and normalizes

        assertUpdate(format("CREATE TABLE %s (key varchar, value integer) WITH(location = '%s')", tableName, tableLocation));
        assertUpdate("INSERT INTO " + tableName + " VALUES ('one', 1)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('two', 2)", 1);

        List<String> initialMetadataFiles = getAllMetadataFilesFromTableDirectory(tableDirectory);
        List<Long> initialSnapshots = getSnapshotIds(tableName);
        assertThat(initialSnapshots).as("initialSnapshots")
                .hasSize(3); // CREATE TABLE creates a snapshot

        Session sessionWithShortRetentionUnlocked = prepareCleanUpSession();
        assertQuerySucceeds(sessionWithShortRetentionUnlocked, "ALTER TABLE " + tableName + " EXECUTE EXPIRE_SNAPSHOTS (retention_threshold => '0s')");
        assertQuerySucceeds(sessionWithShortRetentionUnlocked, "ALTER TABLE " + tableName + " EXECUTE REMOVE_ORPHAN_FILES (retention_threshold => '0s')");
        List<String> prunedMetadataFiles = getAllMetadataFilesFromTableDirectory(tableDirectory);
        List<Long> prunedSnapshots = getSnapshotIds(tableName);
        assertThat(prunedMetadataFiles).as("prunedMetadataFiles")
                .hasSize(initialMetadataFiles.size() - 2);
        assertThat(prunedSnapshots).as("prunedSnapshots")
                .hasSizeLessThan(initialSnapshots.size())
                .hasSize(1);
        assertThat(initialSnapshots).containsAll(prunedSnapshots);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testExplainRemoveOrphanFilesOutput()
    {
        String tableName = "test_remove_orphan_files_output" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (key varchar, value integer) WITH (partitioning = ARRAY['key'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES ('one', 1)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('two', 2)", 1);

        assertExplain("EXPLAIN ALTER TABLE " + tableName + " EXECUTE REMOVE_ORPHAN_FILES (retention_threshold => '0s')",
                "SimpleTableExecute\\[table = iceberg:schemaTableName:tpch.test_remove_orphan_files.*\\[retentionThreshold=0\\.00s].*");
    }

    @Test
    public void testRemoveOrphanFilesParameterValidation()
    {
        assertQueryFails(
                "ALTER TABLE no_such_table_exists EXECUTE REMOVE_ORPHAN_FILES",
                "\\Qline 1:7: Table 'iceberg.tpch.no_such_table_exists' does not exist");
        assertQueryFails(
                "ALTER TABLE nation EXECUTE REMOVE_ORPHAN_FILES (retention_threshold => '33')",
                "\\Qline 1:49: Unable to set catalog 'iceberg' table procedure 'REMOVE_ORPHAN_FILES' property 'retention_threshold' to ['33']: duration is not a valid data duration string: 33");
        assertQueryFails(
                "ALTER TABLE nation EXECUTE REMOVE_ORPHAN_FILES (retention_threshold => '33mb')",
                "\\Qline 1:49: Unable to set catalog 'iceberg' table procedure 'REMOVE_ORPHAN_FILES' property 'retention_threshold' to ['33mb']: Unknown time unit: mb");
        assertQueryFails(
                "ALTER TABLE nation EXECUTE REMOVE_ORPHAN_FILES (retention_threshold => '33s')",
                "\\QRetention specified (33.00s) is shorter than the minimum retention configured in the system (7.00d). Minimum retention can be changed with iceberg.remove-orphan-files.min-retention configuration property or iceberg.remove_orphan_files_min_retention session property");
    }

    @Test
    public void testRemoveOrphanFilesOnSnapshot()
    {
        String tableName = "test_remove_orphan_files_on_snapshot_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a) AS VALUES 11", 1);
        long snapshotId = getCurrentSnapshotId(tableName);
        assertUpdate("INSERT INTO " + tableName + " VALUES 22", 1);
        assertThat(query("ALTER TABLE \"%s@%d\" EXECUTE REMOVE_ORPHAN_FILES".formatted(tableName, snapshotId)))
                .failure().hasMessage(format("line 1:7: Table 'iceberg.tpch.\"%s@%s\"' does not exist", tableName, snapshotId));
        assertThat(query("SELECT * FROM " + tableName))
                .matches("VALUES 11, 22");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testRemoveOrphanFilesSystemTable()
    {
        assertThat(query("ALTER TABLE \"nation$files\" EXECUTE REMOVE_ORPHAN_FILES"))
                .failure().hasMessage("This connector does not support table procedures");
        assertThat(query("ALTER TABLE \"nation$snapshots\" EXECUTE REMOVE_ORPHAN_FILES"))
                .failure().hasMessage("This connector does not support table procedures");
    }

    @Test
    public void testIfDeletesReturnsNumberOfRemovedRows()
    {
        String tableName = "test_delete_returns_number_of_rows_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (key varchar, value integer) WITH (partitioning = ARRAY['key'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES ('one', 1)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('one', 2)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('one', 3)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('two', 1)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('two', 2)", 1);
        assertUpdate("DELETE FROM " + tableName + " WHERE key = 'one'", 3);
        assertUpdate("DELETE FROM " + tableName + " WHERE key = 'one'"); // TODO change this when iceberg will guarantee to always return this (https://github.com/apache/iceberg/issues/4647)
        assertUpdate("DELETE FROM " + tableName + " WHERE key = 'three'");
        assertUpdate("DELETE FROM " + tableName + " WHERE key = 'two'", 2);
    }

    @Test
    public void testUpdatingFileFormat()
    {
        String tableName = "test_updating_file_format_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " WITH (format = 'orc') AS SELECT * FROM nation WHERE nationkey < 10", "SELECT count(*) FROM nation WHERE nationkey < 10");
        assertQuery("SELECT value FROM \"" + tableName + "$properties\" WHERE key = 'write.format.default'", "VALUES 'ORC'");

        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES format = 'parquet'");
        assertQuery("SELECT value FROM \"" + tableName + "$properties\" WHERE key = 'write.format.default'", "VALUES 'PARQUET'");
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM nation WHERE nationkey >= 10", "SELECT count(*) FROM nation WHERE nationkey >= 10");

        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation");
        assertQuery("SELECT count(*) FROM \"" + tableName + "$files\" WHERE file_path LIKE '%.orc'", "VALUES 1");
        assertQuery("SELECT count(*) FROM \"" + tableName + "$files\" WHERE file_path LIKE '%.parquet'", "VALUES 1");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testUpdatingInvalidTableProperty()
    {
        String tableName = "test_updating_invalid_table_property_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a INT, b INT)");
        assertThat(query("ALTER TABLE " + tableName + " SET PROPERTIES not_a_valid_table_property = 'a value'"))
                .failure().hasMessage("line 1:76: Catalog 'iceberg' table property 'not_a_valid_table_property' does not exist");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testEmptyCreateTableAsSelect()
    {
        String tableName = "test_empty_ctas_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM nation WHERE false", 0);
        List<Long> initialTableSnapshots = getSnapshotIds(tableName);
        assertThat(initialTableSnapshots.size())
                .withFailMessage("CTAS operations must create Iceberg snapshot independently whether the selection is empty or not")
                .isEqualTo(1);
        assertQueryReturnsEmptyResult("SELECT * FROM " + tableName);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testEmptyInsert()
    {
        String tableName = "test_empty_insert_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM nation", "SELECT count(*) FROM nation");
        List<Long> initialTableSnapshots = getSnapshotIds(tableName);

        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM nation WHERE false", 0);
        List<Long> updatedTableSnapshots = getSnapshotIds(tableName);

        assertThat(initialTableSnapshots)
                .withFailMessage("INSERT operations that are not changing the state of the table must not cause the creation of a new Iceberg snapshot")
                .hasSize(1)
                .isEqualTo(updatedTableSnapshots);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testEmptyUpdate()
    {
        String tableName = "test_empty_update_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM nation", "SELECT count(*) FROM nation");
        List<Long> initialTableSnapshots = getSnapshotIds(tableName);

        assertUpdate("UPDATE " + tableName + " SET comment = 'new comment' WHERE nationkey IS NULL", 0);
        List<Long> updatedTableSnapshots = getSnapshotIds(tableName);

        assertThat(initialTableSnapshots)
                .withFailMessage("UPDATE operations that are not changing the state of the table must not cause the creation of a new Iceberg snapshot")
                .hasSize(1)
                .isEqualTo(updatedTableSnapshots);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testEmptyDelete()
    {
        String tableName = "test_empty_delete_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " WITH (format = '" + format.name() + "') AS SELECT * FROM nation", "SELECT count(*) FROM nation");
        List<Long> initialTableSnapshots = getSnapshotIds(tableName);

        assertUpdate("DELETE FROM " + tableName + " WHERE nationkey IS NULL", 0);
        List<Long> updatedTableSnapshots = getSnapshotIds(tableName);

        assertThat(initialTableSnapshots)
                .withFailMessage("DELETE operations that are not changing the state of the table must not cause the creation of a new Iceberg snapshot")
                .hasSize(1)
                .isEqualTo(updatedTableSnapshots);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testEmptyFilesTruncate()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_empty_files_truncate_", "AS SELECT 1 AS id")) {
            assertUpdate("TRUNCATE TABLE " + table.getName());
            assertQueryReturnsEmptyResult("SELECT * FROM \"" + table.getName() + "$files\"");
        }
    }

    @Test
    public void testModifyingOldSnapshotIsNotPossible()
    {
        String tableName = "test_modifying_old_snapshot_" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (col int)", tableName));
        assertUpdate(format("INSERT INTO %s VALUES 1,2,3", tableName), 3);
        long oldSnapshotId = getCurrentSnapshotId(tableName);
        assertUpdate(format("INSERT INTO %s VALUES 4,5,6", tableName), 3);
        assertQuery(format("SELECT * FROM %s FOR VERSION AS OF %d", tableName, oldSnapshotId), "VALUES 1,2,3");
        assertThat(query(format("INSERT INTO \"%s@%d\" VALUES 7,8,9", tableName, oldSnapshotId)))
                .failure().hasMessage(format("line 1:1: Table 'iceberg.tpch.\"%s@%s\"' does not exist", tableName, oldSnapshotId));
        assertThat(query(format("DELETE FROM \"%s@%d\" WHERE col = 5", tableName, oldSnapshotId)))
                .failure().hasMessage(format("line 1:1: Table 'iceberg.tpch.\"%s@%s\"' does not exist", tableName, oldSnapshotId));
        assertThat(query(format("UPDATE \"%s@%d\" SET col = 50 WHERE col = 5", tableName, oldSnapshotId)))
                .failure().hasMessage(format("line 1:1: Table 'iceberg.tpch.\"%s@%s\"' does not exist", tableName, oldSnapshotId));
        assertThat(query(format("INSERT INTO \"%s@%d\" VALUES 7,8,9", tableName, getCurrentSnapshotId(tableName))))
                .failure().hasMessage(format("line 1:1: Table 'iceberg.tpch.\"%s@%s\"' does not exist", tableName, getCurrentSnapshotId(tableName)));
        assertThat(query(format("DELETE FROM \"%s@%d\" WHERE col = 9", tableName, getCurrentSnapshotId(tableName))))
                .failure().hasMessage(format("line 1:1: Table 'iceberg.tpch.\"%s@%s\"' does not exist", tableName, getCurrentSnapshotId(tableName)));
        assertThatThrownBy(() -> assertUpdate(format("UPDATE \"%s@%d\" set col = 50 WHERE col = 5", tableName, getCurrentSnapshotId(tableName))))
                .hasMessage(format("line 1:1: Table 'iceberg.tpch.\"%s@%s\"' does not exist", tableName, getCurrentSnapshotId(tableName)));
        assertThat(query(format("ALTER TABLE \"%s@%d\" EXECUTE OPTIMIZE", tableName, oldSnapshotId)))
                .failure().hasMessage(format("line 1:7: Table 'iceberg.tpch.\"%s@%s\"' does not exist", tableName, oldSnapshotId));
        assertQuery(format("SELECT * FROM %s", tableName), "VALUES 1,2,3,4,5,6");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateTableAsSelectFromVersionedTable()
            throws Exception
    {
        String sourceTableName = "test_ctas_versioned_source_" + randomNameSuffix();
        String snapshotVersionedSinkTableName = "test_ctas_snapshot_versioned_sink_" + randomNameSuffix();
        String timestampVersionedSinkTableName = "test_ctas_timestamp_versioned_sink_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + sourceTableName + "(an_integer integer)");
        // Enforce having exactly one snapshot of the table at the timestamp corresponding to `afterInsert123EpochMillis`
        Thread.sleep(1);
        assertUpdate("INSERT INTO " + sourceTableName + " VALUES 1, 2, 3", 3);
        long afterInsert123SnapshotId = getCurrentSnapshotId(sourceTableName);
        long afterInsert123EpochMillis = getCommittedAtInEpochMilliseconds(sourceTableName, afterInsert123SnapshotId);
        Thread.sleep(1);
        assertUpdate("INSERT INTO " + sourceTableName + " VALUES 4, 5, 6", 3);
        long afterInsert456SnapshotId = getCurrentSnapshotId(sourceTableName);
        assertUpdate("INSERT INTO " + sourceTableName + " VALUES 7, 8, 9", 3);

        assertUpdate("CREATE TABLE " + snapshotVersionedSinkTableName + " AS SELECT * FROM " + sourceTableName + " FOR VERSION AS OF " + afterInsert456SnapshotId, 6);
        assertUpdate("CREATE TABLE " + timestampVersionedSinkTableName + " AS SELECT * FROM " + sourceTableName + " FOR TIMESTAMP AS OF " + timestampLiteral(afterInsert123EpochMillis, 9), 3);

        assertQuery("SELECT * FROM " + sourceTableName, "VALUES 1, 2, 3, 4, 5, 6, 7, 8, 9");
        assertQuery("SELECT * FROM " + snapshotVersionedSinkTableName, "VALUES 1, 2, 3, 4, 5, 6");
        assertQuery("SELECT * FROM " + timestampVersionedSinkTableName, "VALUES 1, 2, 3");

        assertUpdate("DROP TABLE " + sourceTableName);
        assertUpdate("DROP TABLE " + snapshotVersionedSinkTableName);
        assertUpdate("DROP TABLE " + timestampVersionedSinkTableName);
    }

    @Test
    public void testSubqueryContainVersionedTable()
    {
        String tableName = "test_subquery_versioned" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 id", 1);
        long snapshotId = getCurrentSnapshotId(tableName);
        String timestamp = timestampLiteral(getCommittedAtInEpochMilliseconds(tableName, snapshotId), 9);
        assertUpdate("INSERT INTO " + tableName + " VALUES 2", 1);

        assertQuery("SELECT * FROM " + tableName + " WHERE id = (SELECT id FROM " + tableName + " FOR VERSION AS OF " + snapshotId + ")", "VALUES 1");
        assertQuery("SELECT * FROM " + tableName + " WHERE id = (SELECT id FROM " + tableName + " FOR TIMESTAMP AS OF " + timestamp + ")", "VALUES 1");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testReadingFromSpecificSnapshot()
    {
        String tableName = "test_reading_snapshot" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (a bigint, b bigint)", tableName));
        assertUpdate(format("INSERT INTO %s VALUES(1, 1)", tableName), 1);
        List<Long> ids = getSnapshotsIdsByCreationOrder(tableName);

        assertQuery(format("SELECT count(*) FROM %s FOR VERSION AS OF %d", tableName, ids.get(0)), "VALUES(0)");
        assertQuery(format("SELECT * FROM %s FOR VERSION AS OF %d", tableName, ids.get(1)), "VALUES(1,1)");
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testSelectWithMoreThanOneSnapshotOfTheSameTable()
    {
        String tableName = "test_reading_snapshot" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (a bigint, b bigint)", tableName));
        assertUpdate(format("INSERT INTO %s VALUES(1, 1)", tableName), 1);
        assertUpdate(format("INSERT INTO %s VALUES(2, 2)", tableName), 1);
        assertUpdate(format("INSERT INTO %s VALUES(3, 3)", tableName), 1);
        List<Long> ids = getSnapshotsIdsByCreationOrder(tableName);

        assertQuery(format("SELECT * FROM %s", tableName), "SELECT * FROM (VALUES(1,1), (2,2), (3,3))");
        assertQuery(
                format("SELECT * FROM %1$s EXCEPT (SELECT * FROM %1$s FOR VERSION AS OF %2$d EXCEPT SELECT * FROM %1$s FOR VERSION AS OF %3$d)", tableName, ids.get(2), ids.get(1)),
                "SELECT * FROM (VALUES(1,1), (3,3))");
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testInsertingIntoTablesWithColumnsWithQuotesInName()
    {
        String tableName = "test_inserting_into_tables_with_quotes_" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (\"an identifier with \"\"quotes\"\" \" INTEGER, x row (\"another identifier\" INTEGER))", tableName));
        assertUpdate(format("INSERT INTO %s VALUES (1, row(11))", tableName), 1);
        assertThat(query(format("SELECT * FROM %s", tableName)))
                .matches("VALUES (INTEGER '1', CAST(ROW(11) AS ROW(\"another identifier\"INTEGER)))");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testInsertIntoBucketedColumnTaskWriterCount()
    {
        int taskWriterCount = 4;
        assertThat(taskWriterCount).isGreaterThan(getQueryRunner().getNodeCount());
        Session session = Session.builder(getSession())
                .setSystemProperty(TASK_MIN_WRITER_COUNT, String.valueOf(taskWriterCount))
                .setSystemProperty(TASK_MAX_WRITER_COUNT, String.valueOf(taskWriterCount))
                .build();

        String tableName = "test_inserting_into_bucketed_column_task_writer_count_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (x INT) WITH (partitioning = ARRAY['bucket(x, 7)'])");

        assertUpdate(session, "INSERT INTO " + tableName + " SELECT nationkey FROM nation", 25);
        assertQuery("SELECT * FROM " + tableName, "SELECT nationkey FROM nation");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testReadFromVersionedTableWithSchemaEvolution()
    {
        String tableName = "test_versioned_table_schema_evolution_" + randomNameSuffix();

        assertQuerySucceeds("CREATE TABLE " + tableName + "(col1 varchar)");
        long v1SnapshotId = getCurrentSnapshotId(tableName);
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF " + v1SnapshotId))
                .result()
                .hasTypes(ImmutableList.of(VARCHAR))
                .isEmpty();

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN  col2 integer");
        assertThat(query("SELECT * FROM " + tableName))
                .result()
                .hasTypes(ImmutableList.of(VARCHAR, INTEGER))
                .isEmpty();

        assertUpdate("INSERT INTO " + tableName + " VALUES ('a', 11)", 1);
        long v2SnapshotId = getCurrentSnapshotId(tableName);
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF " + v2SnapshotId))
                .result()
                .hasTypes(ImmutableList.of(VARCHAR, INTEGER))
                .matches("VALUES (VARCHAR 'a', 11)");
        assertThat(query("SELECT * FROM " + tableName))
                .result()
                .hasTypes(ImmutableList.of(VARCHAR, INTEGER))
                .matches("VALUES (VARCHAR 'a', 11)");

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN  col3 bigint");
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF " + v2SnapshotId))
                .result()
                .hasTypes(ImmutableList.of(VARCHAR, INTEGER))
                .matches("VALUES (VARCHAR 'a', 11)");
        assertThat(query("SELECT * FROM " + tableName))
                .result()
                .hasTypes(ImmutableList.of(VARCHAR, INTEGER, BIGINT))
                .matches("VALUES (VARCHAR 'a', 11, CAST(NULL AS bigint))");

        assertUpdate("INSERT INTO " + tableName + " VALUES ('b', 22, 32)", 1);
        long v3SnapshotId = getCurrentSnapshotId(tableName);
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF " + v1SnapshotId))
                .result()
                .hasTypes(ImmutableList.of(VARCHAR))
                .isEmpty();
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF " + v2SnapshotId))
                .result()
                .hasTypes(ImmutableList.of(VARCHAR, INTEGER))
                .matches("VALUES (VARCHAR 'a', 11)");
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF " + v3SnapshotId))
                .result()
                .hasTypes(ImmutableList.of(VARCHAR, INTEGER, BIGINT))
                .matches("VALUES (VARCHAR 'a', 11, NULL), (VARCHAR 'b', 22, BIGINT '32')");
        assertThat(query("SELECT * FROM " + tableName))
                .result()
                .hasTypes(ImmutableList.of(VARCHAR, INTEGER, BIGINT))
                .matches("VALUES (VARCHAR 'a', 11, NULL), (VARCHAR 'b', 22, BIGINT '32')");
    }

    @Test
    public void testReadFromVersionedTableWithSchemaEvolutionDropColumn()
    {
        String tableName = "test_versioned_table_schema_evolution_drop_column_" + randomNameSuffix();

        assertQuerySucceeds("CREATE TABLE " + tableName + "(col1 varchar, col2 integer, col3 boolean)");
        long v1SnapshotId = getCurrentSnapshotId(tableName);
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF " + v1SnapshotId))
                .result()
                .hasTypes(ImmutableList.of(VARCHAR, INTEGER, BOOLEAN))
                .isEmpty();

        assertUpdate("INSERT INTO " + tableName + " VALUES ('a', 1, true)", 1);
        long v2SnapshotId = getCurrentSnapshotId(tableName);
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF " + v2SnapshotId))
                .result()
                .hasTypes(ImmutableList.of(VARCHAR, INTEGER, BOOLEAN))
                .matches("VALUES (VARCHAR 'a', 1, true)");

        assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN  col3");
        assertUpdate("INSERT INTO " + tableName + " VALUES ('b', 2)", 1);
        long v3SnapshotId = getCurrentSnapshotId(tableName);
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF " + v3SnapshotId))
                .result()
                .hasTypes(ImmutableList.of(VARCHAR, INTEGER))
                .matches("VALUES (VARCHAR 'a', 1), (VARCHAR 'b', 2)");
        assertThat(query("SELECT * FROM " + tableName))
                .result()
                .hasTypes(ImmutableList.of(VARCHAR, INTEGER))
                .matches("VALUES (VARCHAR 'a', 1), (VARCHAR 'b', 2)");
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF " + v2SnapshotId))
                .result()
                .hasTypes(ImmutableList.of(VARCHAR, INTEGER, BOOLEAN))
                .matches("VALUES (VARCHAR 'a', 1, true)");

        assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN  col2");
        assertUpdate("INSERT INTO " + tableName + " VALUES ('c')", 1);
        long v4SnapshotId = getCurrentSnapshotId(tableName);
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF " + v4SnapshotId))
                .result()
                .hasTypes(ImmutableList.of(VARCHAR))
                .matches("VALUES (VARCHAR 'a'), (VARCHAR 'b'), (VARCHAR 'c')");
        assertThat(query("SELECT * FROM " + tableName))
                .result()
                .hasTypes(ImmutableList.of(VARCHAR))
                .matches("VALUES (VARCHAR 'a'), (VARCHAR 'b'), (VARCHAR 'c')");
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF " + v3SnapshotId))
                .result()
                .hasTypes(ImmutableList.of(VARCHAR, INTEGER))
                .matches("VALUES (VARCHAR 'a', 1), (VARCHAR 'b', 2)");
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF " + v2SnapshotId))
                .result()
                .hasTypes(ImmutableList.of(VARCHAR, INTEGER, BOOLEAN))
                .matches("VALUES (VARCHAR 'a', 1, true)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testReadFromVersionedTableWithPartitionSpecEvolution()
            throws Exception
    {
        String tableName = "test_version_table_with_partition_spec_evolution_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (day varchar, views bigint) WITH(partitioning = ARRAY['day'])");
        long v1SnapshotId = getCurrentSnapshotId(tableName);
        long v1EpochMillis = getCommittedAtInEpochMilliseconds(tableName, v1SnapshotId);
        Thread.sleep(1);

        assertUpdate("INSERT INTO " + tableName + " (day, views) VALUES ('2022-06-01', 1)", 1);
        long v2SnapshotId = getCurrentSnapshotId(tableName);
        long v2EpochMillis = getCommittedAtInEpochMilliseconds(tableName, v2SnapshotId);
        Thread.sleep(1);

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN hour varchar");
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES partitioning = ARRAY['day', 'hour']");
        assertUpdate("INSERT INTO " + tableName + " (day, hour, views) VALUES ('2022-06-02', '10', 2), ('2022-06-02', '10', 3), ('2022-06-02', '11', 10)", 3);
        long v3SnapshotId = getCurrentSnapshotId(tableName);
        long v3EpochMillis = getCommittedAtInEpochMilliseconds(tableName, v3SnapshotId);

        assertThat(query("SELECT sum(views), day  FROM " + tableName + " GROUP BY day"))
                .matches("VALUES ROW(BIGINT '1', VARCHAR '2022-06-01'), ROW(BIGINT '15', VARCHAR '2022-06-02')");
        assertThat(query("SELECT sum(views), day  FROM " + tableName + " FOR VERSION AS OF " + v1SnapshotId + " GROUP BY day"))
                .returnsEmptyResult();
        assertThat(query("SELECT sum(views), day  FROM " + tableName + " FOR TIMESTAMP AS OF " + timestampLiteral(v1EpochMillis, 9) + " GROUP BY day"))
                .returnsEmptyResult();
        assertThat(query("SELECT sum(views), day  FROM " + tableName + " FOR VERSION AS OF " + v2SnapshotId + " GROUP BY day"))
                .matches("VALUES ROW(BIGINT '1', VARCHAR '2022-06-01')");
        assertThat(query("SELECT sum(views), day  FROM " + tableName + " FOR TIMESTAMP AS OF " + timestampLiteral(v2EpochMillis, 9) + " GROUP BY day"))
                .matches("VALUES ROW(BIGINT '1', VARCHAR '2022-06-01')");
        assertThat(query("SELECT sum(views), day  FROM " + tableName + " FOR VERSION AS OF " + v3SnapshotId + " GROUP BY day"))
                .matches("VALUES ROW(BIGINT '1', VARCHAR '2022-06-01'), ROW(BIGINT '15', VARCHAR '2022-06-02')");
        assertThat(query("SELECT sum(views), day  FROM " + tableName + " FOR TIMESTAMP AS OF " + timestampLiteral(v3EpochMillis, 9) + " GROUP BY day"))
                .matches("VALUES ROW(BIGINT '1', VARCHAR '2022-06-01'), ROW(BIGINT '15', VARCHAR '2022-06-02')");

        assertThat(query("SELECT sum(views), day, hour  FROM " + tableName + " FOR VERSION AS OF " + v3SnapshotId + " WHERE day = '2022-06-02' GROUP BY day, hour"))
                .matches("VALUES ROW(BIGINT '5', VARCHAR '2022-06-02', VARCHAR '10'), ROW(BIGINT '10', VARCHAR '2022-06-02', VARCHAR '11')");
        assertThat(query("SELECT sum(views), day, hour  FROM " + tableName + " FOR TIMESTAMP AS OF " + timestampLiteral(v3EpochMillis, 9) + " WHERE day = '2022-06-02' GROUP BY day, hour"))
                .matches("VALUES ROW(BIGINT '5', VARCHAR '2022-06-02', VARCHAR '10'), ROW(BIGINT '10', VARCHAR '2022-06-02', VARCHAR '11')");
    }

    @Test
    public void testReadFromVersionedTableWithExpiredHistory()
            throws Exception
    {
        String tableName = "test_version_table_with_expired_snapshots_" + randomNameSuffix();
        Session sessionWithShortRetentionUnlocked = prepareCleanUpSession();
        assertUpdate("CREATE TABLE " + tableName + " (key varchar, value integer)");
        long v1SnapshotId = getCurrentSnapshotId(tableName);
        long v1EpochMillis = getCommittedAtInEpochMilliseconds(tableName, v1SnapshotId);
        Thread.sleep(1);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('one', 1)", 1);
        long v2SnapshotId = getCurrentSnapshotId(tableName);
        long v2EpochMillis = getCommittedAtInEpochMilliseconds(tableName, v2SnapshotId);
        Thread.sleep(1);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('two', 2)", 1);
        long v3SnapshotId = getCurrentSnapshotId(tableName);
        long v3EpochMillis = getCommittedAtInEpochMilliseconds(tableName, v3SnapshotId);
        assertThat(query("SELECT sum(value), listagg(key, ' ') WITHIN GROUP (ORDER BY key) FROM " + tableName))
                .matches("VALUES (BIGINT '3', VARCHAR 'one two')");
        List<Long> initialSnapshots = getSnapshotIds(tableName);
        assertQuerySucceeds(sessionWithShortRetentionUnlocked, "ALTER TABLE " + tableName + " EXECUTE EXPIRE_SNAPSHOTS (retention_threshold => '0s')");
        List<Long> updatedSnapshots = getSnapshotIds(tableName);
        assertThat(updatedSnapshots.size()).isLessThan(initialSnapshots.size());
        assertThat(updatedSnapshots).hasSize(1);

        assertThat(query("SELECT sum(value), listagg(key, ' ') WITHIN GROUP (ORDER BY key) FROM " + tableName + " FOR VERSION AS OF " + v3SnapshotId))
                .matches("VALUES (BIGINT '3', VARCHAR 'one two')");
        assertThat(query("SELECT sum(value), listagg(key, ' ') WITHIN GROUP (ORDER BY key) FROM " + tableName + " FOR TIMESTAMP AS OF " + timestampLiteral(v3EpochMillis, 9)))
                .matches("VALUES (BIGINT '3', VARCHAR 'one two')");

        assertQueryFails("SELECT * FROM " + tableName + " FOR VERSION AS OF " + v2SnapshotId, "Iceberg snapshot ID does not exists\\: " + v2SnapshotId);
        assertQueryFails("SELECT * FROM " + tableName + " FOR TIMESTAMP AS OF " + timestampLiteral(v2EpochMillis, 9), "No version history table .* at or before .*");
        assertQueryFails("SELECT * FROM " + tableName + " FOR VERSION AS OF " + v1SnapshotId, "Iceberg snapshot ID does not exists\\: " + v1SnapshotId);
        assertQueryFails("SELECT * FROM " + tableName + " FOR TIMESTAMP AS OF " + timestampLiteral(v1EpochMillis, 9), "No version history table .* at or before .*");
    }

    @Test
    public void testDeleteRetainsTableHistory()
    {
        String tableName = "test_delete_retains_table_history_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(c1 INT, c2 INT)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 1), (2, 2), (3, 3)", 3);
        assertUpdate("INSERT INTO " + tableName + " VALUES (3, 3), (4, 4), (5, 5)", 3);
        List<Long> snapshots = getTableHistory(tableName);

        assertUpdate("DELETE FROM " + tableName + " WHERE c1 < 4", 4);
        List<Long> snapshotsAfterDelete = getTableHistory(tableName);
        assertThat(snapshotsAfterDelete.size()).isGreaterThan(snapshots.size());
        assertThat(snapshotsAfterDelete).containsAll(snapshots);
    }

    @Test
    public void testDeleteRetainsMetadataFile()
    {
        String tableName = "test_delete_retains_metadata_file_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(c1 INT, c2 INT)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 1), (2, 2), (3, 3)", 3);
        assertUpdate("INSERT INTO " + tableName + " VALUES (3, 3), (4, 4), (5, 5)", 3);
        List<Long> metadataLogEntries = getLatestSequenceNumbersInMetadataLogEntries(tableName);

        assertUpdate("DELETE FROM " + tableName + " WHERE c1 < 4", 4);
        List<Long> metadataLogEntriesAfterDelete = getLatestSequenceNumbersInMetadataLogEntries(tableName);
        assertThat(metadataLogEntriesAfterDelete)
                .hasSizeGreaterThan(metadataLogEntries.size())
                .containsAll(metadataLogEntries);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateOrReplaceTableSnapshots()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_create_or_replace_", " AS SELECT BIGINT '42' a, DOUBLE '-38.5' b")) {
            long v1SnapshotId = getCurrentSnapshotId(table.getName());

            assertUpdate("CREATE OR REPLACE TABLE " + table.getName() + " AS SELECT BIGINT '-42' a, DOUBLE '38.5' b", 1);
            assertThat(query("SELECT CAST(a AS bigint), b FROM " + table.getName()))
                    .matches("VALUES (BIGINT '-42', 385e-1)");

            assertThat(query("SELECT a, b  FROM " + table.getName() + " FOR VERSION AS OF " + v1SnapshotId))
                    .matches("VALUES (BIGINT '42', -385e-1)");
        }
    }

    @Test
    public void testCreateOrReplaceTableChangeColumnNamesAndTypes()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_create_or_replace_", " AS SELECT BIGINT '42' a, DOUBLE '-38.5' b")) {
            long v1SnapshotId = getCurrentSnapshotId(table.getName());

            assertUpdate("CREATE OR REPLACE TABLE " + table.getName() + " AS SELECT CAST(ARRAY[ROW('test')] AS ARRAY(ROW(field VARCHAR))) a, VARCHAR 'test2' b", 1);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (CAST(ARRAY[ROW('test')] AS ARRAY(ROW(field VARCHAR))), VARCHAR 'test2')");

            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF " + v1SnapshotId))
                    .matches("VALUES (BIGINT '42', -385e-1)");
        }
    }

    @Test
    public void testCreateOrReplaceTableChangePartitionedTableIntoUnpartitioned()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_create_or_replace_", " WITH (partitioning=ARRAY['a']) AS SELECT BIGINT '42' a, 'some data' b UNION ALL SELECT BIGINT '43' a, 'another data' b")) {
            long v1SnapshotId = getCurrentSnapshotId(table.getName());

            assertUpdate("CREATE OR REPLACE TABLE " + table.getName() + " WITH (sorted_by=ARRAY['a']) AS SELECT BIGINT '22' a, 'new data' b", 1);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (BIGINT '22', CAST('new data' AS VARCHAR))");

            assertThat(query("SELECT partition FROM \"" + table.getName() + "$partitions\""))
                    .matches("VALUES (ROW(CAST (ROW(NULL) AS ROW(a BIGINT))))");

            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF " + v1SnapshotId))
                    .matches("VALUES (BIGINT '42', CAST('some data' AS VARCHAR)), (BIGINT '43', CAST('another data' AS VARCHAR))");

            assertThat((String) computeScalar("SHOW CREATE TABLE " + table.getName()))
                    .contains("sorted_by = ARRAY['a ASC NULLS FIRST']");
            assertThat((String) computeScalar("SHOW CREATE TABLE " + table.getName()))
                    .doesNotContain("partitioning = ARRAY['a']");
        }
    }

    @Test
    public void testCreateOrReplaceTableChangeUnpartitionedTableIntoPartitioned()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_create_or_replace_", " WITH (sorted_by=ARRAY['a']) AS SELECT BIGINT '22' a, CAST('some data' AS VARCHAR) b")) {
            long v1SnapshotId = getCurrentSnapshotId(table.getName());

            assertUpdate("CREATE OR REPLACE TABLE " + table.getName() + " WITH (partitioning=ARRAY['a']) AS SELECT BIGINT '42' a, 'some data' b UNION ALL SELECT BIGINT '43' a, 'another data' b", 2);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (BIGINT '42', CAST('some data' AS VARCHAR)), (BIGINT '43', CAST('another data' AS VARCHAR))");

            assertThat(query("SELECT partition FROM \"" + table.getName() + "$partitions\""))
                    .matches("VALUES (ROW(CAST (ROW(BIGINT '42') AS ROW(a BIGINT)))), (ROW(CAST (ROW(BIGINT '43') AS ROW(a BIGINT))))");

            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF " + v1SnapshotId))
                    .matches("VALUES (BIGINT '22', CAST('some data' AS VARCHAR))");

            assertThat((String) computeScalar("SHOW CREATE TABLE " + table.getName()))
                    .contains("partitioning = ARRAY['a']");
            assertThat((String) computeScalar("SHOW CREATE TABLE " + table.getName()))
                    .doesNotContain("sorted_by = ARRAY['a ASC NULLS FIRST']");
        }
    }

    @Test
    public void testCreateOrReplaceTableWithComments()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_create_or_replace_", " (a BIGINT COMMENT 'This is a column') COMMENT 'This is a table'")) {
            long v1SnapshotId = getCurrentSnapshotId(table.getName());

            assertUpdate("CREATE OR REPLACE TABLE " + table.getName() + " AS SELECT 1 a", 1);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES 1");

            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF " + v1SnapshotId))
                    .returnsEmptyResult();

            assertThat(getTableComment(getSession().getCatalog().orElseThrow(), getSession().getSchema().orElseThrow(), table.getName()))
                    .isNull();
            assertThat(getColumnComment(table.getName(), "a"))
                    .isNull();

            assertUpdate("CREATE OR REPLACE TABLE " + table.getName() + " (a BIGINT COMMENT 'This is a column') COMMENT 'This is a table'");

            assertThat(getTableComment(getSession().getCatalog().orElseThrow(), getSession().getSchema().orElseThrow(), table.getName()))
                    .isEqualTo("This is a table");
            assertThat(getColumnComment(table.getName(), "a"))
                    .isEqualTo("This is a column");
        }
    }

    @Test
    public void testCreateOrReplaceTableWithSameLocation()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_create_or_replace_with_same_location_",
                "(a integer)")) {
            String initialTableLocation = getTableLocation(table.getName());
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES 1");
            long v1SnapshotId = getCurrentSnapshotId(table.getName());
            assertUpdate("CREATE OR REPLACE TABLE " + table.getName() + " (a integer)");
            assertThat(getTableLocation(table.getName()))
                    .isEqualTo(initialTableLocation);
            assertUpdate("CREATE OR REPLACE TABLE " + table.getName() + " (a integer) WITH (location = '" + initialTableLocation + "')");
            String initialTableLocationWithTrailingSlash = initialTableLocation.endsWith("/") ? initialTableLocation : initialTableLocation + "/";
            assertUpdate("CREATE OR REPLACE TABLE " + table.getName() + " (a integer) WITH (location = '" + initialTableLocationWithTrailingSlash + "')");
            assertThat(getTableLocation(table.getName()))
                    .isEqualTo(initialTableLocation);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .returnsEmptyResult();
            assertUpdate("CREATE OR REPLACE TABLE " + table.getName() + " WITH (location = '" + initialTableLocation + "') AS SELECT 2 as a", 1);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES 2");
            assertThat(getTableLocation(table.getName()))
                    .isEqualTo(initialTableLocation);
            assertThat(query("SELECT *  FROM " + table.getName() + " FOR VERSION AS OF " + v1SnapshotId))
                    .matches("VALUES 1");
        }
    }

    @Test
    public void testCreateOrReplaceTableWithChangeInLocation()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_create_or_replace_change_location_", "(a integer) ")) {
            String initialTableLocation = getTableLocation(table.getName()) + randomNameSuffix();
            long v1SnapshotId = getCurrentSnapshotId(table.getName());
            assertQueryFails(
                    "CREATE OR REPLACE TABLE " + table.getName() + " (a integer) WITH (location = '%s')".formatted(initialTableLocation),
                    "The provided location '%s' does not match the existing table location '.*'".formatted(initialTableLocation));
            assertQueryFails(
                    "CREATE OR REPLACE TABLE " + table.getName() + " WITH (location = '%s') AS SELECT 1 AS a".formatted(initialTableLocation),
                    "The provided location '%s' does not match the existing table location '.*'".formatted(initialTableLocation));
            assertThat(getCurrentSnapshotId(table.getName()))
                    .isEqualTo(v1SnapshotId);
        }
    }

    @Test
    public void testMergeSimpleSelectPartitioned()
    {
        String targetTable = "merge_simple_target_" + randomNameSuffix();
        String sourceTable = "merge_simple_source_" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (partitioning = ARRAY['address'])", targetTable));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);

        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", sourceTable));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Ed', 7, 'Etherville'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire')", sourceTable), 4);

        String sql = format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)";

        assertUpdate(sql, 4);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Aaron', 11, 'Arches'), ('Ed', 7, 'Etherville'), ('Bill', 7, 'Buena'), ('Dave', 22, 'Darbyshire')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeUpdateWithVariousLayouts()
    {
        testMergeUpdateWithVariousLayouts(1, "");
        testMergeUpdateWithVariousLayouts(4, "");
        testMergeUpdateWithVariousLayouts(1, "WITH (partitioning = ARRAY['customer'])");
        testMergeUpdateWithVariousLayouts(4, "WITH (partitioning = ARRAY['customer'])");
        testMergeUpdateWithVariousLayouts(1, "WITH (partitioning = ARRAY['purchase'])");
        testMergeUpdateWithVariousLayouts(4, "WITH (partitioning = ARRAY['purchase'])");
        testMergeUpdateWithVariousLayouts(1, "WITH (partitioning = ARRAY['bucket(customer, 3)'])");
        testMergeUpdateWithVariousLayouts(4, "WITH (partitioning = ARRAY['bucket(customer, 3)'])");
        testMergeUpdateWithVariousLayouts(1, "WITH (partitioning = ARRAY['bucket(purchase, 4)'])");
        testMergeUpdateWithVariousLayouts(4, "WITH (partitioning = ARRAY['bucket(purchase, 4)'])");
    }

    private void testMergeUpdateWithVariousLayouts(int writers, String partitioning)
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(TASK_MIN_WRITER_COUNT, String.valueOf(writers))
                .build();

        String targetTable = "merge_formats_target_" + randomNameSuffix();
        String sourceTable = "merge_formats_source_" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchase VARCHAR) %s", targetTable, partitioning));

        assertUpdate(format("INSERT INTO %s (customer, purchase) VALUES ('Dave', 'dates'), ('Lou', 'limes'), ('Carol', 'candles')", targetTable), 3);
        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Dave', 'dates'), ('Lou', 'limes'), ('Carol', 'candles')");

        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchase VARCHAR)", sourceTable));

        assertUpdate(format("INSERT INTO %s (customer, purchase) VALUES ('Craig', 'candles'), ('Len', 'limes'), ('Joe', 'jellybeans')", sourceTable), 3);

        String sql = format("MERGE INTO %s t USING %s s ON (t.purchase = s.purchase)", targetTable, sourceTable) +
                "    WHEN MATCHED AND s.purchase = 'limes' THEN DELETE" +
                "    WHEN MATCHED THEN UPDATE SET customer = CONCAT(t.customer, '_', s.customer)" +
                "    WHEN NOT MATCHED THEN INSERT (customer, purchase) VALUES(s.customer, s.purchase)";

        assertUpdate(session, sql, 3);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Dave', 'dates'), ('Carol_Craig', 'candles'), ('Joe', 'jellybeans')");
        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    @Override
    public void testMergeMultipleOperations()
    {
        testMergeMultipleOperations(1, "", false);
        testMergeMultipleOperations(4, "", false);
        testMergeMultipleOperations(1, "WITH (partitioning = ARRAY['customer'])", false);
        testMergeMultipleOperations(4, "WITH (partitioning = ARRAY['customer'])", false);
        testMergeMultipleOperations(1, "WITH (partitioning = ARRAY['purchase'])", false);
        testMergeMultipleOperations(4, "WITH (partitioning = ARRAY['purchase'])", false);
        testMergeMultipleOperations(1, "WITH (partitioning = ARRAY['bucket(customer, 3)'])", false);
        testMergeMultipleOperations(4, "WITH (partitioning = ARRAY['bucket(customer, 3)'])", false);
        testMergeMultipleOperations(1, "WITH (partitioning = ARRAY['bucket(purchase, 4)'])", false);
        testMergeMultipleOperations(4, "WITH (partitioning = ARRAY['bucket(purchase, 4)'])", false);
        testMergeMultipleOperations(1, "", true);
        testMergeMultipleOperations(4, "WITH (partitioning = ARRAY['customer'])", true);
        testMergeMultipleOperations(1, "WITH (partitioning = ARRAY['bucket(customer, 3)'])", true);
        testMergeMultipleOperations(4, "WITH (partitioning = ARRAY['bucket(purchase, 4)'])", true);
    }

    private void testMergeMultipleOperations(int writers, String partitioning, boolean determinePartitionCountForWrite)
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(TASK_MIN_WRITER_COUNT, String.valueOf(writers))
                .setSystemProperty(TASK_MAX_WRITER_COUNT, String.valueOf(writers))
                .setSystemProperty(DETERMINE_PARTITION_COUNT_FOR_WRITE_ENABLED, Boolean.toString(determinePartitionCountForWrite))
                .build();

        int targetCustomerCount = 32;
        String targetTable = "merge_multiple_" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (purchase INT, zipcode INT, spouse VARCHAR, address VARCHAR, customer VARCHAR) %s", targetTable, partitioning));
        String originalInsertFirstHalf = range(1, targetCustomerCount / 2)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 1000, 91000, intValue, intValue))
                .collect(joining(", "));
        String originalInsertSecondHalf = range(targetCustomerCount / 2, targetCustomerCount)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 2000, 92000, intValue, intValue))
                .collect(joining(", "));

        assertUpdate(format("INSERT INTO %s (customer, purchase, zipcode, spouse, address) VALUES %s, %s", targetTable, originalInsertFirstHalf, originalInsertSecondHalf), targetCustomerCount - 1);

        String firstMergeSource = range(targetCustomerCount / 2, targetCustomerCount)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jill_%s', '%s Eop Ct')", intValue, 3000, 83000, intValue, intValue))
                .collect(joining(", "));

        assertUpdate(session,
                format("MERGE INTO %s t USING (VALUES %s) AS s(customer, purchase, zipcode, spouse, address)", targetTable, firstMergeSource) +
                        "    ON t.customer = s.customer" +
                        "    WHEN MATCHED THEN UPDATE SET purchase = s.purchase, zipcode = s.zipcode, spouse = s.spouse, address = s.address",
                targetCustomerCount / 2);

        assertQuery(
                "SELECT customer, purchase, zipcode, spouse, address FROM " + targetTable,
                format("VALUES %s, %s", originalInsertFirstHalf, firstMergeSource));

        String nextInsert = range(targetCustomerCount, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('jack_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 4000, 74000, intValue, intValue))
                .collect(joining(", "));

        assertUpdate(format("INSERT INTO %s (customer, purchase, zipcode, spouse, address) VALUES %s", targetTable, nextInsert), targetCustomerCount / 2);

        String secondMergeSource = range(1, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jen_%s', '%s Poe Ct')", intValue, 5000, 85000, intValue, intValue))
                .collect(joining(", "));

        assertUpdate(session,
                format("MERGE INTO %s t USING (VALUES %s) AS s(customer, purchase, zipcode, spouse, address)", targetTable, secondMergeSource) +
                        "    ON t.customer = s.customer" +
                        "    WHEN MATCHED AND t.zipcode = 91000 THEN DELETE" +
                        "    WHEN MATCHED AND s.zipcode = 85000 THEN UPDATE SET zipcode = 60000" +
                        "    WHEN MATCHED THEN UPDATE SET zipcode = s.zipcode, spouse = s.spouse, address = s.address" +
                        "    WHEN NOT MATCHED THEN INSERT (customer, purchase, zipcode, spouse, address) VALUES(s.customer, s.purchase, s.zipcode, s.spouse, s.address)",
                targetCustomerCount * 3 / 2 - 1);

        String updatedBeginning = range(targetCustomerCount / 2, targetCustomerCount)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jill_%s', '%s Eop Ct')", intValue, 3000, 60000, intValue, intValue))
                .collect(joining(", "));
        String updatedMiddle = range(targetCustomerCount, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jen_%s', '%s Poe Ct')", intValue, 5000, 85000, intValue, intValue))
                .collect(joining(", "));
        String updatedEnd = range(targetCustomerCount, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('jack_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 4000, 74000, intValue, intValue))
                .collect(joining(", "));

        assertQuery(
                "SELECT customer, purchase, zipcode, spouse, address FROM " + targetTable,
                format("VALUES %s, %s, %s", updatedBeginning, updatedMiddle, updatedEnd));

        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeSimpleQueryPartitioned()
    {
        String targetTable = "merge_simple_" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (partitioning = ARRAY['address'])", targetTable));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);

        @Language("SQL") String query = format("MERGE INTO %s t USING ", targetTable) +
                "(SELECT * FROM (VALUES ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire'), ('Ed', 7, 'Etherville'))) AS s(customer, purchases, address)" +
                "    " +
                "ON (t.customer = s.customer)" +
                "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)";
        assertUpdate(query, 4);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Aaron', 11, 'Arches'), ('Bill', 7, 'Buena'), ('Dave', 22, 'Darbyshire'), ('Ed', 7, 'Etherville')");

        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    @Override
    public void testMergeMultipleRowsMatchFails()
    {
        testMergeMultipleRowsMatchFails("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)");
        testMergeMultipleRowsMatchFails("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (partitioning = ARRAY['bucket(customer, 3)'])");
        testMergeMultipleRowsMatchFails("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (partitioning = ARRAY['customer'])");
        testMergeMultipleRowsMatchFails("CREATE TABLE %s (customer VARCHAR, address VARCHAR, purchases INT) WITH (partitioning = ARRAY['address'])");
        testMergeMultipleRowsMatchFails("CREATE TABLE %s (purchases INT, customer VARCHAR, address VARCHAR) WITH (partitioning = ARRAY['address', 'customer'])");
    }

    private void testMergeMultipleRowsMatchFails(String createTableSql)
    {
        String targetTable = "merge_multiple_target_" + randomNameSuffix();
        String sourceTable = "merge_multiple_source_" + randomNameSuffix();
        assertUpdate(format(createTableSql, targetTable));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Antioch')", targetTable), 2);

        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", sourceTable));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Adelphi'), ('Aaron', 8, 'Ashland')", sourceTable), 2);

        assertThatThrownBy(() -> computeActual(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                "    WHEN MATCHED THEN UPDATE SET address = s.address"))
                .hasMessage("One MERGE target table row matched more than one source row");

        assertUpdate(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                        "    WHEN MATCHED AND s.address = 'Adelphi' THEN UPDATE SET address = s.address",
                1);
        assertQuery("SELECT customer, purchases, address FROM " + targetTable, "VALUES ('Aaron', 5, 'Adelphi'), ('Bill', 7, 'Antioch')");
        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeWithDifferentPartitioning()
    {
        testMergeWithDifferentPartitioning(
                "target_partitioned_source_and_target_partitioned_and_bucketed",
                "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (partitioning = ARRAY['address', 'bucket(customer, 3)'])",
                "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (partitioning = ARRAY['address', 'bucket(customer, 3)'])");
        testMergeWithDifferentPartitioning(
                "target_flat_source_partitioned_by_customer",
                "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)",
                "CREATE TABLE %s (purchases INT, address VARCHAR, customer VARCHAR) WITH (partitioning = ARRAY['customer'])");
        testMergeWithDifferentPartitioning(
                "target_partitioned_by_customer_source_flat",
                "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (partitioning = ARRAY['customer'])",
                "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)");
        testMergeWithDifferentPartitioning(
                "target_bucketed_by_customer_source_flat",
                "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (partitioning = ARRAY['bucket(customer, 3)'])",
                "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)");
        testMergeWithDifferentPartitioning(
                "target_partitioned_source_partitioned_and_bucketed",
                "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (partitioning = ARRAY['customer'])",
                "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (partitioning = ARRAY['address', 'bucket(customer, 3)'])");
        testMergeWithDifferentPartitioning(
                "target_partitioned_target_partitioned_and_bucketed",
                "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (partitioning = ARRAY['address', 'bucket(customer, 3)'])",
                "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (partitioning = ARRAY['customer'])");
    }

    private void testMergeWithDifferentPartitioning(String testDescription, String createTargetTableSql, String createSourceTableSql)
    {
        String targetTable = format("%s_target_%s", testDescription, randomNameSuffix());
        String sourceTable = format("%s_source_%s", testDescription, randomNameSuffix());

        assertUpdate(format(createTargetTableSql, targetTable));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);

        assertUpdate(format(createSourceTableSql, sourceTable));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Ed', 7, 'Etherville'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire')", sourceTable), 4);

        @Language("SQL") String sql = format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)";
        assertUpdate(sql, 4);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Aaron', 11, 'Arches'), ('Bill', 7, 'Buena'), ('Dave', 22, 'Darbyshire'), ('Ed', 7, 'Etherville')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        // This value depends on metastore type
        return OptionalInt.of(128);
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("Schema name must be shorter than or equal to '128' characters but got '129'");
    }

    @Test
    public void testSnapshotSummariesHaveTrinoQueryIdFormatV1()
    {
        String tableName = "test_snapshot_query_ids_v1" + randomNameSuffix();

        // Create empty table
        assertQueryIdAndUserStored(tableName, executeWithQueryId(format("CREATE TABLE %s (a bigint, b bigint) WITH (format_version = 1, partitioning = ARRAY['a'])", tableName)));

        // Insert some records, creating 3 partitions
        assertQueryIdAndUserStored(tableName, executeWithQueryId(format("INSERT INTO %s VALUES (1, 100), (2, 300), (2, 350), (3, 250)", tableName)));

        // Delete whole partition
        assertQueryIdAndUserStored(tableName, executeWithQueryId(format("DELETE FROM %s WHERE a = 2", tableName)));

        // Insert some more and then optimize
        assertQueryIdAndUserStored(tableName, executeWithQueryId(format("INSERT INTO %s VALUES (1, 400)", tableName)));
        assertQueryIdAndUserStored(tableName, executeWithQueryId(format("ALTER TABLE %s EXECUTE OPTIMIZE", tableName)));
    }

    @Test
    public void testSnapshotSummariesHaveTrinoQueryIdFormatV2()
    {
        String tableName = "test_snapshot_query_ids_v2" + randomNameSuffix();
        String sourceTableName = "test_source_table_for_ctas" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (a bigint, b bigint)", sourceTableName));
        assertUpdate(format("INSERT INTO %s VALUES (1, 1), (1, 4), (1, 20), (2, 2)", sourceTableName), 4);

        // Create table with CTAS
        assertQueryIdAndUserStored(tableName, executeWithQueryId(format("CREATE TABLE %s WITH (format_version = 2, partitioning = ARRAY['a']) " +
                "AS SELECT * FROM %s", tableName, sourceTableName)));

        // Insert records
        assertQueryIdAndUserStored(tableName, executeWithQueryId(format("INSERT INTO %s VALUES (1, 100), (2, 300), (3, 250)", tableName)));

        // Delete a whole partition
        assertQueryIdAndUserStored(tableName, executeWithQueryId(format("DELETE FROM %s WHERE a = 2", tableName)));

        // Delete an individual row
        assertQueryIdAndUserStored(tableName, executeWithQueryId(format("DELETE FROM %s WHERE a = 1 AND b = 4", tableName)));

        // Update an individual row
        assertQueryIdAndUserStored(tableName, executeWithQueryId(format("UPDATE %s SET b = 900 WHERE a = 1 AND b = 1", tableName)));

        // Merge
        assertQueryIdAndUserStored(tableName, executeWithQueryId(format("MERGE INTO %s t USING %s s ON t.a = s.a AND t.b = s.b " +
                "WHEN MATCHED THEN UPDATE SET b = t.b * 50", tableName, sourceTableName)));
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        // This value depends on metastore type
        return OptionalInt.of(128);
    }

    @Override
    protected OptionalInt maxTableRenameLength()
    {
        // This value depends on metastore type
        return OptionalInt.of(128);
    }

    @Test
    public void testSetPartitionedColumnType()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_set_partitioned_column_type_", "WITH (partitioning = ARRAY['part']) AS SELECT 1 AS id, CAST(123 AS integer) AS part")) {
            assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN part SET DATA TYPE bigint");

            assertThat(query("SELECT part FROM " + table.getName()))
                    .matches("VALUES bigint '123'");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE part = 123"))
                    .isFullyPushedDown();
            assertThat((String) computeScalar("SHOW CREATE TABLE " + table.getName()))
                    .contains("partitioning = ARRAY['part']");
        }
    }

    @Test
    public void testSetTransformPartitionedColumnType()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_set_partitioned_column_type_", "WITH (partitioning = ARRAY['bucket(part, 10)']) AS SELECT CAST(123 AS integer) AS part")) {
            assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN part SET DATA TYPE bigint");

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES bigint '123'");
            assertThat((String) computeScalar("SHOW CREATE TABLE " + table.getName()))
                    .contains("partitioning = ARRAY['bucket(part, 10)']");
        }
    }

    @Test
    public void testAlterTableWithUnsupportedProperties()
    {
        String tableName = "test_alter_table_with_unsupported_properties_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a bigint)");

        assertQueryFails("ALTER TABLE " + tableName + " SET PROPERTIES orc_bloom_filter_columns = ARRAY['a']",
                "The following properties cannot be updated: orc_bloom_filter_columns");
        assertQueryFails("ALTER TABLE " + tableName + " SET PROPERTIES location = '/var/data/table/', orc_bloom_filter_fpp = 0.5",
                "The following properties cannot be updated: location, orc_bloom_filter_fpp");
        assertQueryFails("ALTER TABLE " + tableName + " SET PROPERTIES format = 'ORC', orc_bloom_filter_columns = ARRAY['a']",
                "The following properties cannot be updated: orc_bloom_filter_columns");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDropTableWithMissingMetadataFile()
            throws Exception
    {
        String tableName = "test_drop_table_with_missing_metadata_file_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 x, 'INDIA' y", 1);

        String tableLocation = getTableLocation(tableName);
        Location metadataLocation = Location.of(getLatestMetadataLocation(fileSystem, tableLocation));

        // Delete current metadata file
        fileSystem.deleteFile(metadataLocation);
        assertThat(fileSystem.newInputFile(metadataLocation).exists())
                .describedAs("Current metadata file should not exist")
                .isFalse();

        // try to drop table
        assertUpdate("DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertThat(fileSystem.listFiles(Location.of(tableLocation)).hasNext())
                .describedAs("Table location should not exist")
                .isFalse();
    }

    @Test
    public void testDropTableWithMissingSnapshotFile()
            throws Exception
    {
        String tableName = "test_drop_table_with_missing_snapshot_file_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 x, 'INDIA' y", 1);

        String tableLocation = getTableLocation(tableName);
        String metadataLocation = getLatestMetadataLocation(fileSystem, tableLocation);
        TableMetadata tableMetadata = TableMetadataParser.read(new ForwardingFileIo(fileSystem), metadataLocation);
        Location currentSnapshotFile = Location.of(tableMetadata.currentSnapshot().manifestListLocation());

        // Delete current snapshot file
        fileSystem.deleteFile(currentSnapshotFile);
        assertThat(fileSystem.newInputFile(currentSnapshotFile).exists())
                .describedAs("Current snapshot file should not exist")
                .isFalse();

        // try to drop table
        assertUpdate("DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertThat(fileSystem.listFiles(Location.of(tableLocation)).hasNext())
                .describedAs("Table location should not exist")
                .isFalse();
    }

    @Test
    public void testDropTableWithMissingManifestListFile()
            throws Exception
    {
        String tableName = "test_drop_table_with_missing_manifest_list_file_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 x, 'INDIA' y", 1);

        String tableLocation = getTableLocation(tableName);
        String metadataLocation = getLatestMetadataLocation(fileSystem, tableLocation);
        FileIO fileIo = new ForwardingFileIo(fileSystem);
        TableMetadata tableMetadata = TableMetadataParser.read(fileIo, metadataLocation);
        Location manifestListFile = Location.of(tableMetadata.currentSnapshot().allManifests(fileIo).get(0).path());

        // Delete Manifest List file
        fileSystem.deleteFile(manifestListFile);
        assertThat(fileSystem.newInputFile(manifestListFile).exists())
                .describedAs("Manifest list file should not exist")
                .isFalse();

        // try to drop table
        assertUpdate("DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertThat(fileSystem.listFiles(Location.of(tableLocation)).hasNext())
                .describedAs("Table location should not exist")
                .isFalse();
    }

    @Test
    public void testDropTableWithMissingDataFile()
            throws Exception
    {
        String tableName = "test_drop_table_with_missing_data_file_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 x, 'INDIA' y", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'POLAND')", 1);

        Location tableLocation = Location.of(getTableLocation(tableName));
        Location tableDataPath = tableLocation.appendPath("data");
        FileIterator fileIterator = fileSystem.listFiles(tableDataPath);
        assertThat(fileIterator.hasNext()).isTrue();
        Location dataFile = fileIterator.next().location();

        // Delete data file
        fileSystem.deleteFile(dataFile);
        assertThat(fileSystem.newInputFile(dataFile).exists())
                .describedAs("Data file should not exist")
                .isFalse();

        // try to drop table
        assertUpdate("DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertThat(fileSystem.listFiles(tableLocation).hasNext())
                .describedAs("Table location should not exist")
                .isFalse();
    }

    @Test
    public void testDropTableWithNonExistentTableLocation()
            throws Exception
    {
        String tableName = "test_drop_table_with_non_existent_table_location_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 x, 'INDIA' y", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'POLAND')", 1);

        Location tableLocation = Location.of(getTableLocation(tableName));

        // Delete table location
        fileSystem.deleteDirectory(tableLocation);
        assertThat(fileSystem.listFiles(tableLocation).hasNext())
                .describedAs("Table location should not exist")
                .isFalse();

        // try to drop table
        assertUpdate("DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    @Test
    public void testCorruptedTableLocation()
            throws Exception
    {
        String tableName = "test_corrupted_table_location_" + randomNameSuffix();
        SchemaTableName schemaTableName = SchemaTableName.schemaTableName(getSession().getSchema().orElseThrow(), tableName);
        assertUpdate("CREATE TABLE " + tableName + " (id INT, country VARCHAR, independence ROW(month VARCHAR, year INT))");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'INDIA', ROW ('Aug', 1947)), (2, 'POLAND', ROW ('Nov', 1918)), (3, 'USA', ROW ('Jul', 1776))", 3);

        Location tableLocation = Location.of(getTableLocation(tableName));
        Location metadataLocation = tableLocation.appendPath("metadata");

        // break the table by deleting all metadata files
        fileSystem.deleteDirectory(metadataLocation);
        assertThat(fileSystem.listFiles(metadataLocation).hasNext())
                .describedAs("Metadata location should not exist")
                .isFalse();

        // Assert queries fail cleanly
        assertQueryFails("TABLE " + tableName, "Metadata not found in metadata location for table " + schemaTableName);
        assertQueryFails("SELECT * FROM " + tableName + " WHERE false", "Metadata not found in metadata location for table " + schemaTableName);
        assertQueryFails("SELECT 1 FROM " + tableName + " WHERE false", "Metadata not found in metadata location for table " + schemaTableName);
        assertQueryFails("SHOW CREATE TABLE " + tableName, "Metadata not found in metadata location for table " + schemaTableName);
        assertQueryFails("CREATE TABLE a_new_table (LIKE " + tableName + " EXCLUDING PROPERTIES)", "Metadata not found in metadata location for table " + schemaTableName);
        assertQueryFails("CREATE OR REPLACE TABLE " + tableName + " (id INT, country VARCHAR, independence ROW(month VARCHAR, year INT))", "Metadata not found in metadata location for table " + schemaTableName);
        assertQueryFails("CREATE OR REPLACE TABLE " + tableName + " AS SELECT 1 x, 'IRELAND' y", "Metadata not found in metadata location for table " + schemaTableName);
        assertQueryFails("DESCRIBE " + tableName, "Metadata not found in metadata location for table " + schemaTableName);
        assertQueryFails("SHOW COLUMNS FROM " + tableName, "Metadata not found in metadata location for table " + schemaTableName);
        assertQueryFails("SHOW STATS FOR " + tableName, "Metadata not found in metadata location for table " + schemaTableName);
        assertQueryFails("ANALYZE " + tableName, "Metadata not found in metadata location for table " + schemaTableName);
        assertQueryFails("ALTER TABLE " + tableName + " EXECUTE optimize", "Metadata not found in metadata location for table " + schemaTableName);
        assertQueryFails("ALTER TABLE " + tableName + " EXECUTE vacuum", "Metadata not found in metadata location for table " + schemaTableName);
        assertQueryFails("ALTER TABLE " + tableName + " RENAME TO bad_person_some_new_name", "Metadata not found in metadata location for table " + schemaTableName);
        assertQueryFails("ALTER TABLE " + tableName + " ADD COLUMN foo int", "Metadata not found in metadata location for table " + schemaTableName);
        assertQueryFails("ALTER TABLE " + tableName + " ADD COLUMN independence.month int", "Metadata not found in metadata location for table " + schemaTableName);
        assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN country", "Metadata not found in metadata location for table " + schemaTableName);
        assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN independence.month", "Metadata not found in metadata location for table " + schemaTableName);
        assertQueryFails("ALTER TABLE " + tableName + " SET PROPERTIES format = 'PARQUET'", "Metadata not found in metadata location for table " + schemaTableName);
        assertQueryFails("INSERT INTO " + tableName + " VALUES (NULL, NULL, ROW(NULL, NULL))", "Metadata not found in metadata location for table " + schemaTableName);
        assertQueryFails("UPDATE " + tableName + " SET country = 'AUSTRIA'", "Metadata not found in metadata location for table " + schemaTableName);
        assertQueryFails("DELETE FROM " + tableName, "Metadata not found in metadata location for table " + schemaTableName);
        assertQueryFails("MERGE INTO  " + tableName + " USING (SELECT 1 a) input ON true WHEN MATCHED THEN DELETE", "Metadata not found in metadata location for table " + schemaTableName);
        assertQueryFails("TRUNCATE TABLE " + tableName, "Metadata not found in metadata location for table " + schemaTableName);
        assertQueryFails("COMMENT ON TABLE " + tableName + " IS NULL", "Metadata not found in metadata location for table " + schemaTableName);
        assertQueryFails("COMMENT ON COLUMN " + tableName + ".foo IS NULL", "Metadata not found in metadata location for table " + schemaTableName);
        assertQueryFails("CALL iceberg.system.rollback_to_snapshot(CURRENT_SCHEMA, '" + tableName + "', 8954597067493422955)", "Metadata not found in metadata location for table " + schemaTableName);

        // Avoid failing metadata queries
        assertQuery("SHOW TABLES LIKE 'test_corrupted_table_location_%' ESCAPE '\\'", "VALUES '" + tableName + "'");
        assertQueryReturnsEmptyResult("SELECT column_name, data_type FROM information_schema.columns " +
                "WHERE table_schema = CURRENT_SCHEMA AND table_name LIKE 'test_corrupted_table_location_%' ESCAPE '\\'");
        assertQueryReturnsEmptyResult("SELECT column_name, data_type FROM system.jdbc.columns " +
                "WHERE table_cat = CURRENT_CATALOG AND table_schem = CURRENT_SCHEMA AND table_name LIKE 'test_corrupted_table_location_%' ESCAPE '\\'");

        // DROP TABLE should succeed so that users can remove their corrupted table
        assertQuerySucceeds("DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertThat(fileSystem.listFiles(tableLocation).hasNext())
                .describedAs("Table location should not exist")
                .isFalse();
    }

    @Test
    public void testDropCorruptedTableWithHiveRedirection()
            throws Exception
    {
        String hiveRedirectionCatalog = "hive_with_redirections";
        String icebergCatalog = "iceberg_test";
        String schema = "default";
        String tableName = "test_drop_corrupted_table_with_hive_redirection_" + randomNameSuffix();
        String hiveTableName = "%s.%s.%s".formatted(hiveRedirectionCatalog, schema, tableName);
        String icebergTableName = "%s.%s.%s".formatted(icebergCatalog, schema, tableName);

        File dataDirectory = Files.createTempDirectory("test_corrupted_iceberg_table").toFile();
        dataDirectory.deleteOnExit();

        Session icebergSession = testSessionBuilder()
                .setCatalog(icebergCatalog)
                .setSchema(schema)
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(icebergSession)
                .build();
        queryRunner.installPlugin(new IcebergPlugin());
        queryRunner.createCatalog(
                icebergCatalog,
                "iceberg",
                ImmutableMap.of(
                        "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                        "hive.metastore.catalog.dir", dataDirectory.getPath(),
                        "fs.hadoop.enabled", "true"));

        queryRunner.installPlugin(new TestingHivePlugin(dataDirectory.toPath()));
        queryRunner.createCatalog(
                hiveRedirectionCatalog,
                "hive",
                ImmutableMap.of("hive.iceberg-catalog-name", icebergCatalog));

        queryRunner.execute("CREATE SCHEMA " + schema);
        queryRunner.execute("CREATE TABLE " + icebergTableName + " (id INT, country VARCHAR, independence ROW(month VARCHAR, year INT))");
        queryRunner.execute("INSERT INTO " + icebergTableName + " VALUES (1, 'INDIA', ROW ('Aug', 1947)), (2, 'POLAND', ROW ('Nov', 1918)), (3, 'USA', ROW ('Jul', 1776))");

        assertThat(queryRunner.execute("TABLE " + hiveTableName))
                .containsAll(queryRunner.execute("TABLE " + icebergTableName));

        Location tableLocation = Location.of((String) queryRunner.execute("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*/[^/]*$', '') FROM " + tableName).getOnlyValue());
        Location metadataLocation = tableLocation.appendPath("metadata");

        // break the table by deleting all metadata files
        fileSystem.deleteDirectory(metadataLocation);
        assertThat(fileSystem.listFiles(metadataLocation).hasNext())
                .describedAs("Metadata location should not exist")
                .isFalse();

        // DROP TABLE should succeed using hive redirection
        queryRunner.execute("DROP TABLE " + hiveTableName);
        assertThat(queryRunner.tableExists(getSession(), icebergTableName)).isFalse();
        assertThat(fileSystem.listFiles(tableLocation).hasNext())
                .describedAs("Table location should not exist")
                .isFalse();
    }

    @Test
    @Timeout(10)
    public void testNoRetryWhenMetadataFileInvalid()
            throws Exception
    {
        String tableName = "test_no_retry_when_metadata_file_invalid_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 id", 1);

        String tableLocation = getTableLocation(tableName);
        String metadataFileLocation = getLatestMetadataLocation(fileSystem, tableLocation);

        ObjectMapper mapper = JsonUtil.mapper();
        JsonNode jsonNode = mapper.readValue(fileSystem.newInputFile(Location.of(metadataFileLocation)).newStream(), JsonNode.class);
        ArrayNode fieldsNode = (ArrayNode) jsonNode.get("schemas").get(0).get("fields");
        ObjectNode newFieldNode = fieldsNode.get(0).deepCopy();
        // Add duplicate field to produce validation error while reading the metadata file
        fieldsNode.add(newFieldNode);

        byte[] modifiedJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(jsonNode);

        // Corrupt metadata file by overwriting the invalid metadata content
        fileSystem.newOutputFile(Location.of(metadataFileLocation)).createOrOverwrite(modifiedJson);

        assertThat(query("SELECT * FROM " + tableName))
                .failure().hasMessage("Invalid metadata file for table tpch.%s".formatted(tableName));

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testTableChangesFunctionAfterSchemaChange()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_table_changes_function_",
                "AS SELECT nationkey, name FROM tpch.tiny.nation WITH NO DATA")) {
            long initialSnapshot = getCurrentSnapshotId(table.getName());
            assertUpdate("INSERT INTO " + table.getName() + " SELECT nationkey, name FROM nation WHERE nationkey < 5", 5);
            long snapshotAfterInsert = getCurrentSnapshotId(table.getName());

            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN name");
            assertUpdate("INSERT INTO " + table.getName() + " SELECT nationkey FROM nation WHERE nationkey >= 5 AND nationkey < 10", 5);
            long snapshotAfterDropColumn = getCurrentSnapshotId(table.getName());

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN comment VARCHAR");
            assertUpdate("INSERT INTO " + table.getName() + " SELECT nationkey, comment FROM nation WHERE nationkey >= 10 AND nationkey < 15", 5);
            long snapshotAfterAddColumn = getCurrentSnapshotId(table.getName());

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN name VARCHAR");
            assertUpdate("INSERT INTO " + table.getName() + " SELECT nationkey, comment, name FROM nation WHERE nationkey >= 15", 10);
            long snapshotAfterReaddingNameColumn = getCurrentSnapshotId(table.getName());

            assertQuery(
                    "SELECT nationkey, name, _change_type, _change_version_id, _change_ordinal " +
                            "FROM TABLE(system.table_changes(CURRENT_SCHEMA, '%s', %s, %s))".formatted(table.getName(), initialSnapshot, snapshotAfterInsert),
                    "SELECT nationkey, name, 'insert', %s, 0 FROM nation WHERE nationkey < 5".formatted(snapshotAfterInsert));

            assertQuery(
                    "SELECT nationkey, _change_type, _change_version_id, _change_ordinal " +
                            "FROM TABLE(system.table_changes(CURRENT_SCHEMA, '%s', %s, %s))".formatted(table.getName(), initialSnapshot, snapshotAfterDropColumn),
                    "SELECT nationkey, 'insert', %s, 0 FROM nation WHERE nationkey < 5 UNION SELECT nationkey, 'insert', %s, 1 FROM nation WHERE nationkey >= 5 AND nationkey < 10 ".formatted(snapshotAfterInsert, snapshotAfterDropColumn));

            assertQuery(
                    "SELECT nationkey, comment, _change_type, _change_version_id, _change_ordinal " +
                            "FROM TABLE(system.table_changes(CURRENT_SCHEMA, '%s', %s, %s))".formatted(table.getName(), initialSnapshot, snapshotAfterAddColumn),
                    ("SELECT nationkey, NULL, 'insert', %s, 0 FROM nation WHERE nationkey < 5 " +
                            "UNION SELECT nationkey, NULL, 'insert', %s, 1 FROM nation WHERE nationkey >= 5 AND nationkey < 10 " +
                            "UNION SELECT nationkey, comment, 'insert', %s, 2 FROM nation WHERE nationkey >= 10 AND nationkey < 15").formatted(snapshotAfterInsert, snapshotAfterDropColumn, snapshotAfterAddColumn));

            assertQuery(
                    "SELECT nationkey, comment, name, _change_type, _change_version_id, _change_ordinal " +
                            "FROM TABLE(system.table_changes(CURRENT_SCHEMA, '%s', %s, %s))".formatted(table.getName(), initialSnapshot, snapshotAfterReaddingNameColumn),
                    ("SELECT nationkey, NULL, NULL, 'insert', %s, 0 FROM nation WHERE nationkey < 5 " +
                            "UNION SELECT nationkey, NULL, NULL, 'insert', %s, 1 FROM nation WHERE nationkey >= 5 AND nationkey < 10 " +
                            "UNION SELECT nationkey, comment, NULL, 'insert', %s, 2 FROM nation WHERE nationkey >= 10 AND nationkey < 15" +
                            "UNION SELECT nationkey, comment, name, 'insert', %s, 3 FROM nation WHERE nationkey >= 15").formatted(snapshotAfterInsert, snapshotAfterDropColumn, snapshotAfterAddColumn, snapshotAfterReaddingNameColumn));
        }
    }

    @Test
    public void testTableChangesFunctionInvalidArguments()
    {
        assertQueryFails(
                "SELECT * FROM TABLE(system.table_changes(\"SCHEMA\" => CURRENT_SCHEMA, \"SCHEMA_NAME\" => CURRENT_SCHEMA, \"TABLE\" => 'region', START_SNAPSHOT_ID => 1, END_SNAPSHOT_ID => 2))",
                "Cannot use both SCHEMA and SCHEMA_NAME arguments");
        assertQueryFails(
                "SELECT * FROM TABLE(system.table_changes(\"SCHEMA\" => CURRENT_SCHEMA, \"TABLE\" => 'region', \"TABLE_NAME\" => 'region', START_SNAPSHOT_ID => 1, END_SNAPSHOT_ID => 2))",
                "Cannot use both TABLE and TABLE_NAME arguments");

        assertQueryFails(
                "SELECT * FROM TABLE(system.table_changes(start_snapshot_id => 1, end_snapshot_id => 2))",
                "SCHEMA_NAME argument not found");
        assertQueryFails(
                "SELECT * FROM TABLE(system.table_changes(schema_name => 'tpch', start_snapshot_id => 1, end_snapshot_id => 2))",
                "TABLE_NAME argument not found");
    }

    @Test
    public void testIdentityPartitionFilterMissing()
    {
        String tableName = "test_partition_" + randomNameSuffix();

        Session session = withPartitionFilterRequired(getSession());

        assertUpdate(session, "CREATE TABLE " + tableName + " (id integer, a varchar, b varchar, ds varchar) WITH (partitioning = ARRAY['ds'])");
        assertUpdate(session, "INSERT INTO " + tableName + " (id, a, ds) VALUES (1, 'a', 'a')", 1);
        assertQueryFails(session, "SELECT id FROM " + tableName + " WHERE ds IS NOT null OR true", "Filter required for tpch\\." + tableName + " on at least one of the partition columns: ds");
        assertUpdate(session, "DROP TABLE " + tableName);
    }

    @Test
    public void testBucketPartitionFilterMissing()
    {
        String tableName = "test_partition_" + randomNameSuffix();

        Session session = withPartitionFilterRequired(getSession());

        assertUpdate(session, "CREATE TABLE " + tableName + " (id integer, a varchar, b varchar, ds varchar) WITH (partitioning = ARRAY['bucket(ds, 16)'])");
        assertUpdate(session, "INSERT INTO " + tableName + " (id, a, ds) VALUES (1, 'a', 'a')", 1);
        assertQueryFails(session, "SELECT id FROM " + tableName + " WHERE ds IS NOT null OR true", "Filter required for tpch\\." + tableName + " on at least one of the partition columns: ds");
        assertUpdate(session, "DROP TABLE " + tableName);
    }

    @Test
    public void testIdentityPartitionFilterIncluded()
    {
        String tableName = "test_partition_" + randomNameSuffix();

        Session session = withPartitionFilterRequired(getSession());

        assertUpdate(session, "CREATE TABLE " + tableName + " (id integer, a varchar, b varchar, ds varchar) WITH (partitioning = ARRAY['ds'])");
        assertUpdate(session, "INSERT INTO " + tableName + " (id, a, ds) VALUES (1, 'a', 'a')", 1);
        String query = "SELECT id FROM " + tableName + " WHERE ds = 'a'";
        assertQuery(session, query, "VALUES 1");
        assertUpdate(session, "DROP TABLE " + tableName);
    }

    @Test
    public void testBucketPartitionFilterIncluded()
    {
        String tableName = "test_partition_" + randomNameSuffix();

        Session session = withPartitionFilterRequired(getSession());

        assertUpdate(session, "CREATE TABLE " + tableName + " (id integer, a varchar, b varchar, ds varchar) WITH (partitioning = ARRAY['bucket(ds, 16)'])");
        assertUpdate(session, "INSERT INTO " + tableName + " (id, a, ds) VALUES (1, 'a', 'a'), (2, 'b', 'b')", 2);
        String query = "SELECT id FROM " + tableName + " WHERE ds = 'a'";
        assertQuery(session, query, "VALUES 1");
        assertUpdate(session, "DROP TABLE " + tableName);
    }

    @Test
    public void testMultiPartitionedTableFilterIncluded()
    {
        String tableName = "test_partition_" + randomNameSuffix();

        Session session = withPartitionFilterRequired(getSession());

        assertUpdate(session, "CREATE TABLE " + tableName + " (id integer, a varchar, b varchar, ds varchar) WITH (partitioning = ARRAY['id', 'bucket(ds, 16)'])");
        assertUpdate(session, "INSERT INTO " + tableName + " (id, a, ds) VALUES (1, 'a', 'a'), (2, 'b', 'b')", 2);
        // include predicate only on 'id', not on 'ds'
        String query = "SELECT id, ds FROM " + tableName + " WHERE id = 2";
        assertQuery(session, query, "VALUES (2, 'b')");
        assertUpdate(session, "DROP TABLE " + tableName);
    }

    @Test
    public void testIdentityPartitionIsNotNullFilter()
    {
        String tableName = "test_partition_" + randomNameSuffix();

        Session session = withPartitionFilterRequired(getSession());

        assertUpdate(session, "CREATE TABLE " + tableName + " (id integer, a varchar, b varchar, ds varchar) WITH (partitioning = ARRAY['ds'])");
        assertUpdate(session, "INSERT INTO " + tableName + " (id, a, ds) VALUES (1, 'a', 'a')", 1);
        assertQuery(session, "SELECT id FROM " + tableName + " WHERE ds IS NOT null", "VALUES 1");
        assertUpdate(session, "DROP TABLE " + tableName);
    }

    @Test
    public void testJoinPartitionFilterIncluded()
    {
        String tableName1 = "test_partition_" + randomNameSuffix();
        String tableName2 = "test_partition_" + randomNameSuffix();

        Session session = withPartitionFilterRequired(getSession());

        assertUpdate(session, "CREATE TABLE " + tableName1 + " (id integer, a varchar, b varchar, ds varchar) WITH (partitioning = ARRAY['ds'])");
        assertUpdate(session, "INSERT INTO " + tableName1 + " (id, a, ds) VALUES (1, 'a', 'a')", 1);
        assertUpdate(session, "CREATE TABLE " + tableName2 + " (id integer, a varchar, b varchar, ds varchar) WITH (partitioning = ARRAY['ds'])");
        assertUpdate(session, "INSERT INTO " + tableName2 + " (id, a, ds) VALUES (1, 'a', 'a')", 1);
        assertQuery(session, "SELECT a.id, b.id FROM " + tableName1 + " a JOIN " + tableName2 + " b ON (a.ds = b.ds) WHERE a.ds = 'a'", "VALUES (1, 1)");
        assertUpdate(session, "DROP TABLE " + tableName1);
        assertUpdate(session, "DROP TABLE " + tableName2);
    }

    @Test
    public void testJoinWithMissingPartitionFilter()
    {
        String tableName1 = "test_partition_" + randomNameSuffix();
        String tableName2 = "test_partition_" + randomNameSuffix();

        Session session = withPartitionFilterRequired(getSession());

        assertUpdate(session, "CREATE TABLE " + tableName1 + " (id integer, a varchar, b varchar, ds varchar) WITH (partitioning = ARRAY['ds'])");
        assertUpdate(session, "INSERT INTO " + tableName1 + " (id, a, ds) VALUES (1, 'a', 'a')", 1);
        assertUpdate(session, "CREATE TABLE " + tableName2 + " (id integer, a varchar, b varchar, ds varchar) WITH (partitioning = ARRAY['ds'])");
        assertUpdate(session, "INSERT INTO " + tableName2 + " (id, a, ds) VALUES (1, 'a', 'a')", 1);
        assertQueryFails(session, "SELECT a.id, b.id FROM " + tableName1 + " a JOIN " + tableName2 + " b ON (a.id = b.id) WHERE a.ds = 'a'", "Filter required for tpch\\." + tableName2 + " on at least one of the partition columns: ds");
        assertUpdate(session, "DROP TABLE " + tableName1);
        assertUpdate(session, "DROP TABLE " + tableName2);
    }

    @Test
    public void testJoinWithPartitionFilterOnPartitionedTable()
    {
        String tableName1 = "test_partition_" + randomNameSuffix();
        String tableName2 = "test_partition_" + randomNameSuffix();

        Session session = withPartitionFilterRequired(getSession());

        assertUpdate(session, "CREATE TABLE " + tableName1 + " (id integer, a varchar, b varchar, ds varchar) WITH (partitioning = ARRAY['ds'])");
        assertUpdate(session, "INSERT INTO " + tableName1 + " (id, a, ds) VALUES (1, 'a', 'a')", 1);
        assertUpdate(session, "CREATE TABLE " + tableName2 + " (id integer, a varchar, b varchar, ds varchar)");
        assertUpdate(session, "INSERT INTO " + tableName2 + " (id, a, ds) VALUES (1, 'a', 'a')", 1);
        assertQuery(session, "SELECT a.id, b.id FROM " + tableName1 + " a JOIN " + tableName2 + " b ON (a.id = b.id) WHERE a.ds = 'a'", "VALUES (1, 1)");
        assertUpdate(session, "DROP TABLE " + tableName1);
        assertUpdate(session, "DROP TABLE " + tableName2);
    }

    @Test
    public void testPartitionPredicateWithCasting()
    {
        String tableName = "test_partition_" + randomNameSuffix();

        Session session = withPartitionFilterRequired(getSession());

        assertUpdate(session, "CREATE TABLE " + tableName + " (id integer, a varchar, b varchar, ds varchar) WITH (partitioning = ARRAY['ds'])");
        assertUpdate(session, "INSERT INTO " + tableName + " (id, a, ds) VALUES (1, '1', '1')", 1);
        String query = "SELECT id FROM " + tableName + " WHERE cast(ds as integer) = 1";
        assertQuery(session, query, "VALUES 1");
        assertUpdate(session, "DROP TABLE " + tableName);
    }

    @Test
    public void testNestedQueryWithInnerPartitionPredicate()
    {
        String tableName = "test_partition_" + randomNameSuffix();

        Session session = withPartitionFilterRequired(getSession());

        assertUpdate(session, "CREATE TABLE " + tableName + " (id integer, a varchar, b varchar, ds varchar) WITH (partitioning = ARRAY['ds'])");
        assertUpdate(session, "INSERT INTO " + tableName + " (id, a, ds) VALUES (1, '1', '1')", 1);
        String query = "SELECT id FROM (SELECT * FROM " + tableName + " WHERE cast(ds as integer) = 1) WHERE cast(a as integer) = 1";
        assertQuery(session, query, "VALUES 1");
        assertUpdate(session, "DROP TABLE " + tableName + "");
    }

    @Test
    public void testPredicateOnNonPartitionColumn()
    {
        String tableName = "test_partition_" + randomNameSuffix();

        Session session = withPartitionFilterRequired(getSession());

        assertUpdate(session, "CREATE TABLE " + tableName + " (id integer, a varchar, b varchar, ds varchar) WITH (partitioning = ARRAY['ds'])");
        assertUpdate(session, "INSERT INTO " + tableName + " (id, a, ds) VALUES (1, '1', '1')", 1);
        String query = "SELECT id FROM " + tableName + " WHERE cast(b as integer) = 1";
        assertQueryFails(session, query, "Filter required for tpch\\." + tableName + " on at least one of the partition columns: ds");
        assertUpdate(session, "DROP TABLE " + tableName);
    }

    @Test
    public void testNonSelectStatementsWithPartitionFilterRequired()
    {
        String tableName1 = "test_partition_" + randomNameSuffix();
        String tableName2 = "test_partition_" + randomNameSuffix();

        Session session = withPartitionFilterRequired(getSession());

        assertUpdate(session, "CREATE TABLE " + tableName1 + " (id integer, a varchar, b varchar, ds varchar) WITH (partitioning = ARRAY['ds'])");
        assertUpdate(session, "CREATE TABLE " + tableName2 + " (id integer, a varchar, b varchar, ds varchar) WITH (partitioning = ARRAY['ds'])");
        assertUpdate(session, "INSERT INTO " + tableName1 + " (id, a, ds) VALUES (1, '1', '1'), (2, '2', '2')", 2);
        assertUpdate(session, "INSERT INTO " + tableName2 + " (id, a, ds) VALUES (1, '1', '1'), (3, '3', '3')", 2);

        // These non-SELECT statements fail without a partition filter
        String errorMessage = "Filter required for tpch\\." + tableName1 + " on at least one of the partition columns: ds";
        assertQueryFails(session, "ALTER TABLE " + tableName1 + " EXECUTE optimize", errorMessage);
        assertQueryFails(session, "UPDATE " + tableName1 + " SET a = 'New'", errorMessage);
        assertQueryFails(session, "MERGE INTO " + tableName1 + " AS a USING " + tableName2 + " AS b ON (a.ds = b.ds) WHEN MATCHED THEN UPDATE SET a = 'New'", errorMessage);
        assertQueryFails(session, "DELETE FROM " + tableName1 + " WHERE a = '1'", errorMessage);

        // Adding partition filters to each solves the problem
        assertQuerySucceeds(session, "ALTER TABLE " + tableName1 + " EXECUTE optimize WHERE ds in ('2', '4')");
        assertQuerySucceeds(session, "UPDATE " + tableName1 + " SET a = 'New' WHERE ds = '2'");
        assertQuerySucceeds(session, "MERGE INTO " + tableName1 + " AS a USING (SELECT * FROM " + tableName2 + " WHERE ds = '1') AS b ON (a.ds = b.ds) WHEN MATCHED THEN UPDATE SET a = 'New'");
        assertQuerySucceeds(session, "DELETE FROM " + tableName1 + " WHERE ds = '1'");

        // Analyze should always succeed, since currently it cannot take a partition argument like Hive
        assertQuerySucceeds(session, "ANALYZE " + tableName1);
        assertQuerySucceeds(session, "ANALYZE " + tableName2 + " WITH (columns = ARRAY['id', 'a'])");

        assertUpdate(session, "DROP TABLE " + tableName1);
        assertUpdate(session, "DROP TABLE " + tableName2);
    }

    @Test
    public void testPartitionFilterRequiredSchemas()
    {
        String schemaName = "test_unenforced_schema_" + randomNameSuffix();
        String tableName = "test_partition_" + randomNameSuffix();

        Session session = Session.builder(withPartitionFilterRequired(getSession()))
                .setCatalogSessionProperty("iceberg", "query_partition_filter_required_schemas", "[\"tpch\"]")
                .build();

        assertUpdate(session, "CREATE SCHEMA " + schemaName);
        assertUpdate(session, format("CREATE TABLE %s.%s (id, a, ds) WITH (partitioning = ARRAY['ds']) AS SELECT 1, '1', '1'", schemaName, tableName), 1);
        assertUpdate(session, "CREATE TABLE " + tableName + " (id, a, ds) WITH (partitioning = ARRAY['ds']) AS SELECT 1, '1', '1'", 1);

        String enforcedQuery = "SELECT id FROM tpch." + tableName + " WHERE a = '1'";
        assertQueryFails(session, enforcedQuery, "Filter required for tpch\\." + tableName + " on at least one of the partition columns: ds");

        String unenforcedQuery = format("SELECT id FROM %s.%s WHERE a = '1'", schemaName, tableName);
        assertQuerySucceeds(session, unenforcedQuery);

        assertUpdate(session, "DROP TABLE " + tableName);
        assertUpdate(session, "DROP SCHEMA " + schemaName + " CASCADE");
    }

    @Test
    public void testIgnorePartitionFilterRequiredSchemas()
    {
        String tableName = "test_partition_" + randomNameSuffix();

        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", "query_partition_filter_required_schemas", "[\"tpch\"]")
                .build();
        assertUpdate(session, "CREATE TABLE " + tableName + " (id, a, ds) WITH (partitioning = ARRAY['ds']) AS SELECT 1, '1', '1'", 1);
        assertQuerySucceeds(session, "SELECT id FROM " + tableName + " WHERE a = '1'");
        assertUpdate(session, "DROP TABLE " + tableName);
    }

    private static Session withPartitionFilterRequired(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty("iceberg", "query_partition_filter_required", "true")
                .build();
    }

    @Test
    public void testUuidDynamicFilter()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        try (TestTable dataTable = new TestTable(getQueryRunner()::execute, "data_table", "(value uuid)");
                TestTable filteringTable = new TestTable(getQueryRunner()::execute, "filtering_table", "(filtering_value uuid)")) {
            assertUpdate("INSERT INTO " + dataTable.getName() + " VALUES UUID 'f73894f0-5447-41c5-a727-436d04c7f8ab', UUID '4f676658-67c9-4e80-83be-ec75f0b9d0c9'", 2);
            assertUpdate("INSERT INTO " + filteringTable.getName() + " VALUES UUID 'f73894f0-5447-41c5-a727-436d04c7f8ab'", 1);

            assertThat(query(
                    Session.builder(getSession())
                            .setCatalogSessionProperty(catalog, DYNAMIC_FILTERING_WAIT_TIMEOUT, "10s")
                            .build(),
                    "SELECT value FROM " + dataTable.getName() + " WHERE EXISTS (SELECT 1 FROM " + filteringTable.getName() + " WHERE filtering_value = value)"))
                    .matches("VALUES UUID 'f73894f0-5447-41c5-a727-436d04c7f8ab'");
        }
    }

    @Test
    public void testDynamicFilterWithExplicitPartitionFilter()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        try (TestTable salesTable = new TestTable(getQueryRunner()::execute, "sales_table", "(date date, receipt_id varchar, amount decimal(10,2)) with (partitioning=array['date'])");
                TestTable dimensionTable = new TestTable(getQueryRunner()::execute, "dimension_table", "(date date, following_holiday boolean, year int)")) {
            assertUpdate(
                    """
                    INSERT INTO %s
                    VALUES
                        (DATE '2023-01-01' , false, 2023),
                        (DATE '2023-01-02' , true, 2023),
                        (DATE '2023-01-03' , false, 2023)""".formatted(dimensionTable.getName()), 3);
            assertUpdate(
                    """
                    INSERT INTO %s
                    VALUES
                        (DATE '2023-01-02' , '#2023#1', DECIMAL '122.12'),
                        (DATE '2023-01-02' , '#2023#2', DECIMAL '124.12'),
                        (DATE '2023-01-02' , '#2023#3', DECIMAL '99.99'),
                        (DATE '2023-01-02' , '#2023#4', DECIMAL '95.12'),
                        (DATE '2023-01-03' , '#2023#5', DECIMAL '199.12'),
                        (DATE '2023-01-04' , '#2023#6', DECIMAL '99.55'),
                        (DATE '2023-01-05' , '#2023#7', DECIMAL '50.11'),
                        (DATE '2023-01-05' , '#2023#8', DECIMAL '60.20'),
                        (DATE '2023-01-05' , '#2023#9', DECIMAL '70.75'),
                        (DATE '2023-01-05' , '#2023#10', DECIMAL '80.12')""".formatted(salesTable.getName()), 10);

            String selectQuery =
                    """
                    SELECT receipt_id
                    FROM %s s
                    JOIN %s d
                        ON  s.date = d.date
                    WHERE
                        d.following_holiday = true AND
                        d.date BETWEEN DATE '2023-01-01' AND DATE '2024-01-01'""".formatted(salesTable.getName(), dimensionTable.getName());
            MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(
                    Session.builder(getSession())
                            .setCatalogSessionProperty(catalog, DYNAMIC_FILTERING_WAIT_TIMEOUT, "10s")
                            .build(),
                    selectQuery);
            MaterializedResult expected = computeActual(
                    Session.builder(getSession())
                            .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                            .build(),
                    selectQuery);
            assertEqualsIgnoreOrder(result.result(), expected);

            DynamicFilterService.DynamicFiltersStats dynamicFiltersStats = getDistributedQueryRunner().getCoordinator()
                    .getQueryManager()
                    .getFullQueryInfo(result.queryId())
                    .getQueryStats()
                    .getDynamicFiltersStats();
            // The dynamic filter reduces the range specified for the partition column `date` from `date :: [[2023-01-01, 2024-01-01]]` to `date :: {[2023-01-02]}`
            assertThat(dynamicFiltersStats.getTotalDynamicFilters()).isEqualTo(1L);
            assertThat(dynamicFiltersStats.getLazyDynamicFilters()).isEqualTo(1L);
            assertThat(dynamicFiltersStats.getReplicatedDynamicFilters()).isEqualTo(0L);
            assertThat(dynamicFiltersStats.getDynamicFiltersCompleted()).isEqualTo(1L);
        }
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("Table name must be shorter than or equal to '128' characters but got .*");
    }

    @Test
    public void testCreateTableWithCompressionCodec()
    {
        for (HiveCompressionCodec compressionCodec : HiveCompressionCodec.values()) {
            testCreateTableWithCompressionCodec(compressionCodec);
        }
    }

    private void testCreateTableWithCompressionCodec(HiveCompressionCodec compressionCodec)
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "compression_codec", compressionCodec.name())
                .build();
        String tableName = "test_table_with_compression_" + compressionCodec;
        String createTableSql = format("CREATE TABLE %s AS TABLE tpch.tiny.nation", tableName);
        // TODO (https://github.com/trinodb/trino/issues/9142) Support LZ4 compression with native Parquet writer
        if ((format == IcebergFileFormat.PARQUET || format == AVRO) && compressionCodec == HiveCompressionCodec.LZ4) {
            assertTrinoExceptionThrownBy(() -> computeActual(session, createTableSql))
                    .hasMessage("Compression codec LZ4 not supported for " + format.humanName());
            return;
        }
        assertUpdate(session, createTableSql, 25);
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation");
        assertQuery("SELECT count(*) FROM " + tableName, "VALUES 25");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testTypeCoercionOnCreateTableAsSelect()
    {
        for (TypeCoercionTestSetup setup : typeCoercionOnCreateTableAsSelectProvider()) {
            try (TestTable testTable = new TestTable(
                    getQueryRunner()::execute,
                    "test_coercion_show_create_table",
                    format("AS SELECT %s a", setup.sourceValueLiteral))) {
                assertThat(getColumnType(testTable.getName(), "a")).isEqualTo(setup.newColumnType);
                assertThat(query("SELECT * FROM " + testTable.getName()))
                        .as("source value: %s, new type: %s, new value: %s", setup.sourceValueLiteral, setup.newColumnType, setup.newValueLiteral)
                        .skippingTypesCheck()
                        .matches("SELECT " + setup.newValueLiteral);
            }
        }
    }

    @Test
    public void testTypeCoercionOnCreateTableAsSelectWithNoData()
    {
        for (TypeCoercionTestSetup setup : typeCoercionOnCreateTableAsSelectProvider()) {
            try (TestTable testTable = new TestTable(
                    getQueryRunner()::execute,
                    "test_coercion_show_create_table",
                    format("AS SELECT %s a WITH NO DATA", setup.sourceValueLiteral))) {
                assertThat(getColumnType(testTable.getName(), "a")).isEqualTo(setup.newColumnType);
            }
        }
    }

    private List<TypeCoercionTestSetup> typeCoercionOnCreateTableAsSelectProvider()
    {
        return typeCoercionOnCreateTableAsSelectData().stream()
                .map(this::filterTypeCoercionOnCreateTableAsSelectProvider)
                .flatMap(Optional::stream)
                .collect(toList());
    }

    protected Optional<TypeCoercionTestSetup> filterTypeCoercionOnCreateTableAsSelectProvider(TypeCoercionTestSetup setup)
    {
        return Optional.of(setup);
    }

    private List<TypeCoercionTestSetup> typeCoercionOnCreateTableAsSelectData()
    {
        return ImmutableList.<TypeCoercionTestSetup>builder()
                .add(new TypeCoercionTestSetup("TINYINT '127'", "integer", "INTEGER '127'"))
                .add(new TypeCoercionTestSetup("SMALLINT '32767'", "integer", "INTEGER '32767'"))
                .add(new TypeCoercionTestSetup("TIMESTAMP '1970-01-01 00:00:00'", "timestamp(6)", "TIMESTAMP '1970-01-01 00:00:00.000000'"))
                .add(new TypeCoercionTestSetup("TIMESTAMP '1970-01-01 00:00:00.9'", "timestamp(6)", "TIMESTAMP '1970-01-01 00:00:00.900000'"))
                .add(new TypeCoercionTestSetup("TIMESTAMP '1970-01-01 00:00:00.56'", "timestamp(6)", "TIMESTAMP '1970-01-01 00:00:00.560000'"))
                .add(new TypeCoercionTestSetup("TIMESTAMP '1970-01-01 00:00:00.123'", "timestamp(6)", "TIMESTAMP '1970-01-01 00:00:00.123000'"))
                .add(new TypeCoercionTestSetup("TIMESTAMP '1970-01-01 00:00:00.4896'", "timestamp(6)", "TIMESTAMP '1970-01-01 00:00:00.489600'"))
                .add(new TypeCoercionTestSetup("TIMESTAMP '1970-01-01 00:00:00.89356'", "timestamp(6)", "TIMESTAMP '1970-01-01 00:00:00.893560'"))
                .add(new TypeCoercionTestSetup("TIMESTAMP '1970-01-01 00:00:00.123000'", "timestamp(6)", "TIMESTAMP '1970-01-01 00:00:00.123000'"))
                .add(new TypeCoercionTestSetup("TIMESTAMP '1970-01-01 00:00:00.999'", "timestamp(6)", "TIMESTAMP '1970-01-01 00:00:00.999000'"))
                .add(new TypeCoercionTestSetup("TIMESTAMP '1970-01-01 00:00:00.123456'", "timestamp(6)", "TIMESTAMP '1970-01-01 00:00:00.123456'"))
                .add(new TypeCoercionTestSetup("TIMESTAMP '2020-09-27 12:34:56.1'", "timestamp(6)", "TIMESTAMP '2020-09-27 12:34:56.100000'"))
                .add(new TypeCoercionTestSetup("TIMESTAMP '2020-09-27 12:34:56.9'", "timestamp(6)", "TIMESTAMP '2020-09-27 12:34:56.900000'"))
                .add(new TypeCoercionTestSetup("TIMESTAMP '2020-09-27 12:34:56.123'", "timestamp(6)", "TIMESTAMP '2020-09-27 12:34:56.123000'"))
                .add(new TypeCoercionTestSetup("TIMESTAMP '2020-09-27 12:34:56.123000'", "timestamp(6)", "TIMESTAMP '2020-09-27 12:34:56.123000'"))
                .add(new TypeCoercionTestSetup("TIMESTAMP '2020-09-27 12:34:56.999'", "timestamp(6)", "TIMESTAMP '2020-09-27 12:34:56.999000'"))
                .add(new TypeCoercionTestSetup("TIMESTAMP '2020-09-27 12:34:56.123456'", "timestamp(6)", "TIMESTAMP '2020-09-27 12:34:56.123456'"))
                .add(new TypeCoercionTestSetup("TIMESTAMP '1970-01-01 00:00:00.1234561'", "timestamp(6)", "TIMESTAMP '1970-01-01 00:00:00.123456'"))
                .add(new TypeCoercionTestSetup("TIMESTAMP '1970-01-01 00:00:00.123456499'", "timestamp(6)", "TIMESTAMP '1970-01-01 00:00:00.123456'"))
                .add(new TypeCoercionTestSetup("TIMESTAMP '1970-01-01 00:00:00.123456499999'", "timestamp(6)", "TIMESTAMP '1970-01-01 00:00:00.123456'"))
                .add(new TypeCoercionTestSetup("TIMESTAMP '1970-01-01 00:00:00.1234565'", "timestamp(6)", "TIMESTAMP '1970-01-01 00:00:00.123457'"))
                .add(new TypeCoercionTestSetup("TIMESTAMP '1970-01-01 00:00:00.111222333444'", "timestamp(6)", "TIMESTAMP '1970-01-01 00:00:00.111222'"))
                .add(new TypeCoercionTestSetup("TIMESTAMP '1970-01-01 00:00:00.9999995'", "timestamp(6)", "TIMESTAMP '1970-01-01 00:00:01.000000'"))
                .add(new TypeCoercionTestSetup("TIMESTAMP '1970-01-01 23:59:59.9999995'", "timestamp(6)", "TIMESTAMP '1970-01-02 00:00:00.000000'"))
                .add(new TypeCoercionTestSetup("TIMESTAMP '1969-12-31 23:59:59.9999995'", "timestamp(6)", "TIMESTAMP '1970-01-01 00:00:00.000000'"))
                .add(new TypeCoercionTestSetup("TIMESTAMP '1969-12-31 23:59:59.999999499999'", "timestamp(6)", "TIMESTAMP '1969-12-31 23:59:59.999999'"))
                .add(new TypeCoercionTestSetup("TIMESTAMP '1969-12-31 23:59:59.9999994'", "timestamp(6)", "TIMESTAMP '1969-12-31 23:59:59.999999'"))
                .add(new TypeCoercionTestSetup("TIME '00:00:00'", "time(6)", "TIME '00:00:00.000000'"))
                .add(new TypeCoercionTestSetup("TIME '00:00:00.9'", "time(6)", "TIME '00:00:00.900000'"))
                .add(new TypeCoercionTestSetup("TIME '00:00:00.56'", "time(6)", "TIME '00:00:00.560000'"))
                .add(new TypeCoercionTestSetup("TIME '00:00:00.123'", "time(6)", "TIME '00:00:00.123000'"))
                .add(new TypeCoercionTestSetup("TIME '00:00:00.4896'", "time(6)", "TIME '00:00:00.489600'"))
                .add(new TypeCoercionTestSetup("TIME '00:00:00.89356'", "time(6)", "TIME '00:00:00.893560'"))
                .add(new TypeCoercionTestSetup("TIME '00:00:00.123000'", "time(6)", "TIME '00:00:00.123000'"))
                .add(new TypeCoercionTestSetup("TIME '00:00:00.999'", "time(6)", "TIME '00:00:00.999000'"))
                .add(new TypeCoercionTestSetup("TIME '00:00:00.123456'", "time(6)", "TIME '00:00:00.123456'"))
                .add(new TypeCoercionTestSetup("TIME '12:34:56.1'", "time(6)", "TIME '12:34:56.100000'"))
                .add(new TypeCoercionTestSetup("TIME '12:34:56.9'", "time(6)", "TIME '12:34:56.900000'"))
                .add(new TypeCoercionTestSetup("TIME '12:34:56.123'", "time(6)", "TIME '12:34:56.123000'"))
                .add(new TypeCoercionTestSetup("TIME '12:34:56.123000'", "time(6)", "TIME '12:34:56.123000'"))
                .add(new TypeCoercionTestSetup("TIME '12:34:56.999'", "time(6)", "TIME '12:34:56.999000'"))
                .add(new TypeCoercionTestSetup("TIME '12:34:56.123456'", "time(6)", "TIME '12:34:56.123456'"))
                .add(new TypeCoercionTestSetup("TIME '00:00:00.1234561'", "time(6)", "TIME '00:00:00.123456'"))
                .add(new TypeCoercionTestSetup("TIME '00:00:00.123456499'", "time(6)", "TIME '00:00:00.123456'"))
                .add(new TypeCoercionTestSetup("TIME '00:00:00.123456499999'", "time(6)", "TIME '00:00:00.123456'"))
                .add(new TypeCoercionTestSetup("TIME '00:00:00.1234565'", "time(6)", "TIME '00:00:00.123457'"))
                .add(new TypeCoercionTestSetup("TIME '00:00:00.111222333444'", "time(6)", "TIME '00:00:00.111222'"))
                .add(new TypeCoercionTestSetup("TIME '00:00:00.9999995'", "time(6)", "TIME '00:00:01.000000'"))
                .add(new TypeCoercionTestSetup("TIME '23:59:59.9999995'", "time(6)", "TIME '00:00:00.000000'"))
                .add(new TypeCoercionTestSetup("TIME '23:59:59.999999499999'", "time(6)", "TIME '23:59:59.999999'"))
                .add(new TypeCoercionTestSetup("TIME '23:59:59.9999994'", "time(6)", "TIME '23:59:59.999999'"))
                .add(new TypeCoercionTestSetup("CHAR 'A'", "varchar", "'A'"))
                .add(new TypeCoercionTestSetup("CHAR 'é'", "varchar", "'é'"))
                .add(new TypeCoercionTestSetup("CHAR 'A '", "varchar", "'A '"))
                .add(new TypeCoercionTestSetup("CHAR ' A'", "varchar", "' A'"))
                .add(new TypeCoercionTestSetup("CHAR 'ABc'", "varchar", "'ABc'"))
                .add(new TypeCoercionTestSetup("ARRAY[CHAR 'A']", "array(varchar)", "ARRAY['A']"))
                .add(new TypeCoercionTestSetup("ARRAY[ARRAY[CHAR 'nested']]", "array(array(varchar))", "ARRAY[ARRAY['nested']]"))
                .add(new TypeCoercionTestSetup("MAP(ARRAY[CHAR 'key'], ARRAY[CHAR 'value'])", "map(varchar, varchar)", "MAP(ARRAY['key'], ARRAY['value'])"))
                .add(new TypeCoercionTestSetup("MAP(ARRAY[CHAR 'key'], ARRAY[ARRAY[CHAR 'value']])", "map(varchar, array(varchar))", "MAP(ARRAY['key'], ARRAY[ARRAY['value']])"))
                // TODO Add test case for MAP type with ARRAY keys once https://github.com/trinodb/trino/issues/1146 is resolved
                .add(new TypeCoercionTestSetup("CAST(ROW('a') AS ROW(x CHAR))", "row(x varchar)", "CAST(ROW('a') AS ROW(x VARCHAR))"))
                .add(new TypeCoercionTestSetup("CAST(ROW(ROW('a')) AS ROW(x ROW(y CHAR)))", "row(x row(y varchar))", "CAST(ROW(ROW('a')) AS ROW(x ROW(y VARCHAR)))"))
                // tinyint -> integer
                .add(new TypeCoercionTestSetup("ARRAY[TINYINT '127']", "array(integer)", "ARRAY[127]"))
                .add(new TypeCoercionTestSetup("ARRAY[ARRAY[TINYINT '127']]", "array(array(integer))", "ARRAY[ARRAY[127]]"))
                .add(new TypeCoercionTestSetup("MAP(ARRAY[TINYINT '1'], ARRAY[TINYINT '10'])", "map(integer, integer)", "MAP(ARRAY[1], ARRAY[10])"))
                .add(new TypeCoercionTestSetup("MAP(ARRAY[TINYINT '1'], ARRAY[ARRAY[TINYINT '10']])", "map(integer, array(integer))", "MAP(ARRAY[1], ARRAY[ARRAY[10]])"))
                .add(new TypeCoercionTestSetup("CAST(ROW(127) AS ROW(x TINYINT))", "row(x integer)", "CAST(ROW(127) AS ROW(x INTEGER))"))
                .add(new TypeCoercionTestSetup("CAST(ROW(ROW(127)) AS ROW(x ROW(y TINYINT)))", "row(x row(y integer))", "CAST(ROW(ROW(127)) AS ROW(x ROW(y INTEGER)))"))
                // smallint -> integer
                .add(new TypeCoercionTestSetup("ARRAY[SMALLINT '32767']", "array(integer)", "ARRAY[32767]"))
                .add(new TypeCoercionTestSetup("ARRAY[ARRAY[SMALLINT '32767']]", "array(array(integer))", "ARRAY[ARRAY[32767]]"))
                .add(new TypeCoercionTestSetup("MAP(ARRAY[SMALLINT '1'], ARRAY[SMALLINT '10'])", "map(integer, integer)", "MAP(ARRAY[1], ARRAY[10])"))
                .add(new TypeCoercionTestSetup("MAP(ARRAY[SMALLINT '1'], ARRAY[ARRAY[SMALLINT '10']])", "map(integer, array(integer))", "MAP(ARRAY[1], ARRAY[ARRAY[10]])"))
                .add(new TypeCoercionTestSetup("CAST(ROW(32767) AS ROW(x SMALLINT))", "row(x integer)", "CAST(ROW(32767) AS ROW(x INTEGER))"))
                .add(new TypeCoercionTestSetup("CAST(ROW(ROW(32767)) AS ROW(x ROW(y SMALLINT)))", "row(x row(y integer))", "CAST(ROW(ROW(32767)) AS ROW(x ROW(y INTEGER)))"))
                .build();
    }

    public record TypeCoercionTestSetup(@Language("SQL") String sourceValueLiteral, String newColumnType, @Language("SQL") String newValueLiteral)
    {
        public TypeCoercionTestSetup
        {
            requireNonNull(sourceValueLiteral, "sourceValueLiteral is null");
            requireNonNull(newColumnType, "newColumnType is null");
            requireNonNull(newValueLiteral, "newValueLiteral is null");
        }

        public TypeCoercionTestSetup withNewValueLiteral(String newValueLiteral)
        {
            return new TypeCoercionTestSetup(sourceValueLiteral, newColumnType, newValueLiteral);
        }
    }

    @Test
    public void testAddColumnWithTypeCoercion()
    {
        testAddColumnWithTypeCoercion("tinyint", "integer");
        testAddColumnWithTypeCoercion("smallint", "integer");

        testAddColumnWithTypeCoercion("timestamp with time zone", "timestamp(6) with time zone");
        testAddColumnWithTypeCoercion("timestamp(0) with time zone", "timestamp(6) with time zone");
        testAddColumnWithTypeCoercion("timestamp(1) with time zone", "timestamp(6) with time zone");
        testAddColumnWithTypeCoercion("timestamp(2) with time zone", "timestamp(6) with time zone");
        testAddColumnWithTypeCoercion("timestamp(3) with time zone", "timestamp(6) with time zone");
        testAddColumnWithTypeCoercion("timestamp(4) with time zone", "timestamp(6) with time zone");
        testAddColumnWithTypeCoercion("timestamp(5) with time zone", "timestamp(6) with time zone");
        testAddColumnWithTypeCoercion("timestamp(6) with time zone", "timestamp(6) with time zone");
        testAddColumnWithTypeCoercion("timestamp(7) with time zone", "timestamp(6) with time zone");
        testAddColumnWithTypeCoercion("timestamp(8) with time zone", "timestamp(6) with time zone");
        testAddColumnWithTypeCoercion("timestamp(9) with time zone", "timestamp(6) with time zone");
        testAddColumnWithTypeCoercion("timestamp(10) with time zone", "timestamp(6) with time zone");
        testAddColumnWithTypeCoercion("timestamp(11) with time zone", "timestamp(6) with time zone");
        testAddColumnWithTypeCoercion("timestamp(12) with time zone", "timestamp(6) with time zone");

        testAddColumnWithTypeCoercion("timestamp", "timestamp(6)");
        testAddColumnWithTypeCoercion("timestamp(0)", "timestamp(6)");
        testAddColumnWithTypeCoercion("timestamp(1)", "timestamp(6)");
        testAddColumnWithTypeCoercion("timestamp(2)", "timestamp(6)");
        testAddColumnWithTypeCoercion("timestamp(3)", "timestamp(6)");
        testAddColumnWithTypeCoercion("timestamp(4)", "timestamp(6)");
        testAddColumnWithTypeCoercion("timestamp(5)", "timestamp(6)");
        testAddColumnWithTypeCoercion("timestamp(6)", "timestamp(6)");
        testAddColumnWithTypeCoercion("timestamp(7)", "timestamp(6)");
        testAddColumnWithTypeCoercion("timestamp(8)", "timestamp(6)");
        testAddColumnWithTypeCoercion("timestamp(9)", "timestamp(6)");
        testAddColumnWithTypeCoercion("timestamp(10)", "timestamp(6)");
        testAddColumnWithTypeCoercion("timestamp(11)", "timestamp(6)");
        testAddColumnWithTypeCoercion("timestamp(12)", "timestamp(6)");

        testAddColumnWithTypeCoercion("time", "time(6)");
        testAddColumnWithTypeCoercion("time(0)", "time(6)");
        testAddColumnWithTypeCoercion("time(1)", "time(6)");
        testAddColumnWithTypeCoercion("time(2)", "time(6)");
        testAddColumnWithTypeCoercion("time(3)", "time(6)");
        testAddColumnWithTypeCoercion("time(4)", "time(6)");
        testAddColumnWithTypeCoercion("time(5)", "time(6)");
        testAddColumnWithTypeCoercion("time(6)", "time(6)");
        testAddColumnWithTypeCoercion("time(7)", "time(6)");
        testAddColumnWithTypeCoercion("time(8)", "time(6)");
        testAddColumnWithTypeCoercion("time(9)", "time(6)");
        testAddColumnWithTypeCoercion("time(10)", "time(6)");
        testAddColumnWithTypeCoercion("time(11)", "time(6)");
        testAddColumnWithTypeCoercion("time(12)", "time(6)");

        testAddColumnWithTypeCoercion("char(1)", "varchar");

        testAddColumnWithTypeCoercion("array(char(10))", "array(varchar)");
        testAddColumnWithTypeCoercion("map(char(20), char(30))", "map(varchar, varchar)");
        testAddColumnWithTypeCoercion("row(x char(40))", "row(x varchar)");

        testAddColumnWithTypeCoercion("array(tinyint)", "array(integer)");
        testAddColumnWithTypeCoercion("map(tinyint, tinyint)", "map(integer, integer)");
        testAddColumnWithTypeCoercion("row(x tinyint)", "row(x integer)");

        testAddColumnWithTypeCoercion("array(smallint)", "array(integer)");
        testAddColumnWithTypeCoercion("map(smallint, smallint)", "map(integer, integer)");
        testAddColumnWithTypeCoercion("row(x smallint)", "row(x integer)");
    }

    private void testAddColumnWithTypeCoercion(String columnType, String expectedColumnType)
    {
        try (TestTable testTable = new TestTable(getQueryRunner()::execute, "test_coercion_add_column", "(a varchar, b row(x integer))")) {
            assertUpdate("ALTER TABLE " + testTable.getName() + " ADD COLUMN b.y " + columnType);
            assertThat(getColumnType(testTable.getName(), "b")).isEqualTo("row(x integer, y %s)".formatted(expectedColumnType));

            assertUpdate("ALTER TABLE " + testTable.getName() + " ADD COLUMN c " + columnType);
            assertThat(getColumnType(testTable.getName(), "c")).isEqualTo(expectedColumnType);
        }
    }

    @Test
    public void testSystemTables()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        for (TableType tableType : TableType.values()) {
            if (tableType != TableType.DATA) {
                // Like a system table. Make sure this is "table not found".
                assertQueryFails(
                        "TABLE \"$%s\"".formatted(tableType.name().toLowerCase(ENGLISH)),
                        "\\Qline 1:1: Table '%s.%s.\"$%s\"' does not exist".formatted(catalog, schema, tableType.name().toLowerCase(ENGLISH)));
            }
        }

        // given the base table exists
        assertQuerySucceeds("TABLE nation");
        // verify that <base>$<invalid-suffix> results in table not found
        assertQueryFails("TABLE \"nation$foo\"", "\\Qline 1:1: Table '%s.%s.\"nation$foo\"' does not exist".formatted(catalog, schema));
    }

    @Test
    public void testExtraProperties()
    {
        String tableName = "test_create_table_with_multiple_extra_properties_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (c1 integer) WITH (extra_properties = MAP(ARRAY['extra.property.one', 'extra.property.TWO'], ARRAY['one', 'two']))");

        assertThat(query("SELECT key, value FROM \"" + tableName + "$properties\" WHERE key IN ('extra.property.one', 'extra.property.two')"))
                .skippingTypesCheck()
                .matches("VALUES ('extra.property.one', 'one'), ('extra.property.two', 'two')");

        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES extra_properties = MAP(ARRAY['extra.property.one'], ARRAY['updated'])");
        assertThat(query("SELECT key, value FROM \"" + tableName + "$properties\" WHERE key IN ('extra.property.one', 'extra.property.two')"))
                .skippingTypesCheck()
                .matches("VALUES ('extra.property.one', 'updated'), ('extra.property.two', 'two')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testReplaceTableExtraProperties()
    {
        String tableName = "test_replace_table_with_multiple_extra_properties_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (c1 integer) WITH (extra_properties = MAP(ARRAY['extra.property.one', 'extra.property.two'], ARRAY['one', 'two']))");
        assertUpdate("CREATE OR REPLACE TABLE " + tableName + " (c1 integer) WITH (extra_properties = MAP(ARRAY['extra.property.three'], ARRAY['three']))");

        assertThat(query("SELECT key, value FROM \"" + tableName + "$properties\" WHERE key IN ('extra.property.one', 'extra.property.two', 'extra.property.three')"))
                .skippingTypesCheck()
                .matches("VALUES ('extra.property.three', 'three')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateTableAsSelectWithExtraProperties()
    {
        String tableName = "test_ctas_with_extra_properties_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (extra_properties = MAP(ARRAY['extra.property.one', 'extra.property.two'], ARRAY['one', 'two'])) " +
                "AS SELECT 1 as c1", 1);

        assertThat(query("SELECT key, value FROM \"" + tableName + "$properties\" WHERE key IN ('extra.property.one', 'extra.property.two')"))
                .skippingTypesCheck()
                .matches("VALUES ('extra.property.one', 'one'), ('extra.property.two', 'two')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testShowCreateNotContainExtraProperties()
    {
        String tableName = "test_show_create_table_with_extra_properties_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (c1 integer) WITH (extra_properties = MAP(ARRAY['extra.property.one', 'extra.property.two'], ARRAY['one', 'two']))");

        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName)).doesNotContain("extra_properties =", "extra.property.one", "extra.property.two");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testNullExtraProperty()
    {
        assertQueryFails(
                "CREATE TABLE test_create_table_with_null_extra_properties (c1 integer) WITH (extra_properties = MAP(ARRAY['null.property'], ARRAY[null]))",
                ".*\\QUnable to set catalog 'iceberg' table property 'extra_properties' to [MAP(ARRAY['null.property'], ARRAY[null])]: Extra table property value cannot be null '{null.property=null}'\\E");

        assertQueryFails(
                "CREATE TABLE test_create_table_with_as_null_extra_properties WITH (extra_properties = MAP(ARRAY['null.property'], ARRAY[null])) AS SELECT 1 as c1",
                ".*\\QUnable to set catalog 'iceberg' table property 'extra_properties' to [MAP(ARRAY['null.property'], ARRAY[null])]: Extra table property value cannot be null '{null.property=null}'\\E");
    }

    @Test
    public void testIllegalExtraPropertyKey()
    {
        assertQueryFails(
                "CREATE TABLE test_create_table_with_illegal_extra_properties (c1 integer) WITH (extra_properties = MAP(ARRAY['sorted_by'], ARRAY['id']))",
                "\\QIllegal keys in extra_properties: [sorted_by]");

        assertQueryFails(
                "CREATE TABLE test_create_table_as_with_illegal_extra_properties WITH (extra_properties = MAP(ARRAY['extra_properties'], ARRAY['some_value'])) AS SELECT 1 as c1",
                "\\QIllegal keys in extra_properties: [extra_properties]");

        assertQueryFails(
                "CREATE TABLE test_create_table_with_as_illegal_extra_properties WITH (extra_properties = MAP(ARRAY['write.format.default'], ARRAY['ORC'])) AS SELECT 1 as c1",
                "\\QIllegal keys in extra_properties: [write.format.default]");

        assertQueryFails(
                "CREATE TABLE test_create_table_with_as_illegal_extra_properties WITH (extra_properties = MAP(ARRAY['comment'], ARRAY['some comment'])) AS SELECT 1 as c1",
                "\\QIllegal keys in extra_properties: [comment]");

        assertQueryFails(
                "CREATE TABLE test_create_table_with_as_illegal_extra_properties WITH (extra_properties = MAP(ARRAY['not_allowed_property'], ARRAY['foo'])) AS SELECT 1 as c1",
                "\\QIllegal keys in extra_properties: [not_allowed_property]");
    }

    @Test
    public void testSetIllegalExtraPropertyKey()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_set_illegal_table_properties", "(x int)")) {
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " SET PROPERTIES extra_properties = MAP(ARRAY['sorted_by'], ARRAY['id'])",
                    "\\QIllegal keys in extra_properties: [sorted_by]");
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " SET PROPERTIES extra_properties = MAP(ARRAY['comment'], ARRAY['some comment'])",
                    "\\QIllegal keys in extra_properties: [comment]");
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " SET PROPERTIES extra_properties = MAP(ARRAY['not_allowed_property'], ARRAY['foo'])",
                    "\\QIllegal keys in extra_properties: [not_allowed_property]");
        }
    }

    @Test // regression test for https://github.com/trinodb/trino/issues/22922
    void testArrayElementChange()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_array_schema_change",
                "(col array(row(a varchar, b varchar)))",
                List.of("CAST(array[row('a', 'b')] AS array(row(a varchar, b varchar)))"))) {
            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN col.element.a");
            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN col.element.c varchar");
            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN col.element.b");

            String expected = format == ORC ? "CAST(array[row(NULL)] AS array(row(c varchar)))" : "CAST(NULL AS array(row(c varchar)))";
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES " + expected);
        }
    }

    // MAP type is tested in TestIcebergV2.testMapValueSchemaChange

    @Test
    void testRowFieldChange()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_row_schema_change",
                "(col row(a varchar, b varchar))")) {
            assertUpdate("INSERT INTO " + table.getName() + " SELECT CAST(row('a', 'b') AS row(a varchar, b varchar))", 1);

            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN col.a");
            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN col.c varchar");
            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN col.b");

            String expected = format == ORC || format == AVRO ? "CAST(row(NULL) AS row(c varchar))" : "CAST(NULL AS row(c varchar))";
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("SELECT " + expected);
        }
    }

    @Test
    public void testObjectStoreLayoutEnabledAndDataLocation()
            throws Exception
    {
        String tableName = "test_object_store_layout_enabled_data_location" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (object_store_layout_enabled = true, data_location = 'local:///data-location/xyz') AS SELECT 1 AS val", 1);

        Location tableLocation = Location.of(getTableLocation(tableName));
        assertThat(fileSystem.directoryExists(tableLocation).get()).isTrue();

        String filePath = (String) computeScalar("SELECT file_path FROM \"" + tableName + "$files\"");
        Location dataFileLocation = Location.of(filePath);
        assertThat(fileSystem.newInputFile(dataFileLocation).exists()).isTrue();
        assertThat(filePath).matches("local:///data-location/xyz/.{6}/tpch/%s.*".formatted(tableName));

        assertUpdate("DROP TABLE " + tableName);
        assertThat(fileSystem.newInputFile(dataFileLocation).exists()).isFalse();
        assertThat(fileSystem.newInputFile(tableLocation).exists()).isFalse();
    }

    @Test
    public void testCreateTableWithDataLocationButObjectStoreLayoutDisabled()
    {
        assertQueryFails(
                "CREATE TABLE test_data_location WITH (data_location = 'local:///data-location/xyz') AS SELECT 1 AS val",
                "Data location can only be set when object store layout is enabled");
    }

    @Test
    @Override
    public void testSetFieldMapKeyType()
    {
        // Iceberg doesn't support change a map 'key' column. Only map values can be changed.
        assertThatThrownBy(super::testSetFieldMapKeyType)
                .hasMessageContaining("Failed to set field type: Cannot alter map keys");
    }

    @Test
    @Override
    public void testSetNestedFieldMapKeyType()
    {
        // Iceberg doesn't support change a map 'key' column. Only map values can be changed.
        assertThatThrownBy(super::testSetNestedFieldMapKeyType)
                .hasMessageContaining("Failed to set field type: Cannot alter map keys");
    }

    @Override
    protected Optional<SetColumnTypeSetup> filterSetColumnTypesDataProvider(SetColumnTypeSetup setup)
    {
        if (setup.sourceColumnType().equals("timestamp(3) with time zone")) {
            // The connector returns UTC instead of the given time zone
            return Optional.of(setup.withNewValueLiteral("TIMESTAMP '2020-02-12 14:03:00.123000 +00:00'"));
        }
        switch ("%s -> %s".formatted(setup.sourceColumnType(), setup.newColumnType())) {
            case "tinyint -> smallint":
            case "bigint -> integer":
            case "decimal(5,3) -> decimal(5,2)":
            case "varchar -> char(20)":
            case "time(6) -> time(3)":
            case "timestamp(6) -> timestamp(3)":
            case "array(integer) -> array(bigint)":
                // Iceberg allows updating column types if the update is safe. Safe updates are:
                // - int to bigint
                // - float to double
                // - decimal(P,S) to decimal(P2,S) when P2 > P (scale cannot change)
                // https://iceberg.apache.org/docs/latest/spark-ddl/#alter-table--alter-column
                return Optional.of(setup.asUnsupported());

            case "varchar(100) -> varchar(50)":
                // Iceberg connector ignores the varchar length
                return Optional.empty();
        }
        return Optional.of(setup);
    }

    @Override
    protected void verifySetColumnTypeFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching(".*(Failed to set column type: Cannot change (column type:|type from .* to )" +
                "|Time(stamp)? precision \\(3\\) not supported for Iceberg. Use \"time(stamp)?\\(6\\)\" instead" +
                "|Type not supported for Iceberg: smallint|char\\(20\\)).*");
    }

    @Override
    protected Optional<SetColumnTypeSetup> filterSetFieldTypesDataProvider(SetColumnTypeSetup setup)
    {
        switch ("%s -> %s".formatted(setup.sourceColumnType(), setup.newColumnType())) {
            case "tinyint -> smallint":
            case "bigint -> integer":
            case "decimal(5,3) -> decimal(5,2)":
            case "varchar -> char(20)":
            case "time(6) -> time(3)":
            case "timestamp(6) -> timestamp(3)":
            case "array(integer) -> array(bigint)":
            case "row(x integer) -> row(x bigint)":
            case "row(x integer) -> row(y integer)":
            case "row(x integer, y integer) -> row(x integer, z integer)":
            case "row(x integer) -> row(x integer, y integer)":
            case "row(x integer, y integer) -> row(x integer)":
            case "row(x integer, y integer) -> row(y integer, x integer)":
            case "row(x integer, y integer) -> row(z integer, y integer, x integer)":
            case "row(x row(nested integer)) -> row(x row(nested bigint))":
            case "row(x row(a integer, b integer)) -> row(x row(b integer, a integer))":
                // Iceberg allows updating column types if the update is safe. Safe updates are:
                // - int to bigint
                // - float to double
                // - decimal(P,S) to decimal(P2,S) when P2 > P (scale cannot change)
                // https://iceberg.apache.org/docs/latest/spark-ddl/#alter-table--alter-column
                return Optional.of(setup.asUnsupported());

            case "varchar(100) -> varchar(50)":
                // Iceberg connector ignores the varchar length
                return Optional.empty();
        }
        return Optional.of(setup);
    }

    @Override
    protected void verifySetFieldTypeFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching(".*(Failed to set field type: Cannot change (column type:|type from .* to )" +
                "|Time(stamp)? precision \\(3\\) not supported for Iceberg. Use \"time(stamp)?\\(6\\)\" instead" +
                "|Type not supported for Iceberg: smallint|char\\(20\\)" +
                "|Iceberg doesn't support changing field type (from|to) non-primitive types).*");
    }

    @Override
    protected Session withoutSmallFileThreshold(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "parquet_small_file_threshold", "0B")
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "orc_tiny_stripe_threshold", "0B")
                .build();
    }

    private Session withSingleWriterPerTask(Session session)
    {
        return Session.builder(session)
                .setSystemProperty("task_min_writer_count", "1")
                .build();
    }

    private Session prepareCleanUpSession()
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", "expire_snapshots_min_retention", "0s")
                .setCatalogSessionProperty("iceberg", "remove_orphan_files_min_retention", "0s")
                .build();
    }

    private List<String> getAllMetadataFilesFromTableDirectory(String tableLocation)
            throws IOException
    {
        return listFiles(getIcebergTableMetadataPath(tableLocation));
    }

    protected List<String> listFiles(String directory)
            throws IOException
    {
        ImmutableList.Builder<String> files = ImmutableList.builder();
        FileIterator listing = fileSystem.listFiles(Location.of(directory));
        while (listing.hasNext()) {
            String location = listing.next().location().toString();
            if (location.matches(".*/\\..*\\.crc")) {
                continue;
            }
            files.add(location);
        }
        return files.build();
    }

    protected long fileSize(String location)
            throws IOException
    {
        return fileSystem.newInputFile(Location.of(location)).length();
    }

    protected void createFile(String location)
            throws IOException
    {
        fileSystem.newOutputFile(Location.of(location)).create().close();
    }

    private List<Long> getSnapshotIds(String tableName)
    {
        return getQueryRunner().execute(format("SELECT snapshot_id FROM \"%s$snapshots\"", tableName))
                .getOnlyColumn()
                .map(Long.class::cast)
                .collect(toImmutableList());
    }

    private List<Long> getTableHistory(String tableName)
    {
        return getQueryRunner().execute(format("SELECT snapshot_id FROM \"%s$history\"", tableName))
                .getOnlyColumn()
                .map(Long.class::cast)
                .collect(toImmutableList());
    }

    private List<Long> getLatestSequenceNumbersInMetadataLogEntries(String tableName)
    {
        return getQueryRunner().execute(format("SELECT latest_sequence_number FROM \"%s$metadata_log_entries\"", tableName))
                .getOnlyColumn()
                .map(Long.class::cast)
                .collect(toImmutableList());
    }

    private long getCurrentSnapshotId(String tableName)
    {
        return (long) computeScalar("SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC FETCH FIRST 1 ROW WITH TIES");
    }

    private String getIcebergTableDataPath(String tableLocation)
    {
        return tableLocation + "/data";
    }

    private String getIcebergTableMetadataPath(String tableLocation)
    {
        return tableLocation + "/metadata";
    }

    private long getCommittedAtInEpochMilliseconds(String tableName, long snapshotId)
    {
        return ((ZonedDateTime) computeActual(format("SELECT committed_at FROM \"%s$snapshots\" WHERE snapshot_id=%s LIMIT 1", tableName, snapshotId)).getOnlyValue())
                .toInstant().toEpochMilli();
    }

    private static String timestampLiteral(long epochMilliSeconds, int precision)
    {
        return DateTimeFormatter.ofPattern("'TIMESTAMP '''uuuu-MM-dd HH:mm:ss." + "S".repeat(precision) + " VV''")
                .format(Instant.ofEpochMilli(epochMilliSeconds).atZone(UTC));
    }

    private List<Long> getSnapshotsIdsByCreationOrder(String tableName)
    {
        int idField = 0;
        return getQueryRunner().execute(
                        format("SELECT snapshot_id FROM \"%s$snapshots\" ORDER BY committed_at", tableName))
                .getMaterializedRows().stream()
                .map(row -> (Long) row.getField(idField))
                .collect(toList());
    }

    private String getFieldFromLatestSnapshotSummary(String tableName, String summaryFieldName)
    {
        return getQueryRunner().execute(format("SELECT json_extract_scalar(CAST(SUMMARY AS JSON), '$.%s') FROM \"%s$snapshots\" ORDER BY committed_at DESC LIMIT 1", summaryFieldName, tableName))
                .getOnlyColumn()
                .map(String.class::cast)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(format("Table '%s' has zero snapshots or does not have the '%s' field in its snapshot summary.", tableName, summaryFieldName)));
    }

    private QueryId executeWithQueryId(String sql)
    {
        return getDistributedQueryRunner()
                .executeWithPlan(getSession(), sql)
                .queryId();
    }

    private void assertQueryIdAndUserStored(String tableName, QueryId queryId)
    {
        assertThat(getFieldFromLatestSnapshotSummary(tableName, TRINO_QUERY_ID_NAME))
                .isEqualTo(queryId.toString());
        assertThat(getFieldFromLatestSnapshotSummary(tableName, TRINO_USER_NAME))
                .isEqualTo("user");
    }
}
