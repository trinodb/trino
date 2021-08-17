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

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.operator.OperatorStats;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TypeOperators;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.ResultWithQueryId;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import io.trino.testng.services.Flaky;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FileFormat;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.SESSION;
import static io.trino.plugin.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static io.trino.plugin.iceberg.IcebergSplitManager.ICEBERG_DOMAIN_COMPACTION_THRESHOLD;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.testing.assertions.Assert.assertEquals;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.transaction.TransactionBuilder.transaction;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.apache.iceberg.FileFormat.ORC;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public abstract class BaseIcebergConnectorTest
        extends BaseConnectorTest
{
    private static final Pattern WITH_CLAUSE_EXTRACTOR = Pattern.compile(".*(WITH\\s*\\([^)]*\\))\\s*$", Pattern.DOTALL);

    private final FileFormat format;

    protected BaseIcebergConnectorTest(FileFormat format)
    {
        this.format = requireNonNull(format, "format is null");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createIcebergQueryRunner(
                Map.of(),
                Map.of("iceberg.file-format", format.name()),
                REQUIRED_TPCH_TABLES);
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_COMMENT_ON_COLUMN:
            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            case SUPPORTS_CREATE_VIEW:
                return true;

            case SUPPORTS_CREATE_MATERIALIZED_VIEW:
            case SUPPORTS_RENAME_MATERIALIZED_VIEW:
                return true;

            case SUPPORTS_DELETE:
                return true;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Test
    @Override
    public void testDelete()
    {
        // Deletes are covered with testMetadataDelete test methods
        assertThatThrownBy(super::testDelete)
                .hasStackTraceContaining("This connector only supports delete where one or more identity-transformed partitions are deleted entirely");
    }

    @Override
    public void testDeleteWithComplexPredicate()
    {
        // Deletes are covered with testMetadataDelete test methods
        assertThatThrownBy(super::testDeleteWithComplexPredicate)
                .hasStackTraceContaining("This connector only supports delete where one or more identity-transformed partitions are deleted entirely");
    }

    @Override
    public void testDeleteWithSemiJoin()
    {
        // Deletes are covered with testMetadataDelete test methods
        assertThatThrownBy(super::testDeleteWithSemiJoin)
                .hasStackTraceContaining("This connector only supports delete where one or more identity-transformed partitions are deleted entirely");
    }

    @Override
    public void testDeleteWithSubquery()
    {
        // Deletes are covered with testMetadataDelete test methods
        assertThatThrownBy(super::testDeleteWithSubquery)
                .hasStackTraceContaining("This connector only supports delete where one or more identity-transformed partitions are deleted entirely");
    }

    @Override
    public void testExplainAnalyzeWithDeleteWithSubquery()
    {
        // Deletes are covered with testMetadataDelete test methods
        assertThatThrownBy(super::testExplainAnalyzeWithDeleteWithSubquery)
                .hasStackTraceContaining("This connector only supports delete where one or more identity-transformed partitions are deleted entirely");
    }

    @Override
    public void testDeleteWithVarcharPredicate()
    {
        // Deletes are covered with testMetadataDelete test methods
        assertThatThrownBy(super::testDeleteWithVarcharPredicate)
                .hasStackTraceContaining("This connector only supports delete where one or more identity-transformed partitions are deleted entirely");
    }

    @Override
    public void testRowLevelDelete()
    {
        // Deletes are covered with testMetadataDelete test methods
        assertThatThrownBy(super::testRowLevelDelete)
                .hasStackTraceContaining("This connector only supports delete where one or more identity-transformed partitions are deleted entirely");
    }

    @Override
    public void testCharVarcharComparison()
    {
        assertThatThrownBy(super::testCharVarcharComparison)
                .hasMessage("Type not supported for Iceberg: char(3)");
    }

    @Test
    @Override
    public void testShowCreateSchema()
    {
        assertThat(computeActual("SHOW CREATE SCHEMA tpch").getOnlyValue().toString())
                .matches("CREATE SCHEMA iceberg.tpch\n" +
                        "AUTHORIZATION USER user\n" +
                        "WITH \\(\n" +
                        "\\s+location = '.*/iceberg_data/tpch'\n" +
                        "\\)");
    }

    @Override
    @Test
    public void testDescribeTable()
    {
        MaterializedResult expectedColumns = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
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
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        assertEquals(actualColumns, expectedColumns);
    }

    @Override
    @Test
    public void testShowCreateTable()
    {
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("CREATE TABLE iceberg.tpch.orders (\n" +
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
                        "   format = '" + format.name() + "'\n" +
                        ")");
    }

    @Override
    protected void checkInformationSchemaViewsForMaterializedView(String schemaName, String viewName)
    {
        // TODO should probably return materialized view, as it's also a view -- to be double checked
        assertThatThrownBy(() -> super.checkInformationSchemaViewsForMaterializedView(schemaName, viewName))
                .hasMessageFindingMatch("(?s)Expecting.*to contain:.*\\Q[(" + viewName + ")]");
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
        dropTable("test_iceberg_decimal");
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
        dropTable(tableName);
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
        dropTable(tableName);
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
        assertUpdate(format("CREATE TABLE %s (x uuid, y bigint) %s", tableName, partitioning));

        assertUpdate(format("INSERT INTO %s VALUES (UUID '406caec7-68b9-4778-81b2-a12ece70c8b1', 12345)", tableName), 1);
        assertQuery(format("SELECT count(*) FROM %s", tableName), "SELECT 1");
        assertQuery(format("SELECT x FROM %s", tableName), "SELECT CAST('406caec7-68b9-4778-81b2-a12ece70c8b1' AS UUID)");

        assertUpdate(format("INSERT INTO %s VALUES (UUID 'f79c3e09-677c-4bbd-a479-3f349cb785e7', 67890)", tableName), 1);
        assertUpdate(format("INSERT INTO %s VALUES (NULL, 7531)", tableName), 1);
        assertQuery(format("SELECT count(*) FROM %s", tableName), "SELECT 3");
        assertQuery(format("SELECT * FROM %s WHERE x = UUID '406caec7-68b9-4778-81b2-a12ece70c8b1'", tableName), "SELECT CAST('406caec7-68b9-4778-81b2-a12ece70c8b1' AS UUID), 12345");
        assertQuery(format("SELECT * FROM %s WHERE x = UUID 'f79c3e09-677c-4bbd-a479-3f349cb785e7'", tableName), "SELECT CAST('f79c3e09-677c-4bbd-a479-3f349cb785e7' AS UUID), 67890");
        assertQuery(format("SELECT * FROM %s WHERE x IS NULL", tableName), "SELECT NULL, 7531");
        assertQuery(format("SELECT x FROM %s WHERE y = 12345", tableName), "SELECT CAST('406caec7-68b9-4778-81b2-a12ece70c8b1' AS UUID)");
        assertQuery(format("SELECT x FROM %s WHERE y = 67890", tableName), "SELECT CAST('f79c3e09-677c-4bbd-a479-3f349cb785e7' AS UUID)");
        assertQuery(format("SELECT x FROM %s WHERE y = 7531", tableName), "SELECT NULL");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreatePartitionedTable()
    {
        assertUpdate("" +
                "CREATE TABLE test_partitioned_table (" +
                "  _string VARCHAR" +
                ", _bigint BIGINT" +
                ", _integer INTEGER" +
                ", _real REAL" +
                ", _double DOUBLE" +
                ", _boolean BOOLEAN" +
                ", _decimal_short DECIMAL(3,2)" +
                ", _decimal_long DECIMAL(30,10)" +
                ", _timestamp TIMESTAMP(6)" +
                ", _date DATE" +
                ") " +
                "WITH (" +
                "partitioning = ARRAY[" +
                "  '_string'," +
                "  '_integer'," +
                "  '_bigint'," +
                "  '_boolean'," +
                "  '_real'," +
                "  '_double'," +
                "  '_decimal_short', " +
                "  '_decimal_long'," +
                "  '_timestamp'," +
                "  '_date']" +
                ")");

        assertQueryReturnsEmptyResult("SELECT * FROM test_partitioned_table");

        @Language("SQL") String select = "" +
                "SELECT" +
                " 'foo' _string" +
                ", CAST(123 AS BIGINT) _bigint" +
                ", 456 _integer" +
                ", CAST('123.45' AS REAL) _real" +
                ", CAST('3.14' AS DOUBLE) _double" +
                ", true _boolean" +
                ", CAST('3.14' AS DECIMAL(3,2)) _decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _decimal_long" +
                ", CAST('2017-05-01 10:12:34' AS TIMESTAMP) _timestamp" +
                ", CAST('2017-05-01' AS DATE) _date";

        assertUpdate(format("INSERT INTO test_partitioned_table %s", select), 1);
        assertQuery("SELECT * FROM test_partitioned_table", select);

        assertQuery(
                "SELECT * FROM test_partitioned_table WHERE" +
                        " 'foo' = _string" +
                        " AND 456 = _integer" +
                        " AND CAST(123 AS BIGINT) = _bigint" +
                        " AND true = _boolean" +
                        " AND CAST('3.14' AS DECIMAL(3,2)) = _decimal_short" +
                        " AND CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) = _decimal_long" +
                        " AND CAST('2017-05-01 10:12:34' AS TIMESTAMP) = _timestamp" +
                        " AND CAST('2017-05-01' AS DATE) = _date",
                select);

        dropTable("test_partitioned_table");
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

        dropTable("test_partitioned_table_nested_type");
    }

    @Test
    public void testPartitionedTableWithNullValues()
    {
        assertUpdate("CREATE TABLE test_partitioned_table_with_null_values (" +
                "  _string VARCHAR" +
                ", _bigint BIGINT" +
                ", _integer INTEGER" +
                ", _real REAL" +
                ", _double DOUBLE" +
                ", _boolean BOOLEAN" +
                ", _decimal_short DECIMAL(3,2)" +
                ", _decimal_long DECIMAL(30,10)" +
                ", _timestamp TIMESTAMP(6)" +
                ", _date DATE" +
                ") " +
                "WITH (" +
                "partitioning = ARRAY[" +
                "  '_string'," +
                "  '_integer'," +
                "  '_bigint'," +
                "  '_boolean'," +
                "  '_real'," +
                "  '_double'," +
                "  '_decimal_short', " +
                "  '_decimal_long'," +
                "  '_timestamp'," +
                "  '_date']" +
                ")");

        assertQueryReturnsEmptyResult("SELECT * from test_partitioned_table_with_null_values");

        @Language("SQL") String select = "" +
                "SELECT" +
                " null _string" +
                ", null _bigint" +
                ", null _integer" +
                ", null _real" +
                ", null _double" +
                ", null _boolean" +
                ", null _decimal_short" +
                ", null _decimal_long" +
                ", null _timestamp" +
                ", null _date";

        assertUpdate("INSERT INTO test_partitioned_table_with_null_values " + select, 1);
        assertQuery("SELECT * from test_partitioned_table_with_null_values", select);
        dropTable("test_partitioned_table_with_null_values");
    }

    @Test
    public void testCreatePartitionedTableAs()
    {
        assertUpdate(
                "CREATE TABLE test_create_partitioned_table_as " +
                        "WITH (" +
                        "partitioning = ARRAY['ORDER_STATUS', 'Ship_Priority', 'Bucket(order_key,9)']" +
                        ") " +
                        "AS " +
                        "SELECT orderkey AS order_key, shippriority AS ship_priority, orderstatus AS order_status " +
                        "FROM tpch.tiny.orders",
                "SELECT count(*) from orders");

        assertEquals(
                computeScalar("SHOW CREATE TABLE test_create_partitioned_table_as"),
                format(
                        "CREATE TABLE %s.%s.%s (\n" +
                                "   order_key bigint,\n" +
                                "   ship_priority integer,\n" +
                                "   order_status varchar\n" +
                                ")\n" +
                                "WITH (\n" +
                                "   format = '%s',\n" +
                                "   partitioning = ARRAY['order_status','ship_priority','bucket(order_key, 9)']\n" +
                                ")",
                        getSession().getCatalog().orElseThrow(),
                        getSession().getSchema().orElseThrow(),
                        "test_create_partitioned_table_as",
                        format));

        assertQuery("SELECT * from test_create_partitioned_table_as", "SELECT orderkey, shippriority, orderstatus FROM orders");

        dropTable("test_create_partitioned_table_as");
    }

    @Test
    public void testColumnComments()
    {
        // TODO add support for setting comments on existing column and replace the test with io.trino.testing.AbstractTestDistributedQueries#testCommentColumn

        assertUpdate("CREATE TABLE test_column_comments (_bigint BIGINT COMMENT 'test column comment')");
        assertQuery(
                "SHOW COLUMNS FROM test_column_comments",
                "VALUES ('_bigint', 'bigint', '', 'test column comment')");

        assertUpdate("ALTER TABLE test_column_comments ADD COLUMN _varchar VARCHAR COMMENT 'test new column comment'");
        assertQuery(
                "SHOW COLUMNS FROM test_column_comments",
                "VALUES ('_bigint', 'bigint', '', 'test column comment'), ('_varchar', 'varchar', '', 'test new column comment')");

        dropTable("test_column_comments");
    }

    @Test
    public void testTableComments()
    {
        String createTableTemplate = "" +
                "CREATE TABLE iceberg.tpch.test_table_comments (\n" +
                "   _x bigint\n" +
                ")\n" +
                "COMMENT '%s'\n" +
                "WITH (\n" +
                format("   format = '%s'\n", format) +
                ")";
        @Language("SQL") String createTableSql = format(createTableTemplate, "test table comment", format);
        assertUpdate(createTableSql);
        MaterializedResult resultOfCreate = computeActual("SHOW CREATE TABLE test_table_comments");
        assertEquals(getOnlyElement(resultOfCreate.getOnlyColumnAsSet()), createTableSql);

        assertUpdate("COMMENT ON TABLE test_table_comments IS 'different test table comment'");
        MaterializedResult resultOfCommentChange = computeActual("SHOW CREATE TABLE test_table_comments");
        String afterChangeSql = format(createTableTemplate, "different test table comment", format);
        assertEquals(getOnlyElement(resultOfCommentChange.getOnlyColumnAsSet()), afterChangeSql);
        dropTable("iceberg.tpch.test_table_comments");

        String createTableWithoutComment = "" +
                "CREATE TABLE iceberg.tpch.test_table_comments (\n" +
                "   _x bigint\n" +
                ")\n" +
                "WITH (\n" +
                "   format = 'ORC'\n" +
                ")";
        assertUpdate(format(createTableWithoutComment, format));
        assertUpdate("COMMENT ON TABLE test_table_comments IS NULL");
        MaterializedResult resultOfRemovingComment = computeActual("SHOW CREATE TABLE test_table_comments");
        assertEquals(getOnlyElement(resultOfRemovingComment.getOnlyColumnAsSet()), format(createTableWithoutComment, format));

        dropTable("iceberg.tpch.test_table_comments");
    }

    @Test
    public void testRollbackSnapshot()
    {
        assertUpdate("CREATE TABLE test_rollback (col0 INTEGER, col1 BIGINT)");
        long afterCreateTableId = getLatestSnapshotId("test_rollback");

        assertUpdate("INSERT INTO test_rollback (col0, col1) VALUES (123, CAST(987 AS BIGINT))", 1);
        long afterFirstInsertId = getLatestSnapshotId("test_rollback");

        assertUpdate("INSERT INTO test_rollback (col0, col1) VALUES (456, CAST(654 AS BIGINT))", 1);
        assertQuery("SELECT * FROM test_rollback ORDER BY col0", "VALUES (123, CAST(987 AS BIGINT)), (456, CAST(654 AS BIGINT))");

        assertUpdate(format("CALL system.rollback_to_snapshot('tpch', 'test_rollback', %s)", afterFirstInsertId));
        assertQuery("SELECT * FROM test_rollback ORDER BY col0", "VALUES (123, CAST(987 AS BIGINT))");

        assertUpdate(format("CALL system.rollback_to_snapshot('tpch', 'test_rollback', %s)", afterCreateTableId));
        assertEquals((long) computeActual("SELECT COUNT(*) FROM test_rollback").getOnlyValue(), 0);

        dropTable("test_rollback");
    }

    private long getLatestSnapshotId(String tableName)
    {
        return (long) computeActual(format("SELECT snapshot_id FROM \"%s$snapshots\" ORDER BY committed_at DESC LIMIT 1", tableName))
                .getOnlyValue();
    }

    @Test
    public void testInsertIntoNotNullColumn()
    {
        assertUpdate("CREATE TABLE test_not_null_table (c1 INTEGER, c2 INTEGER NOT NULL)");
        assertUpdate("INSERT INTO test_not_null_table (c2) VALUES (2)", 1);
        assertQuery("SELECT * FROM test_not_null_table", "VALUES (NULL, 2)");
        assertQueryFails("INSERT INTO test_not_null_table (c1) VALUES (1)", "NULL value not allowed for NOT NULL column: c2");
        dropTable("test_not_null_table");

        assertUpdate("CREATE TABLE test_commuted_not_null_table (a BIGINT, b BIGINT NOT NULL)");
        assertUpdate("INSERT INTO test_commuted_not_null_table (b) VALUES (2)", 1);
        assertQuery("SELECT * FROM test_commuted_not_null_table", "VALUES (NULL, 2)");
        assertQueryFails("INSERT INTO test_commuted_not_null_table (b, a) VALUES (NULL, 3)", "NULL value not allowed for NOT NULL column: b");
        dropTable("test_commuted_not_null_table");
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
        dropTable("test_schema_evolution_drop_end");

        assertUpdate("CREATE TABLE test_schema_evolution_drop_middle (col0 INTEGER, col1 INTEGER, col2 INTEGER)");
        assertUpdate("INSERT INTO test_schema_evolution_drop_middle VALUES (0, 1, 2)", 1);
        assertQuery("SELECT * FROM test_schema_evolution_drop_middle", "VALUES(0, 1, 2)");
        assertUpdate("ALTER TABLE test_schema_evolution_drop_middle DROP COLUMN col1");
        assertQuery("SELECT * FROM test_schema_evolution_drop_middle", "VALUES(0, 2)");
        assertUpdate("ALTER TABLE test_schema_evolution_drop_middle ADD COLUMN col1 INTEGER");
        assertUpdate("INSERT INTO test_schema_evolution_drop_middle VALUES (3, 4, 5)", 1);
        assertQuery("SELECT * FROM test_schema_evolution_drop_middle", "VALUES(0, 2, NULL), (3, 4, 5)");
        dropTable("test_schema_evolution_drop_middle");
    }

    @Test
    public void testLargeInFailureOnPartitionedColumns()
    {
        assertUpdate("CREATE TABLE test_large_in_failure (col1 BIGINT, col2 BIGINT) WITH (partitioning = ARRAY['col2'])");
        assertUpdate("INSERT INTO test_large_in_failure VALUES (1, 10)", 1L);
        assertUpdate("INSERT INTO test_large_in_failure VALUES (2, 20)", 1L);

        List<String> predicates = IntStream.range(0, 25_000).boxed()
                .map(Object::toString)
                .collect(toImmutableList());

        String filter = format("col2 IN (%s)", String.join(",", predicates));
        assertThatThrownBy(() -> getQueryRunner().execute(format("SELECT * FROM test_large_in_failure WHERE %s", filter)))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("java.lang.StackOverflowError");

        dropTable("test_large_in_failure");
    }

    @Test
    public void testCreateTableLike()
    {
        FileFormat otherFormat = format == PARQUET ? ORC : PARQUET;
        testCreateTableLikeForFormat(otherFormat);
    }

    private void testCreateTableLikeForFormat(FileFormat otherFormat)
    {
        assertUpdate(format("CREATE TABLE test_create_table_like_original (col1 INTEGER, aDate DATE) WITH(format = '%s', partitioning = ARRAY['aDate'])", format));
        assertEquals(getTablePropertiesString("test_create_table_like_original"), "WITH (\n" +
                format("   format = '%s',\n", format) +
                "   partitioning = ARRAY['adate']\n" +
                ")");

        assertUpdate("CREATE TABLE test_create_table_like_copy0 (LIKE test_create_table_like_original, col2 INTEGER)");
        assertUpdate("INSERT INTO test_create_table_like_copy0 (col1, aDate, col2) VALUES (1, CAST('1950-06-28' AS DATE), 3)", 1);
        assertQuery("SELECT * from test_create_table_like_copy0", "VALUES(1, CAST('1950-06-28' AS DATE), 3)");
        dropTable("test_create_table_like_copy0");

        assertUpdate("CREATE TABLE test_create_table_like_copy1 (LIKE test_create_table_like_original)");
        assertEquals(getTablePropertiesString("test_create_table_like_copy1"), "WITH (\n" +
                format("   format = '%s'\n)", format));
        dropTable("test_create_table_like_copy1");

        assertUpdate("CREATE TABLE test_create_table_like_copy2 (LIKE test_create_table_like_original EXCLUDING PROPERTIES)");
        assertEquals(getTablePropertiesString("test_create_table_like_copy2"), "WITH (\n" +
                format("   format = '%s'\n)", format));
        dropTable("test_create_table_like_copy2");

        assertUpdate("CREATE TABLE test_create_table_like_copy3 (LIKE test_create_table_like_original INCLUDING PROPERTIES)");
        assertEquals(getTablePropertiesString("test_create_table_like_copy3"), "WITH (\n" +
                format("   format = '%s',\n", format) +
                "   partitioning = ARRAY['adate']\n" +
                ")");
        dropTable("test_create_table_like_copy3");

        assertUpdate(format("CREATE TABLE test_create_table_like_copy4 (LIKE test_create_table_like_original INCLUDING PROPERTIES) WITH (format = '%s')", otherFormat));
        assertEquals(getTablePropertiesString("test_create_table_like_copy4"), "WITH (\n" +
                format("   format = '%s',\n", otherFormat) +
                "   partitioning = ARRAY['adate']\n" +
                ")");
        dropTable("test_create_table_like_copy4");

        dropTable("test_create_table_like_original");
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
        dropTable("test_predicating_on_real");
    }

    @Test
    public void testHourTransform()
    {
        assertUpdate("CREATE TABLE test_hour_transform (d TIMESTAMP(6), b BIGINT) WITH (partitioning = ARRAY['hour(d)'])");

        @Language("SQL") String values = "VALUES " +
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
        assertUpdate("INSERT INTO test_hour_transform " + values, 11);
        assertQuery("SELECT * FROM test_hour_transform", values);

        @Language("SQL") String expected = "VALUES " +
                "(-2, 1, TIMESTAMP '1969-12-31 22:22:22.222222', TIMESTAMP '1969-12-31 22:22:22.222222', 8, 8), " +
                "(-1, 2, TIMESTAMP '1969-12-31 23:33:11.456789', TIMESTAMP '1969-12-31 23:44:55.567890', 9, 10), " +
                "(0, 1, TIMESTAMP '1970-01-01 00:55:44.765432', TIMESTAMP '1970-01-01 00:55:44.765432', 11, 11), " +
                "(394474, 3, TIMESTAMP '2015-01-01 10:01:23.123456', TIMESTAMP '2015-01-01 10:55:00.456789', 1, 3), " +
                "(397692, 2, TIMESTAMP '2015-05-15 12:05:01.234567', TIMESTAMP '2015-05-15 12:21:02.345678', 4, 5), " +
                "(439525, 2, TIMESTAMP '2020-02-21 13:11:11.876543', TIMESTAMP '2020-02-21 13:12:12.654321', 6, 7)";
        if (format == ORC) {
            expected = "VALUES " +
                    "(-2, 1, NULL, NULL, 8, 8), " +
                    "(-1, 2, NULL, NULL, 9, 10), " +
                    "(0, 1, NULL, NULL, 11, 11), " +
                    "(394474, 3, NULL, NULL, 1, 3), " +
                    "(397692, 2, NULL, NULL, 4, 5), " +
                    "(439525, 2, NULL, NULL, 6, 7)";
        }

        assertQuery("SELECT d_hour, row_count, d.min, d.max, b.min, b.max FROM \"test_hour_transform$partitions\"", expected);

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertQuery(
                "SELECT * FROM test_hour_transform WHERE day_of_week(d) = 3 AND b % 7 = 3",
                "VALUES (TIMESTAMP '1969-12-31 23:44:55.567890', 10)");

        assertThat(query("SHOW STATS FOR test_hour_transform"))
                .projected(0, 2, 3, 4, 5, 6) // ignore data size which is available for Parquet, but not for ORC
                .skippingTypesCheck()
                .matches("VALUES " +
                        "  ('d', NULL, 0e0, NULL, " + (format == ORC ? "NULL, NULL" : "'1969-12-31 22:22:22.222222', '2020-02-21 13:12:12.654321'") + "), " +
                        "  ('b', NULL, 0e0, NULL, '1', '11'), " +
                        "  (NULL, NULL, NULL, 11e0, NULL, NULL)");

        dropTable("test_hour_transform");
    }

    @Test
    public void testDayTransformDate()
    {
        assertUpdate("CREATE TABLE test_day_transform_date (d DATE, b BIGINT) WITH (partitioning = ARRAY['day(d)'])");

        @Language("SQL") String values = "VALUES " +
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
        assertUpdate("INSERT INTO test_day_transform_date " + values, 11);
        assertQuery("SELECT * FROM test_day_transform_date", values);

        assertQuery(
                "SELECT d_day, row_count, d.min, d.max, b.min, b.max FROM \"test_day_transform_date$partitions\"",
                "VALUES " +
                        "(DATE '1969-01-01', 1, DATE '1969-01-01', DATE '1969-01-01', 10, 10), " +
                        "(DATE '1969-12-31', 1, DATE '1969-12-31', DATE '1969-12-31', 11, 11), " +
                        "(DATE '1970-01-01', 1, DATE '1970-01-01', DATE '1970-01-01', 1, 1), " +
                        "(DATE '1970-03-04', 1, DATE '1970-03-04', DATE '1970-03-04', 2, 2), " +
                        "(DATE '2015-01-01', 1, DATE '2015-01-01', DATE '2015-01-01', 3, 3), " +
                        "(DATE '2015-01-13', 2, DATE '2015-01-13', DATE '2015-01-13', 4, 5), " +
                        "(DATE '2015-05-15', 2, DATE '2015-05-15', DATE '2015-05-15', 6, 7), " +
                        "(DATE '2020-02-21', 2, DATE '2020-02-21', DATE '2020-02-21', 8, 9)");

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertQuery(
                "SELECT * FROM test_day_transform_date WHERE day_of_week(d) = 3 AND b % 7 = 3",
                "VALUES (DATE '1969-01-01', 10)");

        assertThat(query("SHOW STATS FOR test_day_transform_date"))
                .projected(0, 2, 3, 4, 5, 6) // ignore data size which is available for Parquet, but not for ORC
                .skippingTypesCheck()
                .matches("VALUES " +
                        "  ('d', NULL, 0e0, NULL, '1969-01-01', '2020-02-21'), " +
                        "  ('b', NULL, 0e0, NULL, '1', '11'), " +
                        "  (NULL, NULL, NULL, 11e0, NULL, NULL)");

        dropTable("test_day_transform_date");
    }

    @Test
    public void testDayTransformTimestamp()
    {
        assertUpdate("CREATE TABLE test_day_transform_timestamp (d TIMESTAMP(6), b BIGINT) WITH (partitioning = ARRAY['day(d)'])");

        @Language("SQL") String values = "VALUES " +
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
        assertUpdate("INSERT INTO test_day_transform_timestamp " + values, 12);
        assertQuery("SELECT * FROM test_day_transform_timestamp", values);

        @Language("SQL") String expected = "VALUES " +
                "(DATE '1969-12-25', 1, TIMESTAMP '1969-12-25 15:13:12.876543', TIMESTAMP '1969-12-25 15:13:12.876543', 8, 8), " +
                "(DATE '1969-12-30', 1, TIMESTAMP '1969-12-30 18:47:33.345678', TIMESTAMP '1969-12-30 18:47:33.345678', 9, 9), " +
                "(DATE '1969-12-31', 2, TIMESTAMP '1969-12-31 00:00:00.000000', TIMESTAMP '1969-12-31 05:06:07.234567', 10, 11), " +
                "(DATE '1970-01-01', 1, TIMESTAMP '1970-01-01 12:03:08.456789', TIMESTAMP '1970-01-01 12:03:08.456789', 12, 12), " +
                "(DATE '2015-01-01', 3, TIMESTAMP '2015-01-01 10:01:23.123456', TIMESTAMP '2015-01-01 12:55:00.456789', 1, 3), " +
                "(DATE '2015-05-15', 2, TIMESTAMP '2015-05-15 13:05:01.234567', TIMESTAMP '2015-05-15 14:21:02.345678', 4, 5), " +
                "(DATE '2020-02-21', 2, TIMESTAMP '2020-02-21 15:11:11.876543', TIMESTAMP '2020-02-21 16:12:12.654321', 6, 7)";
        if (format == ORC) {
            // Parquet has min/max for timestamps but ORC does not.
            expected = "VALUES " +
                    "(DATE '1969-12-25', 1, NULL, NULL, 8, 8), " +
                    "(DATE '1969-12-30', 1, NULL, NULL, 9, 9), " +
                    "(DATE '1969-12-31', 2, NULL, NULL, 10, 11), " +
                    "(DATE '1970-01-01', 1, NULL, NULL, 12, 12), " +
                    "(DATE '2015-01-01', 3, NULL, NULL, 1, 3), " +
                    "(DATE '2015-05-15', 2, NULL, NULL, 4, 5), " +
                    "(DATE '2020-02-21', 2, NULL, NULL, 6, 7)";
        }

        assertQuery("SELECT d_day, row_count, d.min, d.max, b.min, b.max FROM \"test_day_transform_timestamp$partitions\"", expected);

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertQuery(
                "SELECT * FROM test_day_transform_timestamp WHERE day_of_week(d) = 3 AND b % 7 = 3",
                "VALUES (TIMESTAMP '1969-12-31 00:00:00.000000', 10)");

        assertThat(query("SHOW STATS FOR test_day_transform_timestamp"))
                .projected(0, 2, 3, 4, 5, 6) // ignore data size which is available for Parquet, but not for ORC
                .skippingTypesCheck()
                .matches("VALUES " +
                        "  ('d', NULL, 0e0, NULL, " + (format == ORC ? "NULL, NULL" : "'1969-12-25 15:13:12.876543', '2020-02-21 16:12:12.654321'") + "), " +
                        "  ('b', NULL, 0e0, NULL, '1', '12'), " +
                        "  (NULL, NULL, NULL, 12e0, NULL, NULL)");

        dropTable("test_day_transform_timestamp");
    }

    @Test
    public void testMonthTransformDate()
    {
        assertUpdate("CREATE TABLE test_month_transform_date (d DATE, b BIGINT) WITH (partitioning = ARRAY['month(d)'])");

        @Language("SQL") String values = "VALUES " +
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
        assertUpdate("INSERT INTO test_month_transform_date " + values, 14);
        assertQuery("SELECT * FROM test_month_transform_date", values);

        assertQuery(
                "SELECT d_month, row_count, d.min, d.max, b.min, b.max FROM \"test_month_transform_date$partitions\"",
                "VALUES " +
                        "(-2, 1, DATE '1969-11-13', DATE '1969-11-13', 1, 1), " +
                        "(-1, 3, DATE '1969-12-01', DATE '1969-12-31', 2, 4), " +
                        "(0, 1, DATE '1970-01-01', DATE '1970-01-01', 5, 5), " +
                        "(4, 1, DATE '1970-05-13', DATE '1970-05-13', 6, 6), " +
                        "(11, 1, DATE '1970-12-31', DATE '1970-12-31', 7, 7), " +
                        "(600, 1, DATE '2020-01-01', DATE '2020-01-01', 8, 8), " +
                        "(605, 3, DATE '2020-06-06', DATE '2020-06-28', 9, 11), " +
                        "(606, 2, DATE '2020-07-18', DATE '2020-07-28', 12, 13), " +
                        "(611, 1, DATE '2020-12-31', DATE '2020-12-31', 14, 14)");

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertQuery(
                "SELECT * FROM test_month_transform_date WHERE day_of_week(d) = 7 AND b % 7 = 3",
                "VALUES (DATE '2020-06-28', 10)");

        assertThat(query("SHOW STATS FOR test_month_transform_date"))
                .projected(0, 2, 3, 4, 5, 6) // ignore data size which is available for Parquet, but not for ORC
                .skippingTypesCheck()
                .matches("VALUES " +
                        "  ('d', NULL, 0e0, NULL, '1969-11-13', '2020-12-31'), " +
                        "  ('b', NULL, 0e0, NULL, '1', '14'), " +
                        "  (NULL, NULL, NULL, 14e0, NULL, NULL)");

        dropTable("test_month_transform_date");
    }

    @Test
    public void testMonthTransformTimestamp()
    {
        assertUpdate("CREATE TABLE test_month_transform_timestamp (d TIMESTAMP(6), b BIGINT) WITH (partitioning = ARRAY['month(d)'])");

        @Language("SQL") String values = "VALUES " +
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
        assertUpdate("INSERT INTO test_month_transform_timestamp " + values, 12);
        assertQuery("SELECT * FROM test_month_transform_timestamp", values);

        @Language("SQL") String expected = "VALUES " +
                "(-2, 2, TIMESTAMP '1969-11-15 15:13:12.876543', TIMESTAMP '1969-11-19 18:47:33.345678', 8, 9), " +
                "(-1, 2, TIMESTAMP '1969-12-01 00:00:00.000000', TIMESTAMP '1969-12-01 05:06:07.234567', 10, 11), " +
                "(0, 1, TIMESTAMP '1970-01-01 12:03:08.456789', TIMESTAMP '1970-01-01 12:03:08.456789', 12, 12), " +
                "(540, 3, TIMESTAMP '2015-01-01 10:01:23.123456', TIMESTAMP '2015-01-01 12:55:00.456789', 1, 3), " +
                "(544, 2, TIMESTAMP '2015-05-15 13:05:01.234567', TIMESTAMP '2015-05-15 14:21:02.345678', 4, 5), " +
                "(601, 2, TIMESTAMP '2020-02-21 15:11:11.876543', TIMESTAMP '2020-02-21 16:12:12.654321', 6, 7)";
        if (format == ORC) {
            expected = "VALUES " +
                    "(-2, 2, NULL, NULL, 8, 9), " +
                    "(-1, 2, NULL, NULL, 10, 11), " +
                    "(0, 1, NULL, NULL, 12, 12), " +
                    "(540, 3, NULL, NULL, 1, 3), " +
                    "(544, 2, NULL, NULL, 4, 5), " +
                    "(601, 2, NULL, NULL, 6, 7)";
        }

        assertQuery("SELECT d_month, row_count, d.min, d.max, b.min, b.max FROM \"test_month_transform_timestamp$partitions\"", expected);

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertQuery(
                "SELECT * FROM test_month_transform_timestamp WHERE day_of_week(d) = 1 AND b % 7 = 3",
                "VALUES (TIMESTAMP '1969-12-01 00:00:00.000000', 10)");

        assertThat(query("SHOW STATS FOR test_month_transform_timestamp"))
                .projected(0, 2, 3, 4, 5, 6) // ignore data size which is available for Parquet, but not for ORC
                .skippingTypesCheck()
                .matches("VALUES " +
                        "  ('d', NULL, 0e0, NULL, " + (format == ORC ? "NULL, NULL" : "'1969-11-15 15:13:12.876543', '2020-02-21 16:12:12.654321'") + "), " +
                        "  ('b', NULL, 0e0, NULL, '1', '12'), " +
                        "  (NULL, NULL, NULL, 12e0, NULL, NULL)");

        dropTable("test_month_transform_timestamp");
    }

    @Test
    public void testYearTransformDate()
    {
        assertUpdate("CREATE TABLE test_year_transform_date (d DATE, b BIGINT) WITH (partitioning = ARRAY['year(d)'])");

        @Language("SQL") String values = "VALUES " +
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
        assertUpdate("INSERT INTO test_year_transform_date " + values, 12);
        assertQuery("SELECT * FROM test_year_transform_date", values);

        assertQuery(
                "SELECT d_year, row_count, d.min, d.max, b.min, b.max FROM \"test_year_transform_date$partitions\"",
                "VALUES " +
                        "(-2, 1, DATE '1968-10-13', DATE '1968-10-13', 1, 1), " +
                        "(-1, 2, DATE '1969-01-01', DATE '1969-03-15', 2, 3), " +
                        "(0, 2, DATE '1970-01-01', DATE '1970-03-05', 4, 5), " +
                        "(45, 3, DATE '2015-01-01', DATE '2015-07-28', 6, 8), " +
                        "(46, 2, DATE '2016-05-15', DATE '2016-06-06', 9, 10), " +
                        "(50, 2, DATE '2020-02-21', DATE '2020-11-10', 11, 12)");

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertQuery(
                "SELECT * FROM test_year_transform_date WHERE day_of_week(d) = 1 AND b % 7 = 3",
                "VALUES (DATE '2016-06-06', 10)");

        assertThat(query("SHOW STATS FOR test_year_transform_date"))
                .projected(0, 2, 3, 4, 5, 6) // ignore data size which is available for Parquet, but not for ORC
                .skippingTypesCheck()
                .matches("VALUES " +
                        "  ('d', NULL, 0e0, NULL, '1968-10-13', '2020-11-10'), " +
                        "  ('b', NULL, 0e0, NULL, '1', '12'), " +
                        "  (NULL, NULL, NULL, 12e0, NULL, NULL)");

        dropTable("test_year_transform_date");
    }

    @Test
    public void testYearTransformTimestamp()
    {
        assertUpdate("CREATE TABLE test_year_transform_timestamp (d TIMESTAMP(6), b BIGINT) WITH (partitioning = ARRAY['year(d)'])");

        @Language("SQL") String values = "VALUES " +
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
        assertUpdate("INSERT INTO test_year_transform_timestamp " + values, 12);
        assertQuery("SELECT * FROM test_year_transform_timestamp", values);

        @Language("SQL") String expected = "VALUES " +
                "(-2, 2, TIMESTAMP '1968-03-15 15:13:12.876543', TIMESTAMP '1968-11-19 18:47:33.345678', 1, 2), " +
                "(-1, 2, TIMESTAMP '1969-01-01 00:00:00.000000', TIMESTAMP '1969-01-01 05:06:07.234567', 3, 4), " +
                "(0, 4, TIMESTAMP '1970-01-18 12:03:08.456789', TIMESTAMP '1970-12-31 12:55:00.456789', 5, 8), " +
                "(45, 2, TIMESTAMP '2015-05-15 13:05:01.234567', TIMESTAMP '2015-09-15 14:21:02.345678', 9, 10), " +
                "(50, 2, TIMESTAMP '2020-02-21 15:11:11.876543', TIMESTAMP '2020-08-21 16:12:12.654321', 11, 12)";
        if (format == ORC) {
            expected = "VALUES " +
                    "(-2, 2, NULL, NULL, 1, 2), " +
                    "(-1, 2, NULL, NULL, 3, 4), " +
                    "(0, 4, NULL, NULL, 5, 8), " +
                    "(45, 2, NULL, NULL, 9, 10), " +
                    "(50, 2, NULL, NULL, 11, 12)";
        }

        assertQuery("SELECT d_year, row_count, d.min, d.max, b.min, b.max FROM \"test_year_transform_timestamp$partitions\"", expected);

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertQuery(
                "SELECT * FROM test_year_transform_timestamp WHERE day_of_week(d) = 2 AND b % 7 = 3",
                "VALUES (TIMESTAMP '2015-09-15 14:21:02.345678', 10)");

        assertThat(query("SHOW STATS FOR test_year_transform_timestamp"))
                .projected(0, 2, 3, 4, 5, 6) // ignore data size which is available for Parquet, but not for ORC
                .skippingTypesCheck()
                .matches("VALUES " +
                        "  ('d', NULL, 0e0, NULL, " + (format == ORC ? "NULL, NULL" : "'1968-03-15 15:13:12.876543', '2020-08-21 16:12:12.654321'") + "), " +
                        "  ('b', NULL, 0e0, NULL, '1', '12'), " +
                        "  (NULL, NULL, NULL, 12e0, NULL, NULL)");

        dropTable("test_year_transform_timestamp");
    }

    @Test
    public void testTruncateTextTransform()
    {
        assertUpdate("CREATE TABLE test_truncate_text_transform (d VARCHAR, b BIGINT) WITH (partitioning = ARRAY['truncate(d, 2)'])");
        String select = "SELECT d_trunc, row_count, d.min AS d_min, d.max AS d_max, b.min AS b_min, b.max AS b_max FROM \"test_truncate_text_transform$partitions\"";

        assertUpdate("INSERT INTO test_truncate_text_transform VALUES" +
                "('abcd', 1)," +
                "('abxy', 2)," +
                "('ab598', 3)," +
                "('mommy', 4)," +
                "('moscow', 5)," +
                "('Greece', 6)," +
                "('Grozny', 7)", 7);

        assertQuery("SELECT d_trunc FROM \"test_truncate_text_transform$partitions\"", "VALUES 'ab', 'mo', 'Gr'");

        assertQuery("SELECT b FROM test_truncate_text_transform WHERE substring(d, 1, 2) = 'ab'", "VALUES 1, 2, 3");
        assertQuery(select + " WHERE d_trunc = 'ab'", "VALUES ('ab', 3, 'ab598', 'abxy', 1, 3)");

        assertQuery("SELECT b FROM test_truncate_text_transform WHERE substring(d, 1, 2) = 'mo'", "VALUES 4, 5");
        assertQuery(select + " WHERE d_trunc = 'mo'", "VALUES ('mo', 2, 'mommy', 'moscow', 4, 5)");

        assertQuery("SELECT b FROM test_truncate_text_transform WHERE substring(d, 1, 2) = 'Gr'", "VALUES 6, 7");
        assertQuery(select + " WHERE d_trunc = 'Gr'", "VALUES ('Gr', 2, 'Greece', 'Grozny', 6, 7)");

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertQuery(
                "SELECT * FROM test_truncate_text_transform WHERE length(d) = 4 AND b % 7 = 2",
                "VALUES ('abxy', 2)");

        assertThat(query("SHOW STATS FOR test_truncate_text_transform"))
                .projected(0, 2, 3, 4, 5, 6) // ignore data size which is available for Parquet, but not for ORC
                .skippingTypesCheck()
                .matches("VALUES " +
                        "  ('d', NULL, 0e0, NULL, NULL, NULL), " +
                        "  ('b', NULL, 0e0, NULL, '1', '7'), " +
                        "  (NULL, NULL, NULL, 7e0, NULL, NULL)");

        dropTable("test_truncate_text_transform");
    }

    @Test(dataProvider = "truncateNumberTypesProvider")
    public void testTruncateIntegerTransform(String dataType)
    {
        String table = format("test_truncate_%s_transform", dataType);
        assertUpdate(format("CREATE TABLE " + table + " (d %s, b BIGINT) WITH (partitioning = ARRAY['truncate(d, 10)'])", dataType));
        String select = "SELECT d_trunc, row_count, d.min AS d_min, d.max AS d_max, b.min AS b_min, b.max AS b_max FROM \"" + table + "$partitions\"";

        assertUpdate("INSERT INTO " + table + " VALUES" +
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
                "(-130, 15)", 15);

        assertQuery("SELECT d_trunc FROM \"" + table + "$partitions\"", "VALUES 0, 10, 120, -10, -20, -130");

        assertQuery("SELECT b FROM " + table + " WHERE d IN (0, 1, 5, 9)", "VALUES 1, 2, 3, 4");
        assertQuery(select + " WHERE d_trunc = 0", "VALUES (0, 4, 0, 9, 1, 4)");

        assertQuery("SELECT b FROM " + table + " WHERE d IN (10, 11)", "VALUES 5, 6");
        assertQuery(select + " WHERE d_trunc = 10", "VALUES (10, 2, 10, 11, 5, 6)");

        assertQuery("SELECT b FROM " + table + " WHERE d IN (120, 121, 123)", "VALUES 7, 8, 9");
        assertQuery(select + " WHERE d_trunc = 120", "VALUES (120, 3, 120, 123, 7, 9)");

        assertQuery("SELECT b FROM " + table + " WHERE d IN (-1, -5, -10)", "VALUES 10, 11, 12");
        assertQuery(select + " WHERE d_trunc = -10", "VALUES (-10, 3, -10, -1, 10, 12)");

        assertQuery("SELECT b FROM " + table + " WHERE d = -11", "VALUES 13");
        assertQuery(select + " WHERE d_trunc = -20", "VALUES (-20, 1, -11, -11, 13, 13)");

        assertQuery("SELECT b FROM " + table + " WHERE d IN (-123, -130)", "VALUES 14, 15");
        assertQuery(select + " WHERE d_trunc = -130", "VALUES (-130, 2, -130, -123, 14, 15)");

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertQuery(
                "SELECT * FROM " + table + " WHERE d % 10 = -1 AND b % 7 = 3",
                "VALUES (-1, 10)");

        assertThat(query("SHOW STATS FOR " + table))
                .projected(0, 2, 3, 4, 5, 6) // ignore data size which is available for Parquet, but not for ORC
                .skippingTypesCheck()
                .matches("VALUES " +
                        "  ('d', NULL, 0e0, NULL, '-130', '123'), " +
                        "  ('b', NULL, 0e0, NULL, '1', '15'), " +
                        "  (NULL, NULL, NULL, 15e0, NULL, NULL)");

        dropTable(table);
    }

    @DataProvider
    public Object[][] truncateNumberTypesProvider()
    {
        return new Object[][] {
                {"integer"},
                {"bigint"},
        };
    }

    @Test
    public void testTruncateDecimalTransform()
    {
        assertUpdate("CREATE TABLE test_truncate_decimal_transform (d DECIMAL(9, 2), b BIGINT) WITH (partitioning = ARRAY['truncate(d, 10)'])");
        String select = "SELECT d_trunc, row_count, d.min AS d_min, d.max AS d_max, b.min AS b_min, b.max AS b_max FROM \"test_truncate_decimal_transform$partitions\"";

        assertUpdate("INSERT INTO test_truncate_decimal_transform VALUES" +
                "(12.34, 1)," +
                "(12.30, 2)," +
                "(12.29, 3)," +
                "(0.05, 4)," +
                "(-0.05, 5)", 5);

        assertQuery("SELECT d_trunc FROM \"test_truncate_decimal_transform$partitions\"", "VALUES 12.30, 12.20, 0.00, -0.10");

        assertQuery("SELECT b FROM test_truncate_decimal_transform WHERE d IN (12.34, 12.30)", "VALUES 1, 2");
        assertQuery(select + " WHERE d_trunc = 12.30", "VALUES (12.30, 2, 12.30, 12.34, 1, 2)");

        assertQuery("SELECT b FROM test_truncate_decimal_transform WHERE d = 12.29", "VALUES 3");
        assertQuery(select + " WHERE d_trunc = 12.20", "VALUES (12.20, 1, 12.29, 12.29, 3, 3)");

        assertQuery("SELECT b FROM test_truncate_decimal_transform WHERE d = 0.05", "VALUES 4");
        assertQuery(select + " WHERE d_trunc = 0.00", "VALUES (0.00, 1, 0.05, 0.05, 4, 4)");

        assertQuery("SELECT b FROM test_truncate_decimal_transform WHERE d = -0.05", "VALUES 5");
        assertQuery(select + " WHERE d_trunc = -0.10", "VALUES (-0.10, 1, -0.05, -0.05, 5, 5)");

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertQuery(
                "SELECT * FROM test_truncate_decimal_transform WHERE d * 100 % 10 = 9 AND b % 7 = 3",
                "VALUES (12.29, 3)");

        assertThat(query("SHOW STATS FOR test_truncate_decimal_transform"))
                .projected(0, 2, 3, 4, 5, 6) // ignore data size which is available for Parquet, but not for ORC
                .skippingTypesCheck()
                .matches("VALUES " +
                        "  ('d', NULL, 0e0, NULL, '-0.05', '12.34'), " +
                        "  ('b', NULL, 0e0, NULL, '1', '5'), " +
                        "  (NULL, NULL, NULL, 5e0, NULL, NULL)");

        dropTable("test_truncate_decimal_transform");
    }

    @Test
    public void testBucketTransform()
    {
        String select = "SELECT d_bucket, row_count, d.min AS d_min, d.max AS d_max, b.min AS b_min, b.max AS b_max FROM \"test_bucket_transform$partitions\"";

        assertUpdate("CREATE TABLE test_bucket_transform (d VARCHAR, b BIGINT) WITH (partitioning = ARRAY['bucket(d, 2)'])");
        assertUpdate(
                "INSERT INTO test_bucket_transform VALUES" +
                        "('abcd', 1)," +
                        "('abxy', 2)," +
                        "('ab598', 3)," +
                        "('mommy', 4)," +
                        "('moscow', 5)," +
                        "('Greece', 6)," +
                        "('Grozny', 7)",
                7);

        assertQuery("SELECT COUNT(*) FROM \"test_bucket_transform$partitions\"", "SELECT 2");

        assertQuery(select + " WHERE d_bucket = 0", "VALUES(0, 3, 'Grozny', 'mommy', 1, 7)");

        assertQuery(select + " WHERE d_bucket = 1", "VALUES(1, 4, 'Greece', 'moscow', 2, 6)");

        // Exercise IcebergMetadata.applyFilter with non-empty Constraint.predicate, via non-pushdownable predicates
        assertQuery(
                "SELECT * FROM test_bucket_transform WHERE length(d) = 4 AND b % 7 = 2",
                "VALUES ('abxy', 2)");

        assertThat(query("SHOW STATS FOR test_bucket_transform"))
                .projected(0, 2, 3, 4, 5, 6) // ignore data size which is available for Parquet, but not for ORC
                .skippingTypesCheck()
                .matches("VALUES " +
                        "  ('d', NULL, 0e0, NULL, NULL, NULL), " +
                        "  ('b', NULL, 0e0, NULL, '1', '7'), " +
                        "  (NULL, NULL, NULL, 7e0, NULL, NULL)");

        dropTable("test_bucket_transform");
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
        assertQuery(
                "SELECT d_null, row_count, file_count, d.min, d.max, d.null_count, b.min, b.max, b.null_count FROM \"test_void_transform$partitions\"",
                "VALUES (NULL, 7, 1, 'Warsaw', 'mommy', 2, 1, 7, 0)");

        assertQuery(
                "SELECT d, b FROM test_void_transform WHERE d IS NOT NULL",
                "VALUES " +
                        "('abcd', 1)," +
                        "('abxy', 2)," +
                        "('ab598', 3)," +
                        "('mommy', 4)," +
                        "('Warsaw', 5)");

        assertQuery("SELECT b FROM test_void_transform WHERE d IS NULL", "VALUES 6, 7");

        assertThat(query("SHOW STATS FOR test_void_transform"))
                .projected(0, 2, 3, 4, 5, 6) // ignore data size which is available for Parquet, but not for ORC
                .skippingTypesCheck()
                .matches("VALUES " +
                        "  ('d', NULL, 0.2857142857142857, NULL, NULL, NULL), " +
                        "  ('b', NULL, 0e0, NULL, '1', '7'), " +
                        "  (NULL, NULL, NULL, 7e0, NULL, NULL)");

        assertUpdate("DROP TABLE " + "test_void_transform");
    }

    @Test
    public void testMetadataDeleteSimple()
    {
        assertUpdate("CREATE TABLE test_metadata_delete_simple (col1 BIGINT, col2 BIGINT) WITH (partitioning = ARRAY['col1'])");
        assertUpdate("INSERT INTO test_metadata_delete_simple VALUES(1, 100), (1, 101), (1, 102), (2, 200), (2, 201), (3, 300)", 6);
        assertQueryFails(
                "DELETE FROM test_metadata_delete_simple WHERE col1 = 1 AND col2 > 101",
                "This connector only supports delete where one or more identity-transformed partitions are deleted entirely");
        assertQuery("SELECT sum(col2) FROM test_metadata_delete_simple", "SELECT 1004");
        assertQuery("SELECT count(*) FROM \"test_metadata_delete_simple$partitions\"", "SELECT 3");
        assertUpdate("DELETE FROM test_metadata_delete_simple WHERE col1 = 1");
        assertQuery("SELECT sum(col2) FROM test_metadata_delete_simple", "SELECT 701");
        assertQuery("SELECT count(*) FROM \"test_metadata_delete_simple$partitions\"", "SELECT 2");
        dropTable("test_metadata_delete_simple");
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

        assertUpdate("DELETE FROM test_metadata_delete WHERE linestatus = 'F' AND linenumber = 3");
        assertQuery("SELECT * FROM test_metadata_delete", "SELECT orderkey, linenumber, linestatus FROM lineitem WHERE linestatus <> 'F' or linenumber <> 3");
        assertQuery("SELECT count(*) FROM \"test_metadata_delete$partitions\"", "SELECT 13");

        assertUpdate("DELETE FROM test_metadata_delete WHERE linestatus='O'");
        assertQuery("SELECT count(*) FROM \"test_metadata_delete$partitions\"", "SELECT 6");
        assertQuery("SELECT * FROM test_metadata_delete", "SELECT orderkey, linenumber, linestatus FROM lineitem WHERE linestatus <> 'O' AND linenumber <> 3");

        assertQueryFails("DELETE FROM test_metadata_delete WHERE orderkey=1", "This connector only supports delete where one or more identity-transformed partitions are deleted entirely");

        dropTable("test_metadata_delete");
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
        dropTable("test_in_set");
    }

    @Test
    public void testBasicTableStatistics()
    {
        String tableName = "test_basic_table_statistics";
        assertUpdate(format("CREATE TABLE %s (col REAL)", tableName));
        String insertStart = format("INSERT INTO %s", tableName);
        assertUpdate(insertStart + " VALUES -10", 1);
        assertUpdate(insertStart + " VALUES 100", 1);

        // SHOW STATS returns rows of the form: column_name, data_size, distinct_values_count, nulls_fractions, row_count, low_value, high_value

        MaterializedResult result = computeActual("SHOW STATS FOR " + tableName);
        MaterializedResult expectedStatistics =
                resultBuilder(getSession(), VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("col", null, null, 0.0, null, "-10.0", "100.0")
                        .row(null, null, null, null, 2.0, null, null)
                        .build();
        assertEquals(result, expectedStatistics);

        assertUpdate(insertStart + " VALUES 200", 1);

        result = computeActual("SHOW STATS FOR " + tableName);
        expectedStatistics =
                resultBuilder(getSession(), VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("col", null, null, 0.0, null, "-10.0", "200.0")
                        .row(null, null, null, null, 3.0, null, null)
                        .build();
        assertEquals(result, expectedStatistics);

        dropTable(tableName);
    }

    @Test
    public void testMultipleColumnTableStatistics()
    {
        String tableName = "test_multiple_table_statistics";
        assertUpdate(format("CREATE TABLE %s (col1 REAL, col2 INTEGER, col3 DATE)", tableName));
        String insertStart = format("INSERT INTO %s", tableName);
        assertUpdate(insertStart + " VALUES (-10, -1, DATE '2019-06-28')", 1);
        assertUpdate(insertStart + " VALUES (100, 10, DATE '2020-01-01')", 1);

        MaterializedResult result = computeActual("SHOW STATS FOR " + tableName);

        MaterializedResult expectedStatistics =
                resultBuilder(getSession(), VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("col1", null, null, 0.0, null, "-10.0", "100.0")
                        .row("col2", null, null, 0.0, null, "-1", "10")
                        .row("col3", null, null, 0.0, null, "2019-06-28", "2020-01-01")
                        .row(null, null, null, null, 2.0, null, null)
                        .build();
        assertEquals(result, expectedStatistics);

        assertUpdate(insertStart + " VALUES (200, 20, DATE '2020-06-28')", 1);
        result = computeActual("SHOW STATS FOR " + tableName);
        expectedStatistics =
                resultBuilder(getSession(), VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("col1", null, null, 0.0, null, "-10.0", "200.0")
                        .row("col2", null, null, 0.0, null, "-1", "20")
                        .row("col3", null, null, 0.0, null, "2019-06-28", "2020-06-28")
                        .row(null, null, null, null, 3.0, null, null)
                        .build();
        assertEquals(result, expectedStatistics);

        assertUpdate(insertStart + " VALUES " + IntStream.rangeClosed(21, 25)
                .mapToObj(i -> format("(200, %d, DATE '2020-07-%d')", i, i))
                .collect(joining(", ")), 5);

        assertUpdate(insertStart + " VALUES " + IntStream.rangeClosed(26, 30)
                .mapToObj(i -> format("(NULL, %d, DATE '2020-06-%d')", i, i))
                .collect(joining(", ")), 5);

        result = computeActual("SHOW STATS FOR " + tableName);

        expectedStatistics =
                resultBuilder(getSession(), VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("col1", null, null, 5.0 / 13.0, null, "-10.0", "200.0")
                        .row("col2", null, null, 0.0, null, "-1", "30")
                        .row("col3", null, null, 0.0, null, "2019-06-28", "2020-07-25")
                        .row(null, null, null, null, 13.0, null, null)
                        .build();
        assertEquals(result, expectedStatistics);

        dropTable(tableName);
    }

    @Test
    public void testPartitionedTableStatistics()
    {
        assertUpdate("CREATE TABLE iceberg.tpch.test_partitioned_table_statistics (col1 REAL, col2 BIGINT) WITH (partitioning = ARRAY['col2'])");

        String insertStart = "INSERT INTO test_partitioned_table_statistics";
        assertUpdate(insertStart + " VALUES (-10, -1)", 1);
        assertUpdate(insertStart + " VALUES (100, 10)", 1);

        MaterializedResult result = computeActual("SHOW STATS FOR iceberg.tpch.test_partitioned_table_statistics");
        assertEquals(result.getRowCount(), 3);

        MaterializedRow row0 = result.getMaterializedRows().get(0);
        assertEquals(row0.getField(0), "col1");
        assertEquals(row0.getField(3), 0.0);
        assertEquals(row0.getField(5), "-10.0");
        assertEquals(row0.getField(6), "100.0");

        MaterializedRow row1 = result.getMaterializedRows().get(1);
        assertEquals(row1.getField(0), "col2");
        assertEquals(row1.getField(3), 0.0);
        assertEquals(row1.getField(5), "-1");
        assertEquals(row1.getField(6), "10");

        MaterializedRow row2 = result.getMaterializedRows().get(2);
        assertEquals(row2.getField(4), 2.0);

        assertUpdate(insertStart + " VALUES " + IntStream.rangeClosed(1, 5)
                .mapToObj(i -> format("(%d, 10)", i + 100))
                .collect(joining(", ")), 5);

        assertUpdate(insertStart + " VALUES " + IntStream.rangeClosed(6, 10)
                .mapToObj(i -> "(NULL, 10)")
                .collect(joining(", ")), 5);

        result = computeActual("SHOW STATS FOR iceberg.tpch.test_partitioned_table_statistics");
        assertEquals(result.getRowCount(), 3);
        row0 = result.getMaterializedRows().get(0);
        assertEquals(row0.getField(0), "col1");
        assertEquals(row0.getField(3), 5.0 / 12.0);
        assertEquals(row0.getField(5), "-10.0");
        assertEquals(row0.getField(6), "105.0");

        row1 = result.getMaterializedRows().get(1);
        assertEquals(row1.getField(0), "col2");
        assertEquals(row1.getField(3), 0.0);
        assertEquals(row1.getField(5), "-1");
        assertEquals(row1.getField(6), "10");

        row2 = result.getMaterializedRows().get(2);
        assertEquals(row2.getField(4), 12.0);

        assertUpdate(insertStart + " VALUES " + IntStream.rangeClosed(6, 10)
                .mapToObj(i -> "(100, NULL)")
                .collect(joining(", ")), 5);

        result = computeActual("SHOW STATS FOR iceberg.tpch.test_partitioned_table_statistics");
        row0 = result.getMaterializedRows().get(0);
        assertEquals(row0.getField(0), "col1");
        assertEquals(row0.getField(3), 5.0 / 17.0);
        assertEquals(row0.getField(5), "-10.0");
        assertEquals(row0.getField(6), "105.0");

        row1 = result.getMaterializedRows().get(1);
        assertEquals(row1.getField(0), "col2");
        assertEquals(row1.getField(3), 5.0 / 17.0);
        assertEquals(row1.getField(5), "-1");
        assertEquals(row1.getField(6), "10");

        row2 = result.getMaterializedRows().get(2);
        assertEquals(row2.getField(4), 17.0);

        dropTable("iceberg.tpch.test_partitioned_table_statistics");
    }

    @Test
    public void testStatisticsConstraints()
    {
        String tableName = "iceberg.tpch.test_simple_partitioned_table_statistics";
        assertUpdate("CREATE TABLE iceberg.tpch.test_simple_partitioned_table_statistics (col1 BIGINT, col2 BIGINT) WITH (partitioning = ARRAY['col1'])");

        String insertStart = "INSERT INTO iceberg.tpch.test_simple_partitioned_table_statistics";
        assertUpdate(insertStart + " VALUES (1, 101), (2, 102), (3, 103), (4, 104)", 4);
        TableStatistics tableStatistics = getTableStatistics(tableName, new Constraint(TupleDomain.all()));
        IcebergColumnHandle col1Handle = getColumnHandleFromStatistics(tableStatistics, "col1");

        // Constraint.predicate is currently not supported, because it's never provided by the engine.
        // TODO add (restore) test coverage when this changes.

        // predicate on a partition column
        assertThatThrownBy(() ->
                getTableStatistics(tableName, new Constraint(
                        TupleDomain.all(),
                        Optional.of(new TestRelationalNumberPredicate("col1", 3, i1 -> i1 >= 0)),
                        Optional.of(ImmutableSet.of(col1Handle)))))
                .isInstanceOf(VerifyException.class)
                .hasMessage("Unexpected Constraint predicate");

        // predicate on an unspecified set of columns column
        assertThatThrownBy(() ->
                getTableStatistics(tableName, new Constraint(
                        TupleDomain.all(),
                        Optional.of(new TestRelationalNumberPredicate("col2", 102, i -> i >= 0)),
                        Optional.empty())))
                .isInstanceOf(VerifyException.class)
                .hasMessage("Unexpected Constraint predicate");

        dropTable(tableName);
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
        assertTrue(values.size() > ICEBERG_DOMAIN_COMPACTION_THRESHOLD);
        String valuesString = String.join(",", values.stream().map(Object::toString).collect(toImmutableList()));
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

        dropTable(tableName.getObjectName());
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

        dropTable(tableName);
    }

    private void assertFilterPushdown(
            QualifiedObjectName tableName,
            Map<String, Domain> filter,
            Map<String, Domain> expectedEnforcedPredicate,
            Map<String, Domain> expectedUnenforcedPredicate)
    {
        Metadata metadata = getQueryRunner().getMetadata();

        newTransaction().execute(getSession(), session -> {
            TableHandle table = metadata.getTableHandle(session, tableName)
                    .orElseThrow(() -> new TableNotFoundException(tableName.asSchemaTableName()));

            Map<String, ColumnHandle> columns = metadata.getColumnHandles(session, table);
            TupleDomain<ColumnHandle> domains = TupleDomain.withColumnDomains(
                    filter.entrySet().stream()
                            .collect(toImmutableMap(entry -> columns.get(entry.getKey()), Map.Entry::getValue)));

            Optional<ConstraintApplicationResult<TableHandle>> result = metadata.applyFilter(session, table, new Constraint(domains));

            assertTrue(result.isEmpty() == (expectedUnenforcedPredicate == null && expectedEnforcedPredicate == null));

            if (result.isPresent()) {
                IcebergTableHandle newTable = (IcebergTableHandle) result.get().getHandle().getConnectorHandle();

                assertEquals(
                        newTable.getEnforcedPredicate(),
                        TupleDomain.withColumnDomains(expectedEnforcedPredicate.entrySet().stream()
                                .collect(toImmutableMap(entry -> columns.get(entry.getKey()), Map.Entry::getValue))));

                assertEquals(
                        newTable.getUnenforcedPredicate(),
                        TupleDomain.withColumnDomains(expectedUnenforcedPredicate.entrySet().stream()
                                .collect(toImmutableMap(entry -> columns.get(entry.getKey()), Map.Entry::getValue))));
            }
        });
    }

    private static class TestRelationalNumberPredicate
            implements Predicate<Map<ColumnHandle, NullableValue>>
    {
        private final String columnName;
        private final Number comparand;
        private final Predicate<Integer> comparePredicate;

        public TestRelationalNumberPredicate(String columnName, Number comparand, Predicate<Integer> comparePredicate)
        {
            this.columnName = columnName;
            this.comparand = comparand;
            this.comparePredicate = comparePredicate;
        }

        @Override
        public boolean test(Map<ColumnHandle, NullableValue> nullableValues)
        {
            for (Map.Entry<ColumnHandle, NullableValue> entry : nullableValues.entrySet()) {
                IcebergColumnHandle handle = (IcebergColumnHandle) entry.getKey();
                if (columnName.equals(handle.getName())) {
                    Object object = entry.getValue().getValue();
                    if (object instanceof Long) {
                        return comparePredicate.test(((Long) object).compareTo(comparand.longValue()));
                    }
                    if (object instanceof Double) {
                        return comparePredicate.test(((Double) object).compareTo(comparand.doubleValue()));
                    }
                    throw new IllegalArgumentException(format("NullableValue is neither Long or Double, but %s", object));
                }
            }
            return false;
        }
    }

    private ColumnStatistics getStatisticsForColumn(TableStatistics tableStatistics, String columnName)
    {
        for (Map.Entry<ColumnHandle, ColumnStatistics> entry : tableStatistics.getColumnStatistics().entrySet()) {
            IcebergColumnHandle handle = (IcebergColumnHandle) entry.getKey();
            if (handle.getName().equals(columnName)) {
                return checkColumnStatistics(entry.getValue());
            }
        }
        throw new IllegalArgumentException("TableStatistics did not contain column named " + columnName);
    }

    private static IcebergColumnHandle getColumnHandleFromStatistics(TableStatistics tableStatistics, String columnName)
    {
        for (ColumnHandle columnHandle : tableStatistics.getColumnStatistics().keySet()) {
            IcebergColumnHandle handle = (IcebergColumnHandle) columnHandle;
            if (handle.getName().equals(columnName)) {
                return handle;
            }
        }
        throw new IllegalArgumentException("TableStatistics did not contain column named " + columnName);
    }

    private ColumnStatistics checkColumnStatistics(ColumnStatistics statistics)
    {
        assertNotNull(statistics, "statistics is null");
        // Sadly, statistics.getDataSize().isUnknown() for columns in ORC files. See the TODO
        // in IcebergOrcFileWriter.
        if (format == ORC) {
            assertTrue(statistics.getDataSize().isUnknown());
        }
        else {
            assertFalse(statistics.getDataSize().isUnknown());
        }
        assertFalse(statistics.getNullsFraction().isUnknown(), "statistics nulls fraction is unknown");
        assertFalse(statistics.getRange().isEmpty(), "statistics range is not present");
        return statistics;
    }

    private TableStatistics getTableStatistics(String tableName, Constraint constraint)
    {
        Metadata metadata = getDistributedQueryRunner().getCoordinator().getMetadata();
        QualifiedObjectName qualifiedName = QualifiedObjectName.valueOf(tableName);
        return transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                .execute(getSession(), session -> {
                    Optional<TableHandle> optionalHandle = metadata.getTableHandle(session, qualifiedName);
                    checkArgument(optionalHandle.isPresent(), "Could not create table handle for table %s", tableName);
                    return metadata.getTableStatistics(session, optionalHandle.get(), constraint);
                });
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
                ", str ROW(id INTEGER , vc VARCHAR)" +
                ", dt DATE)" +
                " WITH (partitioning = ARRAY['int'])");

        assertUpdate(
                "INSERT INTO test_nested_table_1 " +
                        " select true, 1, array['uno', 'dos', 'tres'], BIGINT '1', REAL '1.0', DOUBLE '1.0', map(array[1,2,3,4], array['ek','don','teen','char'])," +
                        " CAST(1.0 as DECIMAL(5,2))," +
                        " 'one', VARBINARY 'binary0/1values',\n" +
                        " TIMESTAMP '2021-07-24 02:43:57.348000'," +
                        " (CAST(ROW(null, 'this is a random value') AS ROW(int, varchar))), " +
                        " DATE '2021-07-24'",
                1);
        assertEquals(computeActual("SELECT * from test_nested_table_1").getRowCount(), 1);

        assertThat(query("SHOW STATS FOR test_nested_table_1"))
                .projected(0, 2, 3, 4, 5, 6) // ignore data size which is available for Parquet, but not for ORC
                .skippingTypesCheck()
                .matches("VALUES " +
                        "  ('bool', NULL, 0e0, NULL, NULL, NULL), " +
                        "  ('int', NULL, 0e0, NULL, '1', '1'), " +
                        "  ('arr', NULL, " + (format == ORC ? "0e0" : "NULL") + ", NULL, NULL, NULL), " +
                        "  ('big', NULL, 0e0, NULL, '1', '1'), " +
                        "  ('rl', NULL, 0e0, NULL, '1.0', '1.0'), " +
                        "  ('dbl', NULL, 0e0, NULL, '1.0', '1.0'), " +
                        "  ('mp', NULL, " + (format == ORC ? "0e0" : "NULL") + ", NULL, NULL, NULL), " +
                        "  ('dec', NULL, 0e0, NULL, '1.0', '1.0'), " +
                        "  ('vc', NULL, 0e0, NULL, NULL, NULL), " +
                        "  ('vb', NULL, 0e0, NULL, NULL, NULL), " +
                        "  ('ts', NULL, 0e0, NULL, " + (format == ORC ? "NULL, NULL" : "'2021-07-24 02:43:57.348000', '2021-07-24 02:43:57.348000'") + "), " +
                        "  ('str', NULL, " + (format == ORC ? "0e0" : "NULL") + ", NULL, NULL, NULL), " +
                        "  ('dt', NULL, 0e0, NULL, '2021-07-24', '2021-07-24'), " +
                        "  (NULL, NULL, NULL, 1e0, NULL, NULL)");

        dropTable("test_nested_table_1");

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
        assertEquals(computeActual("SELECT * from test_nested_table_2").getRowCount(), 1);

        assertThat(query("SHOW STATS FOR test_nested_table_2"))
                .projected(0, 2, 3, 4, 5, 6) // ignore data size which is available for Parquet, but not for ORC
                .skippingTypesCheck()
                .matches("VALUES " +
                        "  ('int', NULL, 0e0, NULL, '1', '1'), " +
                        "  ('arr', NULL, " + (format == ORC ? "0e0" : "NULL") + ", NULL, NULL, NULL), " +
                        "  ('big', NULL, 0e0, NULL, '1', '1'), " +
                        "  ('rl', NULL, 0e0, NULL, '1.0', '1.0'), " +
                        "  ('dbl', NULL, 0e0, NULL, '1.0', '1.0'), " +
                        "  ('mp', NULL, " + (format == ORC ? "0e0" : "NULL") + ", NULL, NULL, NULL), " +
                        "  ('dec', NULL, 0e0, NULL, '1.0', '1.0'), " +
                        "  ('vc', NULL, 0e0, NULL, NULL, NULL), " +
                        "  ('str', NULL, " + (format == ORC ? "0e0" : "NULL") + ", NULL, NULL, NULL), " +
                        "  (NULL, NULL, NULL, 1e0, NULL, NULL)");

        assertUpdate("CREATE TABLE test_nested_table_3 WITH (partitioning = ARRAY['int']) AS SELECT * FROM test_nested_table_2", 1);

        assertEquals(computeActual("SELECT * FROM test_nested_table_3").getRowCount(), 1);

        assertThat(query("SHOW STATS FOR test_nested_table_3"))
                .matches("SHOW STATS FOR test_nested_table_2");

        dropTable("test_nested_table_2");
        dropTable("test_nested_table_3");
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

        dropTable("test_read_isolation");
    }

    private void withTransaction(Consumer<Session> consumer)
    {
        transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                .readCommitted()
                .execute(getSession(), consumer);
    }

    private void dropTable(String table)
    {
        Session session = getSession();
        assertUpdate(session, "DROP TABLE " + table);
        assertFalse(getQueryRunner().tableExists(session, table));
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
        assertUpdate("DELETE FROM test_metadata_optimization WHERE b = 6");
        assertQuery(session, "SELECT DISTINCT b FROM test_metadata_optimization", "VALUES (9)");

        // TODO: assert behavior after deleting the last row of a partition, once row-level deletes are supported.
        // i.e. a query like 'DELETE FROM test_metadata_optimization WHERE b = 6 AND a = 5'

        dropTable("test_metadata_optimization");
    }

    @Test
    public void testIncorrectIcebergFileSizes()
            throws Exception
    {
        // Create a table with a single insert
        assertUpdate("CREATE TABLE test_iceberg_file_size (x BIGINT) WITH (format='PARQUET')");
        assertUpdate("INSERT INTO test_iceberg_file_size VALUES (123), (456), (758)", 3);

        // Get manifest file
        MaterializedResult result = computeActual("SELECT path FROM \"test_iceberg_file_size$manifests\"");
        assertEquals(result.getRowCount(), 1);
        String manifestFile = (String) result.getOnlyValue();

        // Read manifest file
        Schema schema;
        GenericData.Record entry = null;
        try (DataFileReader<GenericData.Record> dataFileReader = new DataFileReader<>(new File(manifestFile), new GenericDatumReader<>())) {
            schema = dataFileReader.getSchema();
            int recordCount = 0;
            while (dataFileReader.hasNext()) {
                entry = dataFileReader.next();
                recordCount++;
            }
            assertEquals(recordCount, 1);
        }

        // Alter data file entry to store incorrect file size
        GenericData.Record dataFile = (GenericData.Record) entry.get("data_file");
        long alteredValue = 50L;
        assertNotEquals((long) dataFile.get("file_size_in_bytes"), alteredValue);
        dataFile.put("file_size_in_bytes", alteredValue);

        // Replace the file through HDFS client. This is required for correct checksums.
        HdfsEnvironment.HdfsContext context = new HdfsContext(getSession().toConnectorSession());
        Path manifestFilePath = new Path(manifestFile);
        FileSystem fs = HDFS_ENVIRONMENT.getFileSystem(context, manifestFilePath);

        // Write altered metadata
        try (OutputStream out = fs.create(manifestFilePath);
                DataFileWriter<GenericData.Record> dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            dataFileWriter.create(schema, out);
            dataFileWriter.append(entry);
        }

        // Ignoring Iceberg provided file size makes the query succeed
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", "use_file_size_from_metadata", "false")
                .build();
        assertQuery(session, "SELECT * FROM test_iceberg_file_size", "VALUES (123), (456), (758)");

        // Using Iceberg provided file size fails the query
        assertQueryFails("SELECT * FROM test_iceberg_file_size", format("Error reading tail from .* with length %d", alteredValue));

        dropTable("test_iceberg_file_size");
    }

    @Test
    @Flaky(issue = "https://github.com/trinodb/trino/issues/5172", match = "Couldn't find operator summary, probably due to query statistic collection error")
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

        // TODO(https://github.com/trinodb/trino/issues/9309) should be 1
        verifySplitCount("SELECT * FROM " + tableName + " WHERE regionkey % 5 = 3", 5);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAllAvailableTypes()
    {
        assertUpdate("CREATE TABLE test_all_types (" +
                "  a_bool BOOLEAN" +
                ", a_int INTEGER" +
                ", a_big BIGINT" +
                ", a_real REAL" +
                ", a_double DOUBLE" +
                ", a_short_decimal DECIMAL(5,2)" +
                ", a_long_decimal DECIMAL(38,20)" +
                ", a_date DATE" +
                ", a_time TIME(6)" +
                ", a_timestamp TIMESTAMP(6)" +
                ", a_timestamptz TIMESTAMP(6) WITH TIME ZONE" +
                ", a_string VARCHAR" +
                ", a_fixed VARBINARY" +
                ", a_binary VARBINARY" +
                ", a_row ROW(id INTEGER , vc VARCHAR)" +
                ", a_array ARRAY(VARCHAR)" +
                ", a_map MAP(INTEGER, VARCHAR)" +
                ")");

        assertUpdate(
                "INSERT INTO test_all_types VALUES(" +
                        "true, " +
                        "1, " +
                        "BIGINT '1', " +
                        "REAL '1.0', " +
                        "DOUBLE '1.0',  " +
                        "CAST(1.0 as DECIMAL(5,2)),  " +
                        "CAST(11.0 as DECIMAL(38,20)),  " +
                        "DATE '2021-07-24'," +
                        "TIME '02:43:57.348000',  " +
                        "TIMESTAMP '2021-07-24 03:43:57.348000'," +
                        "TIMESTAMP '2021-07-24 04:43:57.348000' AT TIME ZONE 'America/Los_Angeles',  " +
                        "'onefsadfdsf', " +
                        "X'000102f0feff', " +
                        "VARBINARY 'binary2/3values', " +
                        "(CAST(ROW(null, 'this is a random value') AS ROW(int, varchar))), " +
                        "array['uno', 'dos', 'tres'], " +
                        "map(array[1,2], array['ek', 'one']))", 1);

        @Language("SQL") String expectedResult = "" +
                "VALUES(" +
                " true" +
                ", 1" +
                ", 1" +
                ", CAST('1.0' AS REAL)" +
                ", CAST('1.0' AS DOUBLE)" +
                ", CAST('1.0' AS DECIMAL(5,2))" +
                ", CAST('11.0' AS DECIMAL(38,20))" +
                ", CAST('2021-07-24' AS DATE)" +
                ", CAST('02:43:57.348000' AS TIME(6))" +
                ", CAST('2021-07-24 03:43:57.348000' AS TIMESTAMP(6))" +
                ", 'onefsadfdsf')";
        assertQuery("SELECT a_bool, a_int, a_big, a_real, a_double, a_short_decimal, a_long_decimal, a_date, a_time, a_timestamp, a_string FROM test_all_types", expectedResult);
        query("SELECT a_timestamptz, a_fixed, a_binary, a_row, a_array, a_map FROM test_all_types").assertThat()
                .matches(
                        resultBuilder(
                                SESSION,
                                TIMESTAMP_TZ_MICROS,
                                VARBINARY,
                                VARBINARY,
                                RowType.from(
                                        ImmutableList.of(
                                                new RowType.Field(Optional.of("id"), INTEGER),
                                                new RowType.Field(Optional.of("vc"), VARCHAR))),
                                new ArrayType(VARCHAR),
                                new MapType(INTEGER, VARCHAR, new TypeOperators()))
                                .row(
                                        ZonedDateTime.of(2021, 7, 23, 15, 43, 57, 348000000, ZoneId.of("UTC")),
                                        new byte[]{00, 01, 02, -16, -2, -1},
                                        "binary2/3values".getBytes(StandardCharsets.UTF_8),
                                        new MaterializedRow(Arrays.asList(null, "this is a random value")),
                                        Arrays.asList("uno", "dos", "tres"),
                                        ImmutableMap.of(1, "ek", 2, "one"))
                                .build());
    }

    @Test(dataProvider = "testDataMappingSmokeTestDataProvider")
    @Flaky(issue = "https://github.com/trinodb/trino/issues/5172", match = "Couldn't find operator summary, probably due to query statistic collection error")
    public void testSplitPruningForFilterOnNonPartitionColumn(DataMappingTestSetup testSetup)
    {
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
            assertQuery("select count(distinct file_path) from \"" + tableName + "$files\"", "VALUES 2");

            int expectedSplitCount = supportsIcebergFileStatistics(testSetup.getTrinoTypeName()) ? 1 : 2;
            verifySplitCount("SELECT row_id FROM " + tableName, 2);
            verifySplitCount("SELECT row_id FROM " + tableName + " WHERE col = " + sampleValue, expectedSplitCount);
            verifySplitCount("SELECT row_id FROM " + tableName + " WHERE col = " + highValue, expectedSplitCount);
            verifySplitCount("SELECT row_id FROM " + tableName + " WHERE col > " + sampleValue, expectedSplitCount);
            verifySplitCount("SELECT row_id FROM " + tableName + " WHERE col < " + highValue, expectedSplitCount);
        }
    }

    protected abstract boolean supportsIcebergFileStatistics(String typeName);

    @Test(dataProvider = "testDataMappingSmokeTestDataProvider")
    @Flaky(issue = "https://github.com/trinodb/trino/issues/5172", match = "Couldn't find operator summary, probably due to query statistic collection error")
    public void testSplitPruningFromDataFileStatistics(DataMappingTestSetup testSetup)
    {
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
                            .collect(Collectors.joining(", "));
            assertUpdate(withSmallRowGroups(getSession()), "INSERT INTO " + tableName + " VALUES " + values, 200);

            String query = "SELECT * FROM " + tableName + " WHERE col = " + testSetup.getSampleValueLiteral();
            verifyPredicatePushdownDataRead(query, supportsRowGroupStatistics(testSetup.getTrinoTypeName()));
        }
    }

    protected abstract Session withSmallRowGroups(Session session);

    protected abstract boolean supportsRowGroupStatistics(String typeName);

    private void verifySplitCount(String query, int expectedSplitCount)
    {
        // using assertEventually here as the operator stats mechanism is known to be best-effort and some late-stage stats are sometimes not recorded
        // (see https://github.com/trinodb/trino/issues/5172)
        assertEventually(new Duration(10, TimeUnit.SECONDS), () -> {
            ResultWithQueryId<MaterializedResult> selectAllPartitionsResult = getDistributedQueryRunner().executeWithQueryId(getSession(), query);
            assertEqualsIgnoreOrder(selectAllPartitionsResult.getResult().getMaterializedRows(), computeActual(withoutPredicatePushdown(getSession()), query).getMaterializedRows());
            verifySplitCount(selectAllPartitionsResult.getQueryId(), expectedSplitCount);
        });
    }

    private void verifyPredicatePushdownDataRead(@Language("SQL") String query, boolean supportsPushdown)
    {
        try {
            ResultWithQueryId<MaterializedResult> resultWithPredicatePushdown = getDistributedQueryRunner().executeWithQueryId(getSession(), query);
            ResultWithQueryId<MaterializedResult> resultWithoutPredicatePushdown = getDistributedQueryRunner().executeWithQueryId(
                    withoutPredicatePushdown(getSession()),
                    query);

            DataSize withPushdownDataSize = getOperatorStats(resultWithPredicatePushdown.getQueryId()).getInputDataSize();
            DataSize withoutPushdownDataSize = getOperatorStats(resultWithoutPredicatePushdown.getQueryId()).getInputDataSize();
            if (supportsPushdown) {
                assertThat(withPushdownDataSize).isLessThan(withoutPushdownDataSize);
            }
            else {
                assertThat(withPushdownDataSize).isEqualTo(withoutPushdownDataSize);
            }
        }
        catch (NoSuchElementException e) {
            throw new RuntimeException("Couldn't find operator summary, probably due to query statistic collection error", e);
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
        try {
            OperatorStats operatorStats = getOperatorStats(queryId);
            if (expectedSplitCount > 0) {
                assertThat(operatorStats.getTotalDrivers()).isEqualTo(expectedSplitCount);
                assertThat(operatorStats.getAddInputCalls()).isGreaterThan(0);
            }
            else {
                // expectedSplitCount == 0
                assertThat(operatorStats.getTotalDrivers()).isEqualTo(1);
                assertThat(operatorStats.getAddInputCalls()).isEqualTo(0);
            }
        }
        catch (NoSuchElementException e) {
            throw new RuntimeException("Couldn't find operator summary, probably due to query statistic collection error", e);
        }
    }

    private OperatorStats getOperatorStats(QueryId queryId)
    {
        return getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(summary -> summary.getOperatorType().startsWith("TableScan") || summary.getOperatorType().startsWith("Scan"))
                .collect(onlyElement());
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        throw new SkipException("Iceberg connector does not support column default values");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("tinyint")
                || typeName.equals("smallint")
                || typeName.startsWith("char(")) {
            // These types are not supported by Iceberg
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        // According to Iceberg specification all time and timestamp values are stored with microsecond precision.
        if (typeName.equals("time")) {
            return Optional.of(new DataMappingTestSetup("time(6)", "TIME '15:03:00'", "TIME '23:59:59.999999'"));
        }

        if (typeName.equals("timestamp")) {
            return Optional.of(new DataMappingTestSetup("timestamp(6)", "TIMESTAMP '2020-02-12 15:03:00'", "TIMESTAMP '2199-12-31 23:59:59.999999'"));
        }

        if (typeName.equals("timestamp(3) with time zone")) {
            return Optional.of(new DataMappingTestSetup("timestamp(6) with time zone", "TIMESTAMP '2020-02-12 15:03:00 +01:00'", "TIMESTAMP '9999-12-31 23:59:59.999999 +12:00'"));
        }

        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected Optional<DataMappingTestSetup> filterCaseSensitiveDataMappingTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("char(1)")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }
        return Optional.of(dataMappingTestSetup);
    }
}
