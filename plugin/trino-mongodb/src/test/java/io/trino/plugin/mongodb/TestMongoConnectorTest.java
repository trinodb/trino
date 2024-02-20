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
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.mongodb.DBRef;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CreateCollectionOptions;
import io.trino.Session;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static com.mongodb.client.model.CollationCaseFirst.LOWER;
import static com.mongodb.client.model.CollationStrength.PRIMARY;
import static io.trino.plugin.mongodb.MongoQueryRunner.createMongoClient;
import static io.trino.plugin.mongodb.MongoQueryRunner.createMongoQueryRunner;
import static io.trino.plugin.mongodb.TypeUtils.isPushdownSupportedType;
import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestMongoConnectorTest
        extends BaseConnectorTest
{
    protected MongoServer server;
    protected MongoClient client;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = new MongoServer();
        client = createMongoClient(server);
        return createMongoQueryRunner(server, ImmutableMap.of(), REQUIRED_TPCH_TABLES);
    }

    @BeforeAll
    public void initTestSchema()
    {
        assertUpdate("CREATE SCHEMA IF NOT EXISTS test");
    }

    @AfterAll
    public final void destroy()
    {
        server.close();
        server = null;
        client.close();
        client = null;
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_FIELD,
                    SUPPORTS_CREATE_MATERIALIZED_VIEW,
                    SUPPORTS_CREATE_VIEW,
                    SUPPORTS_DROP_FIELD,
                    SUPPORTS_MERGE,
                    SUPPORTS_NOT_NULL_CONSTRAINT,
                    SUPPORTS_RENAME_FIELD,
                    SUPPORTS_RENAME_SCHEMA,
                    SUPPORTS_SET_FIELD_TYPE,
                    SUPPORTS_TRUNCATE,
                    SUPPORTS_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return abort("MongoDB connector does not support column default values");
    }

    @Test
    @Override
    public void testColumnName()
    {
        for (String columnName : testColumnNameDataProvider()) {
            if (columnName.equals("a.dot")) {
                assertThatThrownBy(() -> testColumnName(columnName, requiresDelimiting(columnName)))
                        .isInstanceOf(RuntimeException.class)
                        .hasMessage("Column name must not contain '$' or '.' for INSERT: " + columnName);
                abort("Insert would fail");
            }

            testColumnName(columnName, requiresDelimiting(columnName));
        }
    }

    @Test
    @Override
    public void testSortItemsReflectedInExplain()
    {
        // The format of the string representation of what gets shown in the table scan is connector-specific
        // and there's no requirement that the conform to a specific shape or contain certain keywords.
        assertExplain(
                "EXPLAIN SELECT name FROM nation ORDER BY nationkey DESC NULLS LAST LIMIT 5",
                "TopNPartial\\[count = 5, orderBy = \\[nationkey DESC");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("time(6)") ||
                typeName.equals("timestamp(6)") ||
                typeName.equals("timestamp(6) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Test
    public void testGuessFieldTypes()
    {
        testGuessFieldTypes("true", "true"); // boolean -> boolean
        testGuessFieldTypes("2147483647", "bigint '2147483647'"); // int32 -> bigint
        testGuessFieldTypes("{\"$numberLong\": \"9223372036854775807\"}", "9223372036854775807"); // int64 -> bigint
        testGuessFieldTypes("1.23", "double '1.23'"); // double -> double
        testGuessFieldTypes("{\"$date\": \"1970-01-01T00:00:00.000Z\"}", "timestamp '1970-01-01 00:00:00.000'"); // date -> timestamp(3)
        testGuessFieldTypes("'String type'", "varchar 'String type'"); // string -> varchar
        testGuessFieldTypes("{$binary: \"\",\"$type\": \"0\"}", "to_utf8('')"); // binary -> varbinary
        testGuessFieldTypes("{\"$oid\": \"6216f0c6c432d45190f25e7c\"}", "ObjectId('6216f0c6c432d45190f25e7c')"); // objectid -> objectid
        testGuessFieldTypes("[1]", "array[bigint '1']"); // array with single type -> array
        testGuessFieldTypes("{\"field\": \"object\"}", "CAST(row('object') AS row(field varchar))"); // object -> row
        testGuessFieldTypes("[9, \"test\"]", "CAST(row(9, 'test') AS row(_pos1 bigint, _pos2 varchar))"); // array with multiple types -> row
        testGuessFieldTypes("{\"$ref\":\"test_ref\",\"$id\":ObjectId(\"4e3f33de6266b5845052c02c\"),\"$db\":\"test_db\"}", "CAST(row('test_db', 'test_ref', ObjectId('4e3f33de6266b5845052c02c')) AS row(databasename varchar, collectionname varchar, id ObjectId))"); // dbref -> row
    }

    private void testGuessFieldTypes(String mongoValue, String trinoValue)
    {
        String tableName = "test_guess_field_type_" + randomNameSuffix();
        Document document = Document.parse(format("{\"test\":%s}", mongoValue));

        assertUpdate("DROP TABLE IF EXISTS test." + tableName);
        client.getDatabase("test").getCollection(tableName).insertOne(document);

        assertThat(query("SELECT test FROM test." + tableName))
                .matches("SELECT " + trinoValue);

        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void createTableWithEveryType()
    {
        String tableName = "test_types_table_" + randomNameSuffix();
        String query = "CREATE TABLE " +
                tableName +
                " AS SELECT" +
                " 'foo' _varchar" +
                ", cast('bar' as varbinary) _varbinary" +
                ", cast(1 as bigint) _bigint" +
                ", 3.14E0 _double" +
                ", true _boolean" +
                ", DATE '1980-05-07' _date" +
                ", TIMESTAMP '1980-05-07 11:22:33.456' _timestamp" +
                ", ObjectId('ffffffffffffffffffffffff') _objectid" +
                ", JSON '{\"name\":\"alice\"}' _json" +
                ", cast(12.3 as decimal(30, 5)) _long_decimal";

        assertUpdate(query, 1);

        MaterializedResult results = getQueryRunner().execute(getSession(), "SELECT * FROM " + tableName).toTestTypes();
        assertThat(results.getRowCount())
                .isEqualTo(1);
        MaterializedRow row = results.getMaterializedRows().get(0);
        assertThat(row.getField(0))
                .isEqualTo("foo");
        assertThat(row.getField(1))
                .isEqualTo("bar".getBytes(UTF_8));
        assertThat(row.getField(2))
                .isEqualTo(1L);
        assertThat(row.getField(3))
                .isEqualTo(3.14);
        assertThat(row.getField(4))
                .isEqualTo(true);
        assertThat(row.getField(5))
                .isEqualTo(LocalDate.of(1980, 5, 7));
        assertThat(row.getField(6))
                .isEqualTo(LocalDateTime.of(1980, 5, 7, 11, 22, 33, 456_000_000));
        assertThat(row.getField(8))
                .isEqualTo("{\"name\":\"alice\"}");
        assertThat(row.getField(9))
                .isEqualTo(new BigDecimal("12.30000"));
        assertUpdate("DROP TABLE " + tableName);

        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    @Test
    public void testInsertWithEveryType()
    {
        String tableName = "test_insert_types_table_" + randomNameSuffix();
        String createSql = "CREATE TABLE " +
                tableName +
                "(" +
                "  vc varchar" +
                ", vb varbinary" +
                ", bi bigint" +
                ", d double" +
                ", b boolean" +
                ", dt  date" +
                ", ts  timestamp" +
                ", objid objectid" +
                ", _json json" +
                ")";
        getQueryRunner().execute(getSession(), createSql);

        String insertSql = "INSERT INTO " +
                tableName +
                " SELECT" +
                " 'foo' _varchar" +
                ", cast('bar' as varbinary) _varbinary" +
                ", cast(1 as bigint) _bigint" +
                ", 3.14E0 _double" +
                ", true _boolean" +
                ", DATE '1980-05-07' _date" +
                ", TIMESTAMP '1980-05-07 11:22:33.456' _timestamp" +
                ", ObjectId('ffffffffffffffffffffffff') _objectid" +
                ", JSON '{\"name\":\"alice\"}' _json";
        getQueryRunner().execute(getSession(), insertSql);

        MaterializedResult results = getQueryRunner().execute(getSession(), "SELECT * FROM " + tableName).toTestTypes();
        assertThat(results.getRowCount())
                .isEqualTo(1);
        MaterializedRow row = results.getMaterializedRows().get(0);
        assertThat(row.getField(0))
                .isEqualTo("foo");
        assertThat(row.getField(1))
                .isEqualTo("bar".getBytes(UTF_8));
        assertThat(row.getField(2))
                .isEqualTo(1L);
        assertThat(row.getField(3))
                .isEqualTo(3.14);
        assertThat(row.getField(4))
                .isEqualTo(true);
        assertThat(row.getField(5))
                .isEqualTo(LocalDate.of(1980, 5, 7));
        assertThat(row.getField(6))
                .isEqualTo(LocalDateTime.of(1980, 5, 7, 11, 22, 33, 456_000_000));
        assertThat(row.getField(8))
                .isEqualTo("{\"name\":\"alice\"}");
        assertUpdate("DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    @Test
    @Override
    public void testDeleteWithComplexPredicate()
    {
        assertThatThrownBy(super::testDeleteWithComplexPredicate)
                .hasStackTraceContaining("TrinoException: " + MODIFYING_ROWS_MESSAGE);
    }

    @Test
    @Override
    public void testDeleteWithLike()
    {
        assertThatThrownBy(super::testDeleteWithLike)
                .hasStackTraceContaining("TrinoException: " + MODIFYING_ROWS_MESSAGE);
    }

    @Test
    @Override
    public void testDeleteWithSemiJoin()
    {
        assertThatThrownBy(super::testDeleteWithSemiJoin)
                .hasStackTraceContaining("TrinoException: " + MODIFYING_ROWS_MESSAGE);
    }

    @Test
    @Override
    public void testDeleteWithSubquery()
    {
        assertThatThrownBy(super::testDeleteWithSubquery)
                .hasStackTraceContaining("TrinoException: " + MODIFYING_ROWS_MESSAGE);
    }

    @Test
    @Override
    public void testExplainAnalyzeWithDeleteWithSubquery()
    {
        assertThatThrownBy(super::testExplainAnalyzeWithDeleteWithSubquery)
                .hasStackTraceContaining("TrinoException: " + MODIFYING_ROWS_MESSAGE);
    }

    @Test
    public void testPredicatePushdown()
    {
        testPredicatePushdown("true");
        testPredicatePushdown("tinyint '1'");
        testPredicatePushdown("smallint '2'");
        testPredicatePushdown("integer '3'");
        testPredicatePushdown("bigint '4'");
        testPredicatePushdown("decimal '3.14'");
        testPredicatePushdown("decimal '1234567890.123456789'");
        testPredicatePushdown("'test'");
        testPredicatePushdown("char 'test'");
        testPredicatePushdown("objectid('6216f0c6c432d45190f25e7c')");
        testPredicatePushdown("date '1970-01-01'");
        testPredicatePushdown("time '00:00:00.000'");
        testPredicatePushdown("timestamp '1970-01-01 00:00:00.000'");
        testPredicatePushdown("timestamp '1970-01-01 00:00:00.000 UTC'");
    }

    private void testPredicatePushdown(String value)
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_predicate_pushdown", "AS SELECT %s col".formatted(value))) {
            testPredicatePushdown(table.getName(), "col = " + value);
            testPredicatePushdown(table.getName(), "col != " + value);
            testPredicatePushdown(table.getName(), "col < " + value);
            testPredicatePushdown(table.getName(), "col > " + value);
            testPredicatePushdown(table.getName(), "col <= " + value);
            testPredicatePushdown(table.getName(), "col >= " + value);
        }
    }

    @Test
    public void testPredicatePushdownRealType()
    {
        testPredicatePushdownFloatingPoint("real '1.234'");
    }

    @Test
    public void testPredicatePushdownDoubleType()
    {
        testPredicatePushdownFloatingPoint("double '5.678'");
    }

    private void testPredicatePushdownFloatingPoint(String value)
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_floating_point_pushdown", "AS SELECT %s col".formatted(value))) {
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE col = " + value))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE col <= " + value))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE col >= " + value))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE col > " + value))
                    .returnsEmptyResult()
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE col < " + value))
                    .returnsEmptyResult()
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE col != " + value))
                    .returnsEmptyResult()
                    .isNotFullyPushedDown(FilterNode.class);
        }
    }

    @Test
    public void testPredicatePushdownCharWithPaddedSpace()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_predicate_pushdown_char_with_padded_space",
                "(k, v) AS VALUES" +
                        "   (-1, CAST(NULL AS char(3))), " +
                        "   (0, CAST('' AS char(3)))," +
                        "   (1, CAST(' ' AS char(3))), " +
                        "   (2, CAST('  ' AS char(3))), " +
                        "   (3, CAST('   ' AS char(3)))," +
                        "   (4, CAST('x' AS char(3)))," +
                        "   (5, CAST('x ' AS char(3)))," +
                        "   (6, CAST('x  ' AS char(3)))," +
                        "   (7, CAST('\0' AS char(3)))," +
                        "   (8, CAST('\0 ' AS char(3)))," +
                        "   (9, CAST('\0  ' AS char(3)))")) {
            assertThat(query("SELECT k FROM " + table.getName() + " WHERE v = ''"))
                    // The value is included because both sides of the comparison are coerced to char(3)
                    .matches("VALUES 0, 1, 2, 3")
                    .isFullyPushedDown();
            assertThat(query("SELECT k FROM " + table.getName() + " WHERE v = 'x '"))
                    // The value is included because both sides of the comparison are coerced to char(3)
                    .matches("VALUES 4, 5, 6")
                    .isFullyPushedDown();
            assertThat(query("SELECT k FROM " + table.getName() + " WHERE v = '\0  '"))
                    // The value is included because both sides of the comparison are coerced to char(3)
                    .matches("VALUES 7, 8, 9")
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testPredicatePushdownMultipleNotEquals()
    {
        // Regression test for https://github.com/trinodb/trino/issues/19404
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_predicate_pushdown_with_multiple_not_equals",
                "(id, value) AS VALUES (1, 10), (2, 20), (3, 30)")) {
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE id != 1 AND value != 20"))
                    .matches("VALUES (3, 30)")
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testHighPrecisionDecimalPredicate()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_high_precision_decimal_predicate",
                "(col DECIMAL(34, 0))",
                Arrays.asList("decimal '3141592653589793238462643383279502'", null))) {
            // Filter clause with 38 precision decimal value
            String predicateValue = "decimal '31415926535897932384626433832795028841'";
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE col = " + predicateValue))
                    // With EQUAL operator when column type precision is less than the predicate value's precision,
                    // PushPredicateIntoTableScan#pushFilterIntoTableScan returns ValuesNode. So It is not possible to verify isFullyPushedDown.
                    .returnsEmptyResult();
            testPredicatePushdown(table.getName(), "col != " + predicateValue);
            testPredicatePushdown(table.getName(), "col < " + predicateValue);
            testPredicatePushdown(table.getName(), "col > " + predicateValue);
            testPredicatePushdown(table.getName(), "col <= " + predicateValue);
            testPredicatePushdown(table.getName(), "col >= " + predicateValue);

            // Filter clause with 34 precision decimal value
            predicateValue = "decimal '3141592653589793238462643383279502'";
            testPredicatePushdown(table.getName(), "col = " + predicateValue);
            testPredicatePushdown(table.getName(), "col != " + predicateValue);
            testPredicatePushdown(table.getName(), "col < " + predicateValue);
            testPredicatePushdown(table.getName(), "col > " + predicateValue);
            testPredicatePushdown(table.getName(), "col <= " + predicateValue);
            testPredicatePushdown(table.getName(), "col >= " + predicateValue);
        }
    }

    private void testPredicatePushdown(String tableName, String whereClause)
    {
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + whereClause))
                .isFullyPushedDown();
    }

    @Test
    public void testJson()
    {
        String tableName = "test_json_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT, col JSON)");

        assertUpdate("INSERT INTO " + tableName + " VALUES (1, JSON '{\"name\":\"alice\"}')", 1);
        assertQuery("SELECT json_extract_scalar(col, '$.name') FROM " + tableName + " WHERE id = 1", "SELECT 'alice'");

        assertUpdate("INSERT INTO " + tableName + " VALUES (2, JSON '{\"numbers\":[1, 2, 3]}')", 1);
        assertQuery("SELECT json_extract(col, '$.numbers[0]') FROM " + tableName + " WHERE id = 2", "SELECT 1");

        assertUpdate("INSERT INTO " + tableName + " VALUES (3, NULL)", 1);
        assertQuery("SELECT col FROM " + tableName + " WHERE id = 3", "SELECT NULL");

        assertQueryFails(
                "CREATE TABLE test_json_scalar AS SELECT JSON '1' AS col",
                "Can't convert json to MongoDB Document.*");

        assertQueryFails(
                "CREATE TABLE test_json_array AS SELECT JSON '[\"a\", \"b\", \"c\"]' AS col",
                "Can't convert json to MongoDB Document.*");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testArrays()
    {
        String arrayIntegerTable = "test_array_integer" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + arrayIntegerTable + " AS SELECT ARRAY[1, 2, NULL] AS col", 1);
        assertQuery("SELECT col[2] FROM " + arrayIntegerTable, "SELECT 2");
        assertQuery("SELECT col[3] FROM " + arrayIntegerTable, "SELECT NULL");
        assertUpdate("DROP TABLE " + arrayIntegerTable);

        String arrayDoubleTable = "test_array_double" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + arrayDoubleTable + " AS SELECT ARRAY[1.0E0, 2.5E0, 3.5E0] AS col", 1);
        assertQuery("SELECT col[2] FROM " + arrayDoubleTable, "SELECT 2.5");
        assertUpdate("DROP TABLE " + arrayDoubleTable);

        String arrayVarcharTable = "test_array_varchar" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + arrayVarcharTable + " AS SELECT ARRAY['puppies', 'kittens', NULL] AS col", 1);
        assertQuery("SELECT col[2] FROM " + arrayVarcharTable, "SELECT 'kittens'");
        assertQuery("SELECT col[3] FROM " + arrayVarcharTable, "SELECT NULL");
        assertUpdate("DROP TABLE " + arrayVarcharTable);

        String arrayBooleanTable = "test_array_boolean" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + arrayBooleanTable + " AS SELECT ARRAY[TRUE, NULL] AS col", 1);
        assertQuery("SELECT col[1] FROM " + arrayBooleanTable, "SELECT TRUE");
        assertQuery("SELECT col[2] FROM " + arrayBooleanTable, "SELECT NULL");
        assertUpdate("DROP TABLE " + arrayBooleanTable);

        String nestedArrayIntegerTable = "test_nested_array_integer" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + nestedArrayIntegerTable + " AS SELECT ARRAY[ARRAY[1, 2], NULL, ARRAY[3, 4]] AS col", 1);
        assertQuery("SELECT col[1][2] FROM " + nestedArrayIntegerTable, "SELECT 2");
        assertUpdate("DROP TABLE " + nestedArrayIntegerTable);

        String nestedArrayVarcharTable = "test_nested_array_varchar" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + nestedArrayVarcharTable + " AS SELECT ARRAY[ARRAY['\"hi\"'], NULL, ARRAY['puppies']] AS col", 1);
        assertQuery("SELECT col[1][1] FROM " + nestedArrayVarcharTable, "SELECT '\"hi\"'");
        assertQuery("SELECT col[3][1] FROM " + nestedArrayVarcharTable, "SELECT 'puppies'");
        assertUpdate("DROP TABLE " + nestedArrayVarcharTable);
    }

    @Test
    public void testTemporalArrays()
    {
        String dateArrayTable = "test_array_date" + randomNameSuffix();
        String timestampArrayTable = "test_array_timestamp" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + dateArrayTable + " AS SELECT ARRAY[DATE '2014-09-30'] AS col", 1);
        assertOneNotNullResult("SELECT col[1] FROM " + dateArrayTable);
        assertUpdate("DROP TABLE " + dateArrayTable);

        assertUpdate("CREATE TABLE " + timestampArrayTable + " AS SELECT ARRAY[TIMESTAMP '2001-08-22 03:04:05.321'] AS col", 1);
        assertOneNotNullResult("SELECT col[1] FROM " + timestampArrayTable);
        assertUpdate("DROP TABLE " + timestampArrayTable);
    }

    @Test
    public void testSkipUnknownTypes()
    {
        String unknownFieldTable = "test_unknown_field" + randomNameSuffix();
        Document document1 = new Document("col", Document.parse("{\"key1\": \"value1\", \"key2\": null}"));
        client.getDatabase("test").getCollection(unknownFieldTable).insertOne(document1);
        assertQuery("SHOW COLUMNS FROM test." + unknownFieldTable, "SELECT 'col', 'row(key1 varchar)', '', ''");
        assertQuery("SELECT col.key1 FROM test." + unknownFieldTable, "SELECT 'value1'");
        assertUpdate("DROP TABLE test." + unknownFieldTable);

        String allUnknownFieldTable = "test_all_unknown_field" + randomNameSuffix();
        Document document2 = new Document("col", new Document("key1", null));
        client.getDatabase("test").getCollection(allUnknownFieldTable).insertOne(document2);
        assertQueryReturnsEmptyResult("SHOW COLUMNS FROM test." + allUnknownFieldTable);
        assertUpdate("DROP TABLE test." + allUnknownFieldTable);
    }

    @Test
    public void testSkipUnsupportedDecimal128()
    {
        String tableName = "test_unsupported_decimal128" + randomNameSuffix();

        Document document = new Document(ImmutableMap.<String, Object>builder()
                .put("col", 1)
                .put("nan", Decimal128.NaN)
                .put("negative_nan", Decimal128.NEGATIVE_NaN)
                .put("positive_infinity", Decimal128.POSITIVE_INFINITY)
                .put("negative_infinity", Decimal128.NEGATIVE_INFINITY)
                .put("negative_zero", Decimal128.NEGATIVE_ZERO)
                .buildOrThrow());
        client.getDatabase("test").getCollection(tableName).insertOne(document);
        assertQuery("SHOW COLUMNS FROM test." + tableName, "SELECT 'col', 'bigint', '', ''");
        assertQuery("SELECT col FROM test." + tableName, "SELECT 1");

        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testNegativeZeroDecimal()
    {
        String tableName = "test_negative_zero" + randomNameSuffix();

        assertUpdate("CREATE TABLE test." + tableName + "(id int, short_decimal decimal(1), long_decimal decimal(38))");
        client.getDatabase("test").getCollection(tableName)
                .insertOne(new Document(ImmutableMap.<String, Object>builder()
                        .put("id", 1)
                        .put("short_decimal", Decimal128.NEGATIVE_ZERO)
                        .put("long_decimal", Decimal128.NEGATIVE_ZERO)
                        .buildOrThrow()));
        client.getDatabase("test").getCollection(tableName)
                .insertOne(new Document(ImmutableMap.<String, Object>builder()
                        .put("id", 2)
                        .put("short_decimal", Decimal128.parse("-0.000"))
                        .put("long_decimal", Decimal128.parse("-0.000"))
                        .buildOrThrow()));

        assertThat(query("SELECT * FROM test." + tableName))
                .matches("VALUES (1, CAST('0' AS decimal(1)), CAST('0' AS decimal(38))), (2, CAST('0' AS decimal(1)), CAST('0' AS decimal(38)))");

        assertThat(query("SELECT id FROM test." + tableName + " WHERE short_decimal = decimal '0'"))
                .matches("VALUES 1, 2");

        assertThat(query("SELECT id FROM test." + tableName + " WHERE long_decimal = decimal '0'"))
                .matches("VALUES 1, 2");

        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testDBRef()
    {
        testDBRef("String type", "varchar 'String type'", "varchar");
        testDBRef("BinData".getBytes(UTF_8), "to_utf8('BinData')", "varbinary");
        testDBRef(1234567890, "bigint '1234567890'", "bigint");
        testDBRef(true, "true", "boolean");
        testDBRef(12.3f, "double '12.3'", "double");
        testDBRef(new Date(0), "timestamp '1970-01-01 00:00:00.000'", "timestamp(3)");
        testDBRef(ImmutableList.of(1), "array[bigint '1']", "array(bigint)");
        testDBRef(new ObjectId("5126bc054aed4daf9e2ab772"), "ObjectId('5126bc054aed4daf9e2ab772')", "ObjectId");
    }

    private void testDBRef(Object objectId, String expectedValue, String expectedType)
    {
        Document document = Document.parse("{\"_id\":ObjectId(\"5126bbf64aed4daf9e2ab771\"),\"col1\":\"foo\"}");

        DBRef dbRef = new DBRef("test", "creators", objectId);
        document.append("creator", dbRef);

        String tableName = "test_dbref_" + randomNameSuffix();

        assertUpdate("DROP TABLE IF EXISTS test." + tableName);
        client.getDatabase("test").getCollection(tableName).insertOne(document);

        assertThat(query("SELECT creator.databaseName, creator.collectionName, creator.id FROM test." + tableName))
                .matches("SELECT varchar 'test', varchar 'creators', " + expectedValue);
        assertQuery(
                "SELECT typeof(creator) FROM test." + tableName,
                "SELECT 'row(databaseName varchar, collectionName varchar, id " + expectedType + ")'");

        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testDbRefFieldOrder()
    {
        // DBRef's field order is databaseName, collectionName and id
        // Create a table with different order and verify the result
        String tableName = "test_dbref_field_order" + randomNameSuffix();
        assertUpdate("CREATE TABLE test." + tableName + "(x row(id int, \"collectionName\" varchar, \"databaseName\" varchar))");

        Document document = new Document()
                .append("x", new DBRef("test_db", "test_collection", 1));
        client.getDatabase("test").getCollection(tableName).insertOne(document);

        assertThat(query("SELECT * FROM test." + tableName))
                .matches("SELECT CAST(row(1, 'test_collection', 'test_db') AS row(id int, \"collectionName\" varchar, \"databaseName\" varchar))");

        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testDbRefMissingField()
    {
        // DBRef has 3 fields (databaseName, collectionName and id)
        // Create a table without id field and verify the result
        String tableName = "test_dbref_missing_field" + randomNameSuffix();
        assertUpdate("CREATE TABLE test." + tableName + "(x row(\"databaseName\" varchar, \"collectionName\" varchar))");

        Document document = new Document()
                .append("x", new DBRef("test_db", "test_collection", 1));
        client.getDatabase("test").getCollection(tableName).insertOne(document);

        // TODO Fix MongoPageSource to throw TrinoException
        assertThat(query("SELECT * FROM test." + tableName))
                .nonTrinoExceptionFailure().hasMessageContaining("DBRef should have 3 fields : row(databaseName varchar, collectionName varchar)");

        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testDbRefWrongFieldName()
    {
        // DBRef has 3 fields databaseName, collectionName and id
        // Create a table with different field names and verify the failure
        String tableName = "test_dbref_wrong_field_name" + randomNameSuffix();
        assertUpdate("CREATE TABLE test." + tableName + "(x row(a varchar, b varchar, c int))");

        Document document = new Document()
                .append("x", new DBRef("test_db", "test_collection", 1));
        client.getDatabase("test").getCollection(tableName).insertOne(document);

        assertQueryFails("SELECT * FROM test." + tableName, "Unexpected field name for DBRef: a");

        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testDbRefWrongFieldType()
    {
        // DBRef has 3 fields (varchar databaseName, varchar collectionName and arbitrary type id)
        // Create a table with different types and verify the result
        String tableName = "test_dbref_wrong_field_type" + randomNameSuffix();
        assertUpdate("CREATE TABLE test." + tableName + "(x row(\"databaseName\" int, \"collectionName\" int, id int))");

        Document document = new Document()
                .append("x", new DBRef("test_db", "test_collection", "test_id"));
        client.getDatabase("test").getCollection(tableName).insertOne(document);

        // The connector returns NULL when the actual field value is different from the column type
        // See TODO comment in MongoPageSource
        assertThat(query("SELECT * FROM test." + tableName))
                .matches("SELECT CAST(row(NULL, NULL, NULL) AS row(\"databaseName\" int, \"collectionName\" int, id int))");

        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testMaps()
    {
        String mapIntegerTable = "test_map_integer" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + mapIntegerTable + " AS SELECT MAP(ARRAY[0,1], ARRAY[2,NULL]) AS col", 1);
        assertQuery("SELECT col[0] FROM " + mapIntegerTable, "SELECT 2");
        assertQuery("SELECT col[1] FROM " + mapIntegerTable, "SELECT NULL");
        assertUpdate("DROP TABLE " + mapIntegerTable);

        String mapDoubleTable = "test_map_double" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + mapDoubleTable + " AS SELECT MAP(ARRAY[1.0E0], ARRAY[2.5E0]) AS col", 1);
        assertQuery("SELECT col[1.0] FROM " + mapDoubleTable, "SELECT 2.5");
        assertUpdate("DROP TABLE " + mapDoubleTable);

        String mapVarcharTable = "test_map_varchar" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + mapVarcharTable + " AS SELECT MAP(ARRAY['puppies'], ARRAY['kittens']) AS col", 1);
        assertQuery("SELECT col['puppies'] FROM " + mapVarcharTable, "SELECT 'kittens'");
        assertUpdate("DROP TABLE " + mapVarcharTable);

        String mapBooleanTable = "test_map_boolean" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + mapBooleanTable + " AS SELECT MAP(ARRAY[TRUE], ARRAY[FALSE]) AS col", "SELECT 1");
        assertQuery("SELECT col[TRUE] FROM " + mapBooleanTable, "SELECT FALSE");
        assertUpdate("DROP TABLE " + mapBooleanTable);

        String mapDoubleNestedTable = "test_map_double_nested" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + mapDoubleNestedTable + " AS SELECT MAP(ARRAY[1.0E0], ARRAY[ARRAY[1, 2]]) AS col", 1);
        assertQuery("SELECT col[1.0][2] FROM " + mapDoubleNestedTable, "SELECT 2");
        assertUpdate("DROP TABLE " + mapDoubleNestedTable);

        String mapDateTable = "test_map_date" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + mapDateTable + " AS SELECT MAP(ARRAY[DATE '2014-09-30'], ARRAY[DATE '2014-09-29']) AS col", 1);
        assertOneNotNullResult("SELECT col[DATE '2014-09-30'] FROM " + mapDateTable);
        assertUpdate("DROP TABLE " + mapDateTable);

        String mapTimestampTable = "test_map_timestamp" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + mapTimestampTable + " AS SELECT MAP(ARRAY[TIMESTAMP '2001-08-22 03:04:05.321'], ARRAY[TIMESTAMP '2001-08-22 03:04:05.321']) AS col", 1);
        assertOneNotNullResult("SELECT col[TIMESTAMP '2001-08-22 03:04:05.321'] FROM " + mapTimestampTable);
        assertUpdate("DROP TABLE " + mapTimestampTable);

        String mapTable = "test_map" + randomNameSuffix();
        assertUpdate("CREATE TABLE test." + mapTable + " (col MAP<VARCHAR, VARCHAR>)");
        client.getDatabase("test").getCollection(mapTable).insertOne(new Document(
                ImmutableMap.of("col", new Document(ImmutableMap.of("key1", "value1", "key2", "value2")))));
        assertQuery("SELECT col['key1'] FROM test." + mapTable, "SELECT 'value1'");
        assertUpdate("DROP TABLE test." + mapTable);

        String simpleMapToVarcharTable = "test_simple_map_to_varchar" + randomNameSuffix();
        assertUpdate("CREATE TABLE test." + simpleMapToVarcharTable + " (col VARCHAR)");
        client.getDatabase("test").getCollection(simpleMapToVarcharTable).insertOne(new Document(
                ImmutableMap.of("col", new Document(ImmutableMap.of("key1", "value1", "key2", "value2")))));
        assertQuery("SELECT col FROM test." + simpleMapToVarcharTable, "SELECT '{\"key1\": \"value1\", \"key2\": \"value2\"}'");
        assertUpdate("DROP TABLE test." + simpleMapToVarcharTable);

        String listMapToVarcharTable = "test_list_map_to_varchar" + randomNameSuffix();
        assertUpdate("CREATE TABLE test." + listMapToVarcharTable + " (col VARCHAR)");
        client.getDatabase("test").getCollection(listMapToVarcharTable).insertOne(new Document(
                ImmutableMap.of("col", ImmutableList.of(new Document(ImmutableMap.of("key1", "value1", "key2", "value2")),
                        new Document(ImmutableMap.of("key3", "value3", "key4", "value4"))))));
        assertQuery("SELECT col FROM test." + listMapToVarcharTable, "SELECT '[{\"key1\": \"value1\", \"key2\": \"value2\"}, {\"key3\": \"value3\", \"key4\": \"value4\"}]'");
        assertUpdate("DROP TABLE test." + listMapToVarcharTable);

        String integerToVarcharTable = "test_integer_to_varchar" + randomNameSuffix();
        assertUpdate("CREATE TABLE test." + integerToVarcharTable + " (col VARCHAR)");
        client.getDatabase("test").getCollection(integerToVarcharTable).insertOne(new Document(
                ImmutableMap.of("col", 10)));
        assertQuery("SELECT col FROM test." + integerToVarcharTable, "SELECT '10'");
        assertUpdate("DROP TABLE test." + integerToVarcharTable);

        String arrayToVarcharTable = "test_array_to_varchar" + randomNameSuffix();
        assertUpdate("CREATE TABLE test." + arrayToVarcharTable + " (col VARCHAR)");
        client.getDatabase("test").getCollection(arrayToVarcharTable).insertOne(new Document(
                ImmutableMap.of("col", Arrays.asList(10, null, 11))));
        assertQuery("SELECT col FROM test." + arrayToVarcharTable, "SELECT '[10, null, 11]'");
        assertUpdate("DROP TABLE test." + arrayToVarcharTable);
    }

    @Test
    public void testCollectionNameContainsDots()
    {
        String tableName = "test.dot1_" + randomNameSuffix();
        assertUpdate("CREATE TABLE \"" + tableName + "\" AS SELECT 'foo' _varchar", 1);
        assertQuery("SELECT _varchar FROM \"" + tableName + "\"", "SELECT 'foo'");
        assertUpdate("DROP TABLE \"" + tableName + "\"");
    }

    @Test
    public void testObjectIds()
    {
        String values = "VALUES " +
                " (10, NULL, NULL)," +
                " (11, ObjectId('ffffffffffffffffffffffff'), ObjectId('ffffffffffffffffffffffff'))," +
                " (12, ObjectId('ffffffffffffffffffffffff'), ObjectId('aaaaaaaaaaaaaaaaaaaaaaaa'))," +
                " (13, ObjectId('000000000000000000000000'), ObjectId('000000000000000000000000'))," +
                " (14, ObjectId('ffffffffffffffffffffffff'), NULL)," +
                " (15, NULL, ObjectId('ffffffffffffffffffffffff'))";
        String inlineTable = format("(%s) AS t(i, one, two)", values);

        String tableName = "test_objectid_" + randomNameSuffix();
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM " + inlineTable, 6);

        // IS NULL
        assertQuery("SELECT i FROM " + inlineTable + " WHERE one IS NULL", "VALUES 10, 15");
        assertQuery("SELECT i FROM " + tableName + " WHERE one IS NULL", "SELECT 0 WHERE false"); // NULL gets replaced with new unique ObjectId in MongoPageSink, this affects other test cases

        // CAST AS varchar
        assertQuery(
                "SELECT i, CAST(one AS varchar) FROM " + inlineTable + " WHERE i <= 13",
                "VALUES (10, NULL), (11, 'ffffffffffffffffffffffff'), (12, 'ffffffffffffffffffffffff'), (13, '000000000000000000000000')");

        // EQUAL
        assertQuery("SELECT i FROM " + tableName + " WHERE one = two", "VALUES 11, 13");
        assertQuery("SELECT i FROM " + tableName + " WHERE one = ObjectId('ffffffffffffffffffffffff')", "VALUES 11, 12, 14");

        // IS DISTINCT FROM
        assertQuery("SELECT i FROM " + inlineTable + " WHERE one IS DISTINCT FROM two", "VALUES 12, 14, 15");
        assertQuery("SELECT i FROM " + inlineTable + " WHERE one IS NOT DISTINCT FROM two", "VALUES 10, 11, 13");

        assertQuery("SELECT i FROM " + tableName + " WHERE one IS DISTINCT FROM two", "VALUES 10, 12, 14, 15");
        assertQuery("SELECT i FROM " + tableName + " WHERE one IS NOT DISTINCT FROM two", "VALUES 11, 13");

        // Join on ObjectId
        assertQuery(
                format("SELECT l.i, r.i FROM (%1$s) AS l(i, one, two) JOIN (%1$s) AS r(i, one, two) ON l.one = r.two", values),
                "VALUES (11, 11), (14, 11), (11, 15), (12, 15), (12, 11), (14, 15), (13, 13)");

        // Group by ObjectId (IS DISTINCT FROM)
        assertQuery("SELECT array_agg(i ORDER BY i) FROM " + inlineTable + " GROUP BY one", "VALUES (ARRAY[10, 15]), (ARRAY[11, 12, 14]), (ARRAY[13])");
        assertQuery("SELECT i FROM " + inlineTable + " GROUP BY one, i", "VALUES 10, 11, 12, 13, 14, 15");

        // Group by Row(ObjectId) (ID DISTINCT FROM in @OperatorDependency)
        assertQuery(
                "SELECT r.i, count(*) FROM (SELECT CAST(row(one, i) AS row(one ObjectId, i bigint)) r FROM " + inlineTable + ") GROUP BY r",
                "VALUES (10, 1), (11, 1), (12, 1), (13, 1), (14, 1), (15, 1)");
        assertQuery(
                "SELECT r.x, CAST(r.one AS varchar), count(*) FROM (SELECT CAST(row(one, i / 3 * 3) AS row(one ObjectId, x bigint)) r FROM " + inlineTable + ") GROUP BY r",
                "VALUES (9, NULL, 1), (9, 'ffffffffffffffffffffffff', 1), (12, 'ffffffffffffffffffffffff', 2), (12, '000000000000000000000000', 1), (15, NULL, 1)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testSelectView()
    {
        String tableName = "test_view_base_" + randomNameSuffix();
        String viewName = "test_view_" + randomNameSuffix();
        assertUpdate("CREATE TABLE test." + tableName + " AS SELECT 'foo' _varchar", 1);
        client.getDatabase("test").createView(viewName, tableName, ImmutableList.of());
        assertQuery("SELECT * FROM test." + viewName, "SELECT 'foo'");
        assertUpdate("DROP TABLE test." + viewName);
        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testBooleanPredicates()
    {
        String tableName = "test_boolean_predicates_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(id integer, value boolean)");
        assertUpdate("INSERT INTO " + tableName + " VALUES(1, true)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES(2, false)", 1);

        assertQuery("SELECT id FROM " + tableName + " WHERE value = true", "VALUES 1");
        assertQuery("SELECT id FROM " + tableName + " WHERE value = false", "VALUES 2");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testNullPredicates()
    {
        String tableName = "test_null_predicates_" + randomNameSuffix();
        assertUpdate("CREATE TABLE test." + tableName + "(name varchar, value integer)");

        MongoCollection<Document> collection = client.getDatabase("test").getCollection(tableName);
        collection.insertOne(new Document(ImmutableMap.of("name", "abc", "value", 1)));
        collection.insertOne(new Document(ImmutableMap.of("name", "abcd")));
        collection.insertOne(new Document(Document.parse("{\"name\": \"abcde\", \"value\": null}")));

        assertQuery("SELECT count(*) FROM test." + tableName + " WHERE value IS NULL OR rand() = 42", "SELECT 2");
        assertQuery("SELECT count(*) FROM test." + tableName + " WHERE value IS NULL", "SELECT 2");
        assertQuery("SELECT count(*) FROM test." + tableName + " WHERE value IS NOT NULL", "SELECT 1");

        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testLimitPushdown()
    {
        assertThat(query("SELECT name FROM nation LIMIT 30")).isFullyPushedDown(); // Use high limit for result determinism

        // Make sure LIMIT 0 returns empty result because cursor.limit(0) means no limit in MongoDB
        assertThat(query("SELECT name FROM nation LIMIT 0")).returnsEmptyResult();

        // MongoDB doesn't support limit number greater than integer max
        assertThat(query("SELECT name FROM nation LIMIT 2147483647")).isFullyPushedDown();
        assertThat(query("SELECT name FROM nation LIMIT 2147483648")).isNotFullyPushedDown(LimitNode.class);
    }

    @Test
    public void testCollationAccent()
    {
        String tableName = "test_collation_accent" + randomNameSuffix();
        Collation collation = Collation.builder().locale("en_US").collationStrength(PRIMARY).build();
        client.getDatabase("test").createCollection(tableName, new CreateCollectionOptions().collation(collation));
        client.getDatabase("test").getCollection(tableName)
                .insertMany(ImmutableList.of(new Document("text", "e"), new Document("text", "é")));

        assertQuery("SELECT * FROM test." + tableName + " WHERE text = 'e'", "VALUES 'e'");
        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testCollationCaseSensitivity()
    {
        String tableName = "test_collation_case_sensitivity" + randomNameSuffix();
        Collation collation = Collation.builder().locale("en_US").collationCaseFirst(LOWER).build();
        client.getDatabase("test").createCollection(tableName, new CreateCollectionOptions().collation(collation));
        client.getDatabase("test").getCollection(tableName)
                .insertMany(ImmutableList.of(new Document("text", "abc"), new Document("text", "ABC")));

        assertQuery("SELECT * FROM test." + tableName + " WHERE text > 'ABC'", "VALUES 'abc'");
        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testCollationNumericOrdering()
    {
        String tableName = "test_collation_numeric_ordering" + randomNameSuffix();
        Collation collation = Collation.builder().locale("en_US").numericOrdering(true).build();
        client.getDatabase("test").createCollection(tableName, new CreateCollectionOptions().collation(collation));
        client.getDatabase("test").getCollection(tableName)
                .insertMany(ImmutableList.of(new Document("number", "-10"), new Document("number", "-2.1"), new Document("number", "1")));

        assertQuery("SELECT * FROM test." + tableName + " WHERE number > '-2.1'", "VALUES '1'");
        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    @Override
    public void testAddColumnConcurrently()
    {
        // TODO: Enable after supporting multi-document transaction https://www.mongodb.com/docs/manual/core/transactions/
        abort("TODO");
    }

    @Test
    public void testNativeQuerySimple()
    {
        assertThat(query("SELECT * FROM TABLE(mongodb.system.query(database => 'tpch', collection => 'region', filter => '{ regionkey: 1 }'))"))
                .matches("SELECT * FROM region WHERE regionkey = 1");

        // Empty filter '{}' returns all rows
        assertThat(query("SELECT * FROM TABLE(mongodb.system.query(database => 'tpch', collection => 'region', filter => '{}'))"))
                .matches("SELECT * FROM region");
    }

    @Test
    public void testNativeQueryArray()
    {
        String tableName = "test_array" + randomNameSuffix();
        MongoCollection<Document> collection = client.getDatabase("tpch").getCollection(tableName);
        collection.insertOne(new Document("array_field", ImmutableList.of("zero", "one", "two")));
        collection.insertOne(new Document("array_field", ImmutableList.of("0", "1", "2")));

        assertThat(query("SELECT array_field FROM TABLE(mongodb.system.query(database => 'tpch', collection => '" + tableName + "', filter => '{ \"array_field.1\": \"one\" }'))"))
                .matches("VALUES CAST(ARRAY['zero', 'one', 'two'] AS array(varchar))");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testNativeQueryNestedRow()
    {
        String tableName = "test_nested_row" + randomNameSuffix();
        MongoCollection<Document> collection = client.getDatabase("tpch").getCollection(tableName);
        collection.insertOne(new Document("row_field", new Document("first", new Document("second", 1))));
        collection.insertOne(new Document("row_field", new Document("first", new Document("second", 2))));

        assertThat(query("SELECT row_field.first.second FROM TABLE(mongodb.system.query(database => 'tpch', collection => '" + tableName + "', filter => '{ \"row_field.first.second\": 1 }'))"))
                .matches("VALUES BIGINT '1'")
                .isFullyPushedDown();

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testNativeQueryHelperFunction()
    {
        String tableName = "test_query_helper_function" + randomNameSuffix();
        MongoCollection<Document> collection = client.getDatabase("tpch").getCollection(tableName);
        collection.insertOne(new Document(ImmutableMap.of("id", 1, "timestamp", LocalDateTime.of(2023, 3, 20, 1, 2, 3))));
        collection.insertOne(new Document(ImmutableMap.of("id", 2, "timestamp", LocalDateTime.of(2024, 3, 20, 1, 2, 3))));

        assertQuery(
                "SELECT id FROM TABLE(mongodb.system.query(database => 'tpch', collection => '" + tableName + "', filter => '{ timestamp: ISODate(\"2023-03-20T01:02:03.000Z\") }'))",
                "VALUES 1");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testNativeQueryFilterAndWhere()
    {
        assertThat(query("SELECT * FROM TABLE(mongodb.system.query(database => 'tpch', collection => 'nation', filter => '{ regionkey: 0 }')) WHERE name = 'ALGERIA'"))
                .matches("SELECT * FROM nation WHERE regionkey = 0 AND name = 'ALGERIA'");

        assertThat(query("SELECT * FROM TABLE(mongodb.system.query(database => 'tpch', collection => 'nation', filter => '{ regionkey: {$gte: 1} }')) WHERE regionkey = 4"))
                .matches("SELECT * FROM nation WHERE regionkey >= 1 AND regionkey = 4");

        assertThat(query("SELECT * FROM TABLE(mongodb.system.query(database => 'tpch', collection => 'nation', filter => '{ regionkey: {$gte: 1} }')) WHERE regionkey < 1"))
                .returnsEmptyResult();
    }

    @Test
    public void testNativeQueryEmptyResult()
    {
        assertThat(query("SELECT * FROM TABLE(mongodb.system.query(database => 'tpch', collection => 'region', filter => '{ regionkey: 999 }'))"))
                .returnsEmptyResult();
    }

    @Test
    public void testNativeQueryLimit()
    {
        // Use high limit for result determinism
        assertThat(query("SELECT * FROM TABLE(mongodb.system.query(database => 'tpch', collection => 'region', filter => '{}')) LIMIT 30"))
                .isFullyPushedDown();

        assertThat(query("SELECT * FROM TABLE(mongodb.system.query(database => 'tpch', collection => 'region', filter => '{}')) LIMIT 0"))
                .returnsEmptyResult();
    }

    @Test
    public void testNativeQueryProjection()
    {
        assertThat(query("SELECT name FROM TABLE(mongodb.system.query(database => 'tpch', collection => 'region', filter => '{}'))"))
                .matches("SELECT name FROM region")
                .isFullyPushedDown();
    }

    @Test
    public void testNativeQueryCaseNonLowercaseColumn()
    {
        String tableName = "test_non_lowercase_column" + randomNameSuffix();
        client.getDatabase("test").getCollection(tableName)
                .insertOne(new Document("TestColumn", 1));

        assertQuery(
                "SELECT * FROM TABLE(mongodb.system.query(database => 'test', collection => '" + tableName + "', filter => '{\"TestColumn\": 1}'))",
                "VALUES 1");
        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testNativeQueryInvalidArgument()
    {
        assertQueryFails(
                "SELECT * FROM TABLE(mongodb.system.query(database => 'invalid', collection => 'region', filter => '{}'))",
                "Table 'invalid.region' not found");
        assertQueryFails(
                "SELECT * FROM TABLE(mongodb.system.query(database => 'tpch', collection => 'invalid', filter => '{}'))",
                "Table 'tpch.invalid' not found");

        assertQueryFails(
                "SELECT * FROM TABLE(mongodb.system.query(database => 'TPCH', collection => 'region', filter => '{}'))",
                "Only lowercase database name is supported");
        assertQueryFails(
                "SELECT * FROM TABLE(mongodb.system.query(database => 'tpch', collection => 'REGION', filter => '{}'))",
                 "Only lowercase collection name is supported");

        assertQueryFails(
                "SELECT * FROM TABLE(mongodb.system.query(database => 'tpch', collection => 'region', filter => '{ invalid }'))",
                "Can't parse 'filter' argument as json");
    }

    @Test
    public void testRenameTableTo120bytesTableName()
    {
        String sourceTableName = "test_rename_source_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + sourceTableName + " AS SELECT 123 x", 1);

        // The new table has 120 bytes as fully qualified identifier (あ is 3 bytes char)
        String targetTableName = "a".repeat(120 - "tpch.".length() - 3) + "あ";
        assertThat(targetTableName.length()).isLessThan(120);
        assertUpdate("ALTER TABLE " + sourceTableName + " RENAME TO \"" + targetTableName + "\"");
        assertQuery("SELECT x FROM \"" + targetTableName + "\"", "VALUES 123");
        assertUpdate("DROP TABLE \"" + targetTableName + "\"");

        targetTableName = targetTableName + "z";
        assertUpdate("CREATE TABLE " + sourceTableName + " AS SELECT 123 x", 1);
        assertQueryFails(
                "ALTER TABLE " + sourceTableName + " RENAME TO \"" + targetTableName + "\"",
                "Qualified identifier name must be shorter than or equal to '120' bytes: .*");
        assertUpdate("DROP TABLE \"" + sourceTableName + "\"");
    }

    @Test
    public void testListTablesFromSchemaWithBigAmountOfTables()
    {
        MongoDatabase database = client.getDatabase("huge_schema");
        for (int i = 0; i < 10_000; i++) {
            database.createCollection("table_" + i);
        }

        assertThat(getQueryRunner().execute("SHOW TABLES FROM mongodb.huge_schema").getRowCount()).isEqualTo(10_000);
    }

    @Test
    public void testSystemSchemas()
    {
        // Ensures that system schemas are inaccessible
        assertQueryReturnsEmptyResult("SHOW SCHEMAS IN mongodb LIKE 'admin'");
        assertQueryReturnsEmptyResult("SHOW SCHEMAS IN mongodb LIKE 'config'");
        assertQueryReturnsEmptyResult("SHOW SCHEMAS IN mongodb LIKE 'local'");
    }

    @Test
    public void testReadTopLevelDottedField()
    {
        String tableName = "test_read_top_level_dotted_field_" + randomNameSuffix();

        Document document = new Document()
                .append("_id", new ObjectId("5126bbf64aed4daf9e2ab771"))
                .append("dotted.field", "foo");
        client.getDatabase("test").getCollection(tableName).insertOne(document);

        assertThat(query("SELECT \"dotted.field\" FROM test." + tableName))
                .skippingTypesCheck()
                .matches("SELECT NULL")
                .isFullyPushedDown();

        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testReadMiddleLevelDottedField()
    {
        String tableName = "test_read_middle_level_dotted_field_" + randomNameSuffix();

        assertUpdate("CREATE TABLE test." + tableName + " (root ROW(\"dotted.field\" ROW(leaf VARCHAR)))");
        assertUpdate("INSERT INTO test." + tableName + " SELECT ROW(ROW('foo'))", 1);

        assertThat(query("SELECT root.\"dotted.field\" FROM test." + tableName))
                .skippingTypesCheck()
                .matches("SELECT ROW(varchar 'foo')")
                .isNotFullyPushedDown(ProjectNode.class);

        assertThat(query("SELECT root.\"dotted.field\".leaf FROM test." + tableName))
                .matches("SELECT varchar 'foo'")
                .isNotFullyPushedDown(ProjectNode.class);

        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testReadLeafLevelDottedField()
    {
        String tableName = "test_read_leaf_level_dotted_field_" + randomNameSuffix();

        assertUpdate("CREATE TABLE test." + tableName + " (root ROW(\"dotted.field\" VARCHAR, field VARCHAR))");
        assertUpdate("INSERT INTO test." + tableName + " SELECT ROW('foo', 'bar')", 1);

        assertThat(query("SELECT root.\"dotted.field\" FROM test." + tableName))
                .matches("SELECT varchar 'foo'")
                .isNotFullyPushedDown(ProjectNode.class);

        assertThat(query("SELECT root.\"dotted.field\", root.field FROM test." + tableName))
                .matches("SELECT varchar 'foo', varchar 'bar'")
                .isNotFullyPushedDown(ProjectNode.class);

        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testReadWithDollarPrefixedFieldName()
    {
        String tableName = "test_read_with_dollar_prefixed_field_name_" + randomNameSuffix();

        assertUpdate("CREATE TABLE test." + tableName + " (root ROW(\"$field1\" VARCHAR, field2 VARCHAR))");
        assertUpdate("INSERT INTO test." + tableName + " SELECT ROW('foo', 'bar')", 1);

        assertThat(query("SELECT root.\"$field1\" FROM test." + tableName))
                .matches("SELECT varchar 'foo'")
                .isNotFullyPushedDown(ProjectNode.class);

        assertThat(query("SELECT root.\"$field1\", root.field2 FROM test." + tableName))
                .matches("SELECT varchar 'foo', varchar 'bar'")
                .isNotFullyPushedDown(ProjectNode.class);

        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testReadWithDollarInsideFieldName()
    {
        String tableName = "test_read_with_dollar_inside_field_name_" + randomNameSuffix();

        assertUpdate("CREATE TABLE test." + tableName + " (root ROW(\"fi$ld1\" VARCHAR, field2 VARCHAR))");
        assertUpdate("INSERT INTO test." + tableName + " SELECT ROW('foo', 'bar')", 1);

        assertThat(query("SELECT root.\"fi$ld1\" FROM test." + tableName))
                .matches("SELECT varchar 'foo'")
                .isNotFullyPushedDown(ProjectNode.class);

        assertThat(query("SELECT root.\"fi$ld1\", root.field2 FROM test." + tableName))
                .matches("SELECT varchar 'foo', varchar 'bar'")
                .isNotFullyPushedDown(ProjectNode.class);

        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testReadDottedFieldInsideDollarPrefixedField()
    {
        String tableName = "test_read_dotted_field_inside_dollar_prefixed_field_" + randomNameSuffix();

        assertUpdate("CREATE TABLE test." + tableName + " (root ROW(\"$field\" ROW(\"dotted.field\" VARCHAR)))");
        assertUpdate("INSERT INTO test." + tableName + " SELECT ROW(ROW('foo'))", 1);

        assertThat(query("SELECT root.\"$field\".\"dotted.field\" FROM test." + tableName))
                .matches("SELECT varchar 'foo'")
                .isNotFullyPushedDown(ProjectNode.class);

        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testReadDollarPrefixedFieldInsideDottedField()
    {
        String tableName = "test_read_dollar_prefixed_field_inside_dotted_field_" + randomNameSuffix();

        assertUpdate("CREATE TABLE test." + tableName + " (root ROW(\"dotted.field\" ROW(\"$field\" VARCHAR)))");
        assertUpdate("INSERT INTO test." + tableName + " SELECT ROW(ROW('foo'))", 1);

        assertThat(query("SELECT root.\"dotted.field\".\"$field\" FROM test." + tableName))
                .matches("SELECT varchar 'foo'")
                .isNotFullyPushedDown(ProjectNode.class);

        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testPredicateOnDottedField()
    {
        String tableName = "test_predicate_on_dotted_field_" + randomNameSuffix();

        assertUpdate("CREATE TABLE test." + tableName + " (root ROW(\"dotted.field\" VARCHAR))");
        assertUpdate("INSERT INTO test." + tableName + " SELECT ROW('foo')", 1);

        assertThat(query("SELECT root.\"dotted.field\" FROM test." + tableName + " WHERE root.\"dotted.field\" = 'foo'"))
                .matches("SELECT varchar 'foo'")
                .isNotFullyPushedDown(FilterNode.class);

        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testPredicateOnDollarPrefixedField()
    {
        String tableName = "test_predicate_on_dollar_prefixed_field_" + randomNameSuffix();

        assertUpdate("CREATE TABLE test." + tableName + " (root ROW(\"$field\" VARCHAR))");
        assertUpdate("INSERT INTO test." + tableName + " SELECT ROW('foo')", 1);

        assertThat(query("SELECT root.\"$field\" FROM test." + tableName + " WHERE root.\"$field\" = 'foo'"))
                .matches("SELECT varchar 'foo'")
                .isNotFullyPushedDown(FilterNode.class);

        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testProjectionPushdownMixedWithUnsupportedFieldName()
    {
        String tableName = "test_projection_pushdown_mixed_with_unsupported_field_name_" + randomNameSuffix();

        assertUpdate("CREATE TABLE test." + tableName + " (id INT, root1 ROW(field VARCHAR, \"dotted.field\" VARCHAR), root2 ROW(field VARCHAR, \"$field\" VARCHAR))");
        assertUpdate("INSERT INTO test." + tableName + " SELECT 1, ROW('foo1', 'bar1'), ROW('foo2', 'bar2')", 1);

        assertThat(query("SELECT root1.field, root2.\"$field\" FROM test." + tableName))
                .matches("SELECT varchar 'foo1', varchar 'bar2'")
                .isNotFullyPushedDown(ProjectNode.class);

        assertThat(query("SELECT root1.\"dotted.field\", root2.field FROM test." + tableName))
                .matches("SELECT varchar 'bar1', varchar 'foo2'")
                .isNotFullyPushedDown(ProjectNode.class);

        assertThat(query("SELECT root1.\"dotted.field\", root2.\"$field\" FROM test." + tableName))
                .matches("SELECT varchar 'bar1', varchar 'bar2'")
                .isNotFullyPushedDown(ProjectNode.class);

        assertThat(query("SELECT root1.field, root2.field FROM test." + tableName))
                .matches("SELECT varchar 'foo1', varchar 'foo2'")
                .isFullyPushedDown();

        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testFiltersOnDereferenceColumnReadsLessData()
    {
        testFiltersOnDereferenceColumnReadsLessData("varchar 'String type'", "varchar");
        testFiltersOnDereferenceColumnReadsLessData("to_utf8('BinData')", "varbinary");
        testFiltersOnDereferenceColumnReadsLessData("bigint '1234567890'", "bigint");
        testFiltersOnDereferenceColumnReadsLessData("true", "boolean");
        testFiltersOnDereferenceColumnReadsLessData("double '12.3'", "double");
        testFiltersOnDereferenceColumnReadsLessData("timestamp '1970-01-01 00:00:00.000'", "timestamp(3)");
        testFiltersOnDereferenceColumnReadsLessData("array[bigint '1']", "array(bigint)");
        testFiltersOnDereferenceColumnReadsLessData("ObjectId('5126bc054aed4daf9e2ab772')", "ObjectId");
    }

    private void testFiltersOnDereferenceColumnReadsLessData(String expectedValue, String expectedType)
    {
        if (!isPushdownSupportedType(getQueryRunner().getPlannerContext().getTypeManager().fromSqlType(expectedType))) {
            abort("Type doesn't support filter pushdown");
        }

        Session sessionWithoutPushdown = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "projection_pushdown_enabled", "false")
                .build();

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "filter_on_projection_columns",
                format("(col_0 ROW(col_1 %1$s, col_2 ROW(col_3 %1$s, col_4 ROW(col_5 %1$s))))", expectedType))) {
            assertUpdate(format("INSERT INTO %s VALUES NULL", table.getName()), 1);
            assertUpdate(format("INSERT INTO %1$s SELECT ROW(%2$s, ROW(%2$s, ROW(%2$s)))", table.getName(), expectedValue), 1);
            assertUpdate(format("INSERT INTO %1$s SELECT ROW(%2$s, ROW(NULL, ROW(%2$s)))", table.getName(), expectedValue), 1);

            Set<Object> expected = ImmutableSet.of(1);

            assertQueryStats(
                    getSession(),
                    format("SELECT 1 FROM %s WHERE col_0.col_1 = %s", table.getName(), expectedValue),
                    statsWithPushdown -> {
                        long processedInputPositionWithPushdown = statsWithPushdown.getProcessedInputPositions();
                        assertQueryStats(
                                sessionWithoutPushdown,
                                format("SELECT 1 FROM %s WHERE col_0.col_1 = %s", table.getName(), expectedValue),
                                statsWithoutPushdown -> {
                                    assertThat(statsWithoutPushdown.getProcessedInputPositions())
                                            .isEqualTo(3);
                                    assertThat(processedInputPositionWithPushdown)
                                            .isEqualTo(2);
                                    assertThat(statsWithoutPushdown.getProcessedInputPositions()).isGreaterThan(processedInputPositionWithPushdown);
                                },
                                results -> assertThat(results.getOnlyColumnAsSet())
                                        .isEqualTo(expected));
                    },
                    results -> assertThat(results.getOnlyColumnAsSet())
                            .isEqualTo(expected));

            assertQueryStats(
                    getSession(),
                    format("SELECT 1 FROM %s WHERE col_0.col_2.col_3 = %s", table.getName(), expectedValue),
                    statsWithPushdown -> {
                        long processedInputPositionWithPushdown = statsWithPushdown.getProcessedInputPositions();
                        assertQueryStats(
                                sessionWithoutPushdown,
                                format("SELECT 1 FROM %s WHERE col_0.col_2.col_3 = %s", table.getName(), expectedValue),
                                statsWithoutPushdown -> {
                                    assertThat(statsWithoutPushdown.getProcessedInputPositions())
                                            .isEqualTo(3);
                                    assertThat(processedInputPositionWithPushdown)
                                            .isEqualTo(1);
                                    assertThat(statsWithoutPushdown.getProcessedInputPositions()).isGreaterThan(processedInputPositionWithPushdown);
                                },
                                results -> assertThat(results.getOnlyColumnAsSet())
                                        .isEqualTo(expected));
                    },
                    results -> assertThat(results.getOnlyColumnAsSet())
                            .isEqualTo(expected));

            assertQueryStats(
                    getSession(),
                    format("SELECT 1 FROM %s WHERE col_0.col_2.col_4.col_5 = %s", table.getName(), expectedValue),
                    statsWithPushdown -> {
                        long processedInputPositionWithPushdown = statsWithPushdown.getProcessedInputPositions();
                        assertQueryStats(
                                sessionWithoutPushdown,
                                format("SELECT 1 FROM %s WHERE col_0.col_2.col_4.col_5 = %s", table.getName(), expectedValue),
                                statsWithoutPushdown -> {
                                    assertThat(statsWithoutPushdown.getProcessedInputPositions())
                                            .isEqualTo(3);
                                    assertThat(processedInputPositionWithPushdown)
                                            .isEqualTo(2);
                                    assertThat(statsWithoutPushdown.getProcessedInputPositions()).isGreaterThan(processedInputPositionWithPushdown);
                                },
                                results -> assertThat(results.getOnlyColumnAsSet())
                                        .isEqualTo(expected));
                    },
                    results -> assertThat(results.getOnlyColumnAsSet())
                            .isEqualTo(expected));
        }
    }

    @Test
    public void testFiltersOnDereferenceColumnReadsLessDataNativeQuery()
    {
        String tableName = "test_filter_on_dereference_column_reads_less_data_native_query_" + randomNameSuffix();

        MongoCollection<Document> collection = client.getDatabase("test").getCollection(tableName);
        collection.insertOne(new Document("row_field", new Document("first", new Document("second", 1))));
        collection.insertOne(new Document("row_field", new Document("first", new Document("second", null))));
        collection.insertOne(new Document("row_field", new Document("first", null)));

        assertQueryStats(
                getSession(),
                "SELECT row_field.first.second FROM TABLE(mongodb.system.query(database => 'test', collection => '" + tableName + "', filter => '{ \"row_field.first.second\": 1 }'))",
                stats -> assertThat(stats.getProcessedInputPositions())
                        .isEqualTo(1L),
                results -> assertThat(results.getOnlyColumnAsSet())
                        .isEqualTo(ImmutableSet.of(1L)));

        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testFilterPushdownOnFieldInsideJson()
    {
        String tableName = "test_filter_pushdown_on_json_" + randomNameSuffix();
        assertUpdate("CREATE TABLE test." + tableName + " (id INT, col JSON)");

        assertUpdate("INSERT INTO test." + tableName + " VALUES (1, JSON '{\"name\": { \"first\": \"Monika\", \"last\": \"Geller\" }}')", 1);
        assertUpdate("INSERT INTO test." + tableName + " VALUES (2, JSON '{\"name\": { \"first\": \"Rachel\", \"last\": \"Green\" }}')", 1);

        assertThat(query("SELECT json_extract_scalar(col, '$.name.first') FROM test." + tableName + " WHERE json_extract_scalar(col, '$.name.last') = 'Geller'"))
                .matches("SELECT varchar 'Monika'")
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query("SELECT 1 FROM test." + tableName + " WHERE json_extract_scalar(col, '$.name.last') = 'Geller'"))
                .matches("SELECT 1")
                .isNotFullyPushedDown(FilterNode.class);

        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testProjectionPushdownWithDifferentTypeInDocuments()
    {
        String tableName = "test_projection_pushdown_with_different_type_in_document_" + randomNameSuffix();

        assertUpdate("CREATE TABLE test." + tableName + " (col1 ROW(child VARCHAR))");

        MongoCollection<Document> collection = client.getDatabase("test").getCollection(tableName);
        collection.insertOne(new Document("col1", 100));
        collection.insertOne(new Document("col1", new Document("child", "value1")));

        assertThat(query("SELECT col1.child FROM test." + tableName))
                .skippingTypesCheck()
                .matches("VALUES ('value1'), (NULL)")
                .isFullyPushedDown();

        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testProjectionPushdownWithColumnMissingInDocument()
    {
        String tableName = "test_projection_pushdown_with_column_missing_in_document_" + randomNameSuffix();

        assertUpdate("CREATE TABLE test." + tableName + " (col1 ROW(child VARCHAR))");

        MongoCollection<Document> collection = client.getDatabase("test").getCollection(tableName);
        collection.insertOne(new Document("col1", new Document("child1", "value1")));
        collection.insertOne(new Document("col1", new Document("child", "value2")));

        assertThat(query("SELECT col1.child FROM test." + tableName))
                .skippingTypesCheck()
                .matches("VALUES ('value2'), (NULL)")
                .isFullyPushedDown();

        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testProjectionPushdownWithDBRef()
    {
        testProjectionPushdownWithDBRef("String type", "varchar 'String type'", "varchar");
        testProjectionPushdownWithDBRef("BinData".getBytes(UTF_8), "to_utf8('BinData')", "varbinary");
        testProjectionPushdownWithDBRef(1234567890, "bigint '1234567890'", "bigint");
        testProjectionPushdownWithDBRef(true, "true", "boolean");
        testProjectionPushdownWithDBRef(12.3f, "double '12.3'", "double");
        testProjectionPushdownWithDBRef(new Date(0), "timestamp '1970-01-01 00:00:00.000'", "timestamp(3)");
        testProjectionPushdownWithDBRef(ImmutableList.of(1), "array[bigint '1']", "array(bigint)");
        testProjectionPushdownWithDBRef(new ObjectId("5126bc054aed4daf9e2ab772"), "ObjectId('5126bc054aed4daf9e2ab772')", "ObjectId");
    }

    private void testProjectionPushdownWithDBRef(Object objectId, String expectedValue, String expectedType)
    {
        String tableName = "test_projection_pushdown_with_dbref_" + randomNameSuffix();

        DBRef dbRef = new DBRef("test", "creators", objectId);
        Document document = new Document()
                .append("_id", new ObjectId("5126bbf64aed4daf9e2ab771"))
                .append("col1", "foo")
                .append("creator", dbRef)
                .append("parent", new Document("child", objectId));

        client.getDatabase("test").getCollection(tableName).insertOne(document);

        assertThat(query("SELECT parent.child, creator.databaseName, creator.collectionName, creator.id FROM test." + tableName))
                .matches("SELECT " + expectedValue + ", varchar 'test', varchar 'creators', " + expectedValue)
                .isNotFullyPushedDown(ProjectNode.class);
        assertQuery(
                "SELECT typeof(creator) FROM test." + tableName,
                "SELECT 'row(databaseName varchar, collectionName varchar, id " + expectedType + ")'");

        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testProjectionPushdownWithNestedDBRef()
    {
        testProjectionPushdownWithNestedDBRef("String type", "varchar 'String type'", "varchar");
        testProjectionPushdownWithNestedDBRef("BinData".getBytes(UTF_8), "to_utf8('BinData')", "varbinary");
        testProjectionPushdownWithNestedDBRef(1234567890, "bigint '1234567890'", "bigint");
        testProjectionPushdownWithNestedDBRef(true, "true", "boolean");
        testProjectionPushdownWithNestedDBRef(12.3f, "double '12.3'", "double");
        testProjectionPushdownWithNestedDBRef(new Date(0), "timestamp '1970-01-01 00:00:00.000'", "timestamp(3)");
        testProjectionPushdownWithNestedDBRef(ImmutableList.of(1), "array[bigint '1']", "array(bigint)");
        testProjectionPushdownWithNestedDBRef(new ObjectId("5126bc054aed4daf9e2ab772"), "ObjectId('5126bc054aed4daf9e2ab772')", "ObjectId");
    }

    private void testProjectionPushdownWithNestedDBRef(Object objectId, String expectedValue, String expectedType)
    {
        String tableName = "test_projection_pushdown_with_dbref_" + randomNameSuffix();

        DBRef dbRef = new DBRef("test", "creators", objectId);
        Document document = new Document()
                .append("_id", new ObjectId("5126bbf64aed4daf9e2ab771"))
                .append("col1", "foo")
                .append("parent", new Document()
                        .append("creator", dbRef)
                        .append("child", objectId));

        client.getDatabase("test").getCollection(tableName).insertOne(document);

        assertThat(query("SELECT parent.child, parent.creator.databaseName, parent.creator.collectionName, parent.creator.id FROM test." + tableName))
                .matches("SELECT " + expectedValue + ", varchar 'test', varchar 'creators', " + expectedValue)
                .isNotFullyPushedDown(ProjectNode.class);
        assertQuery(
                "SELECT typeof(parent.creator) FROM test." + tableName,
                "SELECT 'row(databaseName varchar, collectionName varchar, id " + expectedType + ")'");

        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testProjectionPushdownWithPredefinedDBRefKeyword()
    {
        testProjectionPushdownWithPredefinedDBRefKeyword("String type", "varchar 'String type'", "varchar");
        testProjectionPushdownWithPredefinedDBRefKeyword("BinData".getBytes(UTF_8), "to_utf8('BinData')", "varbinary");
        testProjectionPushdownWithPredefinedDBRefKeyword(1234567890, "bigint '1234567890'", "bigint");
        testProjectionPushdownWithPredefinedDBRefKeyword(true, "true", "boolean");
        testProjectionPushdownWithPredefinedDBRefKeyword(12.3f, "double '12.3'", "double");
        testProjectionPushdownWithPredefinedDBRefKeyword(new Date(0), "timestamp '1970-01-01 00:00:00.000'", "timestamp(3)");
        testProjectionPushdownWithPredefinedDBRefKeyword(ImmutableList.of(1), "array[bigint '1']", "array(bigint)");
        testProjectionPushdownWithPredefinedDBRefKeyword(new ObjectId("5126bc054aed4daf9e2ab772"), "ObjectId('5126bc054aed4daf9e2ab772')", "ObjectId");
    }

    private void testProjectionPushdownWithPredefinedDBRefKeyword(Object objectId, String expectedValue, String expectedType)
    {
        String tableName = "test_projection_pushdown_with_predefined_dbref_keyword_" + randomNameSuffix();

        DBRef dbRef = new DBRef("test", "creators", objectId);
        Document document = new Document()
                .append("_id", new ObjectId("5126bbf64aed4daf9e2ab771"))
                .append("col1", "foo")
                .append("parent", new Document("id", dbRef));

        client.getDatabase("test").getCollection(tableName).insertOne(document);

        assertThat(query("SELECT parent.id, parent.id.id FROM test." + tableName))
                .skippingTypesCheck()
                .matches("SELECT row('test', 'creators', %1$s), %1$s".formatted(expectedValue))
                .isNotFullyPushedDown(ProjectNode.class);
        assertQuery(
                "SELECT typeof(parent.id), typeof(parent.id.id) FROM test." + tableName,
                "SELECT 'row(databaseName varchar, collectionName varchar, id %1$s)', '%1$s'".formatted(expectedType));

        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testDBRefLikeDocument()
    {
        testDBRefLikeDocument("String type", "varchar 'String type'");
        testDBRefLikeDocument("BinData".getBytes(UTF_8), "to_utf8('BinData')");
        testDBRefLikeDocument(1234567890, "bigint '1234567890'");
        testDBRefLikeDocument(true, "true");
        testDBRefLikeDocument(12.3f, "double '12.3'");
        testDBRefLikeDocument(new Date(0), "timestamp '1970-01-01 00:00:00.000'");
        testDBRefLikeDocument(ImmutableList.of(1), "array[bigint '1']");
        testDBRefLikeDocument(new ObjectId("5126bc054aed4daf9e2ab772"), "ObjectId('5126bc054aed4daf9e2ab772')");

        testDBRefLikeDocument(dbRefDocument("String type"), documentWithSameDbRefFieldOrder("String type"), "varchar 'String type'");
        testDBRefLikeDocument(dbRefDocument("String type"), getDocumentWithDifferentDbRefFieldOrder("String type"), "varchar 'String type'");
        testDBRefLikeDocument(documentWithSameDbRefFieldOrder("String type"), dbRefDocument("String type"), "varchar 'String type'");

        testDBRefLikeDocument(dbRefDocument("BinData".getBytes(UTF_8)), documentWithSameDbRefFieldOrder("BinData".getBytes(UTF_8)), "to_utf8('BinData')");
        testDBRefLikeDocument(dbRefDocument("BinData".getBytes(UTF_8)), getDocumentWithDifferentDbRefFieldOrder("BinData".getBytes(UTF_8)), "to_utf8('BinData')");
        testDBRefLikeDocument(documentWithSameDbRefFieldOrder("BinData".getBytes(UTF_8)), dbRefDocument("BinData".getBytes(UTF_8)), "to_utf8('BinData')");

        testDBRefLikeDocument(dbRefDocument(1234567890), documentWithSameDbRefFieldOrder(1234567890), "bigint '1234567890'");
        testDBRefLikeDocument(dbRefDocument(1234567890), getDocumentWithDifferentDbRefFieldOrder(1234567890), "bigint '1234567890'");
        testDBRefLikeDocument(documentWithSameDbRefFieldOrder(1234567890), dbRefDocument(1234567890), "bigint '1234567890'");

        testDBRefLikeDocument(dbRefDocument(true), documentWithSameDbRefFieldOrder(true), "true");
        testDBRefLikeDocument(dbRefDocument(true), getDocumentWithDifferentDbRefFieldOrder(true), "true");
        testDBRefLikeDocument(documentWithSameDbRefFieldOrder(true), dbRefDocument(true), "true");

        testDBRefLikeDocument(dbRefDocument(12.3f), documentWithSameDbRefFieldOrder(12.3f), "double '12.3'");
        testDBRefLikeDocument(dbRefDocument(12.3f), getDocumentWithDifferentDbRefFieldOrder(12.3f), "double '12.3'");
        testDBRefLikeDocument(documentWithSameDbRefFieldOrder(12.3f), dbRefDocument(12.3f), "double '12.3'");

        testDBRefLikeDocument(dbRefDocument(new Date(0)), documentWithSameDbRefFieldOrder(new Date(0)), "timestamp '1970-01-01 00:00:00.000'");
        testDBRefLikeDocument(dbRefDocument(new Date(0)), getDocumentWithDifferentDbRefFieldOrder(new Date(0)), "timestamp '1970-01-01 00:00:00.000'");
        testDBRefLikeDocument(documentWithSameDbRefFieldOrder(new Date(0)), dbRefDocument(new Date(0)), "timestamp '1970-01-01 00:00:00.000'");

        testDBRefLikeDocument(dbRefDocument(ImmutableList.of(1)), documentWithSameDbRefFieldOrder(ImmutableList.of(1)), "array[bigint '1']");
        testDBRefLikeDocument(dbRefDocument(ImmutableList.of(1)), getDocumentWithDifferentDbRefFieldOrder(ImmutableList.of(1)), "array[bigint '1']");
        testDBRefLikeDocument(documentWithSameDbRefFieldOrder(ImmutableList.of(1)), dbRefDocument(ImmutableList.of(1)), "array[bigint '1']");

        testDBRefLikeDocument(dbRefDocument(new ObjectId("5126bc054aed4daf9e2ab772")), documentWithSameDbRefFieldOrder(new ObjectId("5126bc054aed4daf9e2ab772")), "ObjectId('5126bc054aed4daf9e2ab772')");
        testDBRefLikeDocument(dbRefDocument(new ObjectId("5126bc054aed4daf9e2ab772")), getDocumentWithDifferentDbRefFieldOrder(new ObjectId("5126bc054aed4daf9e2ab772")), "ObjectId('5126bc054aed4daf9e2ab772')");
        testDBRefLikeDocument(documentWithSameDbRefFieldOrder(new ObjectId("5126bc054aed4daf9e2ab772")), dbRefDocument(new ObjectId("5126bc054aed4daf9e2ab772")), "ObjectId('5126bc054aed4daf9e2ab772')");
    }

    private void testDBRefLikeDocument(Document document1, Document document2, String expectedValue)
    {
        String tableName = "test_dbref_like_document_" + randomNameSuffix();

        client.getDatabase("test").getCollection(tableName).insertOne(document1);
        client.getDatabase("test").getCollection(tableName).insertOne(document2);

        assertThat(query("SELECT * FROM test." + tableName))
                .skippingTypesCheck()
                .matches("VALUES"
                        + " ROW(ROW(varchar 'dbref_test', varchar 'dbref_creators', " + expectedValue + ")),"
                        + " ROW(ROW(varchar 'doc_test', varchar 'doc_creators', " + expectedValue + "))")
                .isFullyPushedDown();

        assertThat(query("SELECT creator.id FROM test." + tableName))
                .skippingTypesCheck()
                .matches("VALUES (%1$s), (%1$s)".formatted(expectedValue))
                .isNotFullyPushedDown(ProjectNode.class);

        assertThat(query("SELECT creator.databasename, creator.collectionname, creator.id FROM test." + tableName))
                .skippingTypesCheck()
                .matches("VALUES ('doc_test', 'doc_creators', %1$s), ('dbref_test', 'dbref_creators', %1$s)".formatted(expectedValue))
                .isNotFullyPushedDown(ProjectNode.class);

        assertUpdate("DROP TABLE test." + tableName);
    }

    private static Document getDocumentWithDifferentDbRefFieldOrder(Object objectId)
    {
        return new Document()
                .append("_id", new ObjectId("5126bbf64aed4daf9e2ab771"))
                .append("creator", new Document().append("collectionName", "doc_creators").append("id", objectId).append("databaseName", "doc_test"));
    }

    private static Document documentWithSameDbRefFieldOrder(Object objectId)
    {
        return new Document()
                .append("_id", new ObjectId("5126bbf64aed4daf9e2ab771"))
                .append("creator", new Document().append("databaseName", "doc_test").append("collectionName", "doc_creators").append("id", objectId));
    }

    private static Document dbRefDocument(Object objectId)
    {
        return new Document()
                .append("_id", new ObjectId("5126bbf64aed4daf9e2ab772"))
                .append("creator", new DBRef("dbref_test", "dbref_creators", objectId));
    }

    private void testDBRefLikeDocument(Object objectId, String expectedValue)
    {
        String tableName = "test_dbref_like_document_fails_" + randomNameSuffix();

        Document documentWithDifferentDbRefFieldOrder = new Document()
                .append("_id", new ObjectId("5126bbf64aed4daf9e2ab771"))
                .append("creator", new Document()
                        .append("databaseName", "doc_test")
                        .append("collectionName", "doc_creators")
                        .append("id", objectId));
        Document dbRefDocument = new Document()
                .append("_id", new ObjectId("5126bbf64aed4daf9e2ab772"))
                .append("creator", new DBRef("dbref_test", "dbref_creators", objectId));
        client.getDatabase("test").getCollection(tableName).insertOne(documentWithDifferentDbRefFieldOrder);
        client.getDatabase("test").getCollection(tableName).insertOne(dbRefDocument);

        assertThat(query("SELECT * FROM test." + tableName))
                .skippingTypesCheck()
                .matches("VALUES "
                        + " row(row('doc_test', 'doc_creators', " + expectedValue + ")),"
                        + " row(row('dbref_test', 'dbref_creators', " + expectedValue + "))");

        assertThat(query("SELECT creator.id FROM test." + tableName))
                .skippingTypesCheck()
                .matches("VALUES " + "(%1$s), (%1$s)".formatted(expectedValue));

        assertThat(query("SELECT creator.databasename, creator.collectionname, creator.id FROM test." + tableName))
                .skippingTypesCheck()
                .matches("VALUES " + "('doc_test', 'doc_creators', %1$s), ('dbref_test', 'dbref_creators', %1$s)".formatted(expectedValue));

        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testPredicateOnDBRefField()
    {
        testPredicateOnDBRefField(true, "true");
        testPredicateOnDBRefField(4, "bigint '4'");
        testPredicateOnDBRefField("test", "'test'");
        testPredicateOnDBRefField(new ObjectId("6216f0c6c432d45190f25e7c"), "ObjectId('6216f0c6c432d45190f25e7c')");
        testPredicateOnDBRefField(new Date(0), "timestamp '1970-01-01 00:00:00.000'");
    }

    private void testPredicateOnDBRefField(Object objectId, String expectedValue)
    {
        String tableName = "test_predicate_on_dbref_field_" + randomNameSuffix();

        Document document = new Document()
                .append("_id", new ObjectId("5126bbf64aed4daf9e2ab771"))
                .append("creator", new DBRef("test", "creators", objectId));

        client.getDatabase("test").getCollection(tableName).insertOne(document);

        assertThat(query("SELECT * FROM test." + tableName + " WHERE creator.id = " + expectedValue))
                .skippingTypesCheck()
                .matches("SELECT ROW(varchar 'test', varchar 'creators', " + expectedValue + ")")
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query("SELECT creator.id FROM test." + tableName + " WHERE creator.id = " + expectedValue))
                .skippingTypesCheck()
                .matches("SELECT " + expectedValue)
                .isNotFullyPushedDown(FilterNode.class);

        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    public void testPredicateOnDBRefLikeDocument()
    {
        testPredicateOnDBRefLikeDocument(true, "true");
        testPredicateOnDBRefLikeDocument(4, "bigint '4'");
        testPredicateOnDBRefLikeDocument("test", "'test'");
        testPredicateOnDBRefLikeDocument(new ObjectId("6216f0c6c432d45190f25e7c"), "ObjectId('6216f0c6c432d45190f25e7c')");
        testPredicateOnDBRefLikeDocument(new Date(0), "timestamp '1970-01-01 00:00:00.000'");
    }

    private void testPredicateOnDBRefLikeDocument(Object objectId, String expectedValue)
    {
        String tableName = "test_predicate_on_dbref_like_document_" + randomNameSuffix();

        Document document = new Document()
                .append("_id", new ObjectId("5126bbf64aed4daf9e2ab771"))
                .append("creator", new Document()
                        .append("databaseName", "test")
                        .append("collectionName", "creators")
                        .append("id", objectId));

        client.getDatabase("test").getCollection(tableName).insertOne(document);

        assertThat(query("SELECT * FROM test." + tableName + " WHERE creator.id = " + expectedValue))
                .skippingTypesCheck()
                .matches("SELECT ROW(varchar 'test', varchar 'creators', " + expectedValue + ")")
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query("SELECT creator.id FROM test." + tableName + " WHERE creator.id = " + expectedValue))
                .skippingTypesCheck()
                .matches("SELECT " + expectedValue)
                .isNotFullyPushedDown(FilterNode.class);

        assertUpdate("DROP TABLE test." + tableName);
    }

    @Test
    @Override
    public void testProjectionPushdownReadsLessData()
    {
        // TODO https://github.com/trinodb/trino/issues/17713
        abort("MongoDB connector does not calculate physical data input size");
    }

    @Test
    @Override
    public void testProjectionPushdownPhysicalInputSize()
    {
        // TODO https://github.com/trinodb/trino/issues/17713
        abort("MongoDB connector does not calculate physical data input size");
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(63);
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("Invalid database name");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(120 - "tpch.".length());
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching(".*fully qualified namespace .* is too long.*|Qualified identifier name must be shorter than or equal to '120'.*");
    }

    @Override
    protected void verifySetColumnTypeFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("Cannot change type");
    }

    @Override
    protected Optional<SetColumnTypeSetup> filterSetColumnTypesDataProvider(SetColumnTypeSetup setup)
    {
        switch ("%s -> %s".formatted(setup.sourceColumnType(), setup.newColumnType())) {
            case "bigint -> integer":
            case "decimal(5,3) -> decimal(5,2)":
            case "time(3) -> time(6)":
            case "time(6) -> time(3)":
            case "timestamp(3) -> timestamp(6)":
            case "timestamp(6) -> timestamp(3)":
            case "timestamp(3) with time zone -> timestamp(6) with time zone":
            case "timestamp(6) with time zone -> timestamp(3) with time zone":
                return Optional.of(setup.asUnsupported());
        }
        return Optional.of(setup);
    }

    private void assertOneNotNullResult(String query)
    {
        MaterializedResult results = getQueryRunner().execute(getSession(), query).toTestTypes();
        assertThat(results.getRowCount())
                .isEqualTo(1);
        assertThat(results.getMaterializedRows().get(0).getFieldCount())
                .isEqualTo(1);
        assertThat(results.getMaterializedRows().get(0).getField(0)).isNotNull();
    }
}
