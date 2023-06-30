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
import com.mongodb.DBRef;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CreateCollectionOptions;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;
import java.util.OptionalInt;

import static com.mongodb.client.model.CollationCaseFirst.LOWER;
import static com.mongodb.client.model.CollationStrength.PRIMARY;
import static io.trino.plugin.mongodb.MongoQueryRunner.createMongoClient;
import static io.trino.plugin.mongodb.MongoQueryRunner.createMongoQueryRunner;
import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

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

    @BeforeClass
    public void initTestSchema()
    {
        assertUpdate("CREATE SCHEMA IF NOT EXISTS test");
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        server.close();
        server = null;
        client.close();
        client = null;
    }

    @SuppressWarnings("DuplicateBranchesInSwitch")
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_DELETE:
                return true;
            case SUPPORTS_UPDATE:
            case SUPPORTS_MERGE:
            case SUPPORTS_TRUNCATE:
                return false;

            case SUPPORTS_DEREFERENCE_PUSHDOWN:
                return false;

            case SUPPORTS_RENAME_SCHEMA:
                return false;

            case SUPPORTS_DROP_FIELD:
                return false;

            case SUPPORTS_CREATE_VIEW:
            case SUPPORTS_CREATE_MATERIALIZED_VIEW:
                return false;

            case SUPPORTS_NOT_NULL_CONSTRAINT:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        throw new SkipException("MongoDB connector does not support column default values");
    }

    @Test(dataProvider = "testColumnNameDataProvider")
    @Override
    public void testColumnName(String columnName)
    {
        if (columnName.equals("a.dot")) {
            assertThatThrownBy(() -> super.testColumnName(columnName))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessage("Column name must not contain '$' or '.' for INSERT: " + columnName);
            throw new SkipException("Insert would fail");
        }

        super.testColumnName(columnName);
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

    @Test(dataProvider = "guessFieldTypesProvider")
    public void testGuessFieldTypes(String mongoValue, String trinoValue)
    {
        String tableName = "test_guess_field_type_" + randomNameSuffix();
        Document document = Document.parse(format("{\"test\":%s}", mongoValue));

        assertUpdate("DROP TABLE IF EXISTS test." + tableName);
        client.getDatabase("test").getCollection(tableName).insertOne(document);

        assertThat(query("SELECT test FROM test." + tableName))
                .matches("SELECT " + trinoValue);

        assertUpdate("DROP TABLE test." + tableName);
    }

    @DataProvider
    public Object[][] guessFieldTypesProvider()
    {
        return new Object[][] {
                {"true", "true"}, // boolean -> boolean
                {"2147483647", "bigint '2147483647'"}, // int32 -> bigint
                {"{\"$numberLong\": \"9223372036854775807\"}", "9223372036854775807"}, // int64 -> bigint
                {"1.23", "double '1.23'"}, // double -> double
                {"{\"$date\": \"1970-01-01T00:00:00.000Z\"}", "timestamp '1970-01-01 00:00:00.000'"}, // date -> timestamp(3)
                {"'String type'", "varchar 'String type'"}, // string -> varchar
                {"{$binary: \"\",\"$type\": \"0\"}", "to_utf8('')"}, // binary -> varbinary
                {"{\"$oid\": \"6216f0c6c432d45190f25e7c\"}", "ObjectId('6216f0c6c432d45190f25e7c')"}, // objectid -> objectid
                {"[1]", "array[bigint '1']"}, // array with single type -> array
                {"{\"field\": \"object\"}", "CAST(row('object') AS row(field varchar))"}, // object -> row
                {"[9, \"test\"]", "CAST(row(9, 'test') AS row(_pos1 bigint, _pos2 varchar))"}, // array with multiple types -> row
                {"{\"$ref\":\"test_ref\",\"$id\":ObjectId(\"4e3f33de6266b5845052c02c\"),\"$db\":\"test_db\"}", "CAST(row('test_db', 'test_ref', ObjectId('4e3f33de6266b5845052c02c')) AS row(databasename varchar, collectionname varchar, id ObjectId))"}, // dbref -> row
        };
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
        assertEquals(results.getRowCount(), 1);
        MaterializedRow row = results.getMaterializedRows().get(0);
        assertEquals(row.getField(0), "foo");
        assertEquals(row.getField(1), "bar".getBytes(UTF_8));
        assertEquals(row.getField(2), 1L);
        assertEquals(row.getField(3), 3.14);
        assertEquals(row.getField(4), true);
        assertEquals(row.getField(5), LocalDate.of(1980, 5, 7));
        assertEquals(row.getField(6), LocalDateTime.of(1980, 5, 7, 11, 22, 33, 456_000_000));
        assertEquals(row.getField(8), "{\"name\":\"alice\"}");
        assertEquals(row.getField(9), new BigDecimal("12.30000"));
        assertUpdate("DROP TABLE " + tableName);

        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
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
        assertEquals(results.getRowCount(), 1);
        MaterializedRow row = results.getMaterializedRows().get(0);
        assertEquals(row.getField(0), "foo");
        assertEquals(row.getField(1), "bar".getBytes(UTF_8));
        assertEquals(row.getField(2), 1L);
        assertEquals(row.getField(3), 3.14);
        assertEquals(row.getField(4), true);
        assertEquals(row.getField(5), LocalDate.of(1980, 5, 7));
        assertEquals(row.getField(6), LocalDateTime.of(1980, 5, 7, 11, 22, 33, 456_000_000));
        assertEquals(row.getField(8), "{\"name\":\"alice\"}");
        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Override
    public void testDeleteWithComplexPredicate()
    {
        assertThatThrownBy(super::testDeleteWithComplexPredicate)
                .hasStackTraceContaining("TrinoException: " + MODIFYING_ROWS_MESSAGE);
    }

    @Override
    public void testDeleteWithLike()
    {
        assertThatThrownBy(super::testDeleteWithLike)
                .hasStackTraceContaining("TrinoException: " + MODIFYING_ROWS_MESSAGE);
    }

    @Override
    public void testDeleteWithSemiJoin()
    {
        assertThatThrownBy(super::testDeleteWithSemiJoin)
                .hasStackTraceContaining("TrinoException: " + MODIFYING_ROWS_MESSAGE);
    }

    @Override
    public void testDeleteWithSubquery()
    {
        assertThatThrownBy(super::testDeleteWithSubquery)
                .hasStackTraceContaining("TrinoException: " + MODIFYING_ROWS_MESSAGE);
    }

    @Override
    public void testExplainAnalyzeWithDeleteWithSubquery()
    {
        assertThatThrownBy(super::testExplainAnalyzeWithDeleteWithSubquery)
                .hasStackTraceContaining("TrinoException: " + MODIFYING_ROWS_MESSAGE);
    }

    @Test(dataProvider = "predicatePushdownProvider")
    public void testPredicatePushdown(String value)
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_predicate_pushdown", "AS SELECT %s col".formatted(value))) {
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE col = " + value + ""))
                    .isFullyPushedDown();
        }
    }

    @DataProvider
    public Object[][] predicatePushdownProvider()
    {
        return new Object[][] {
                {"true"},
                {"tinyint '1'"},
                {"smallint '2'"},
                {"integer '3'"},
                {"bigint '4'"},
                {"'test'"},
                {"objectid('6216f0c6c432d45190f25e7c')"},
                {"date '1970-01-01'"},
                {"time '00:00:00.000'"},
                {"timestamp '1970-01-01 00:00:00.000'"},
                {"timestamp '1970-01-01 00:00:00.000 UTC'"},
        };
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

    @Test(dataProvider = "dbRefProvider")
    public void testDBRef(Object objectId, String expectedValue, String expectedType)
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

    @DataProvider
    public Object[][] dbRefProvider()
    {
        return new Object[][] {
                {"String type", "varchar 'String type'", "varchar"},
                {"BinData".getBytes(UTF_8), "to_utf8('BinData')", "varbinary"},
                {1234567890, "bigint '1234567890'", "bigint"},
                {true, "true", "boolean"},
                {12.3f, "double '12.3'", "double"},
                {new Date(0), "timestamp '1970-01-01 00:00:00.000'", "timestamp(3)"},
                {ImmutableList.of(1), "array[bigint '1']", "array(bigint)"},
                {new ObjectId("5126bc054aed4daf9e2ab772"), "ObjectId('5126bc054aed4daf9e2ab772')", "ObjectId"},
        };
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
        assertThatThrownBy(() -> query("SELECT * FROM test." + tableName))
                .hasMessageContaining("DBRef should have 3 fields : row(databaseName varchar, collectionName varchar)");

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

    @Override
    public void testAddColumnConcurrently()
    {
        // TODO: Enable after supporting multi-document transaction https://www.mongodb.com/docs/manual/core/transactions/
        throw new SkipException("TODO");
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

        assertQuery(
                "SELECT row_field.first.second FROM TABLE(mongodb.system.query(database => 'tpch', collection => '" + tableName + "', filter => '{ \"row_field.first.second\": 1 }'))",
                "VALUES 1");
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
                .matches("SELECT name FROM region");
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
        assertEquals(results.getRowCount(), 1);
        assertEquals(results.getMaterializedRows().get(0).getFieldCount(), 1);
        assertNotNull(results.getMaterializedRows().get(0).getField(0));
    }
}
