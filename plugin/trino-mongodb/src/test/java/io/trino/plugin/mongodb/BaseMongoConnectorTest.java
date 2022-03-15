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
import io.trino.sql.planner.plan.LimitNode;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Date;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

public abstract class BaseMongoConnectorTest
        extends BaseConnectorTest
{
    protected MongoServer server;
    protected MongoClient client;

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        server.close();
        client.close();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_RENAME_SCHEMA:
            case SUPPORTS_NOT_NULL_CONSTRAINT:
            case SUPPORTS_RENAME_TABLE:
            case SUPPORTS_RENAME_COLUMN:
            case SUPPORTS_COMMENT_ON_COLUMN:
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
                "TopNPartial\\[5 by \\(nationkey DESC");
    }

    @Test(dataProvider = "guessFieldTypesProvider")
    public void testGuessFieldTypes(String mongoValue, String trinoValue)
    {
        Document document = Document.parse(format("{\"test\":%s}", mongoValue));

        assertUpdate("DROP TABLE IF EXISTS test.test_guess_field_type");
        client.getDatabase("test").getCollection("test_guess_field_type").insertOne(document);

        assertThat(query("SELECT test FROM test.test_guess_field_type"))
                .matches("SELECT " + trinoValue);

        assertUpdate("DROP TABLE test.test_guess_field_type");
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
        String query = "" +
                "CREATE TABLE test_types_table AS " +
                "SELECT" +
                " 'foo' _varchar" +
                ", cast('bar' as varbinary) _varbinary" +
                ", cast(1 as bigint) _bigint" +
                ", 3.14E0 _double" +
                ", true _boolean" +
                ", DATE '1980-05-07' _date" +
                ", TIMESTAMP '1980-05-07 11:22:33.456' _timestamp" +
                ", ObjectId('ffffffffffffffffffffffff') _objectid" +
                ", JSON '{\"name\":\"alice\"}' _json";

        assertUpdate(query, 1);

        MaterializedResult results = getQueryRunner().execute(getSession(), "SELECT * FROM test_types_table").toTestTypes();
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
        assertUpdate("DROP TABLE test_types_table");

        assertFalse(getQueryRunner().tableExists(getSession(), "test_types_table"));
    }

    @Test
    public void testInsertWithEveryType()
    {
        String createSql = "" +
                "CREATE TABLE test_insert_types_table " +
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

        String insertSql = "" +
                "INSERT INTO test_insert_types_table " +
                "SELECT" +
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

        MaterializedResult results = getQueryRunner().execute(getSession(), "SELECT * FROM test_insert_types_table").toTestTypes();
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
        assertUpdate("DROP TABLE test_insert_types_table");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_insert_types_table"));
    }

    @Test
    public void testJson()
    {
        assertUpdate("CREATE TABLE test_json (id INT, col JSON)");

        assertUpdate("INSERT INTO test_json VALUES (1, JSON '{\"name\":\"alice\"}')", 1);
        assertQuery("SELECT json_extract_scalar(col, '$.name') FROM test_json WHERE id = 1", "SELECT 'alice'");

        assertUpdate("INSERT INTO test_json VALUES (2, JSON '{\"numbers\":[1, 2, 3]}')", 1);
        assertQuery("SELECT json_extract(col, '$.numbers[0]') FROM test_json WHERE id = 2", "SELECT 1");

        assertUpdate("INSERT INTO test_json VALUES (3, NULL)", 1);
        assertQuery("SELECT col FROM test_json WHERE id = 3", "SELECT NULL");

        assertQueryFails(
                "CREATE TABLE test_json_scalar AS SELECT JSON '1' AS col",
                "Can't convert json to MongoDB Document.*");

        assertQueryFails(
                "CREATE TABLE test_json_array AS SELECT JSON '[\"a\", \"b\", \"c\"]' AS col",
                "Can't convert json to MongoDB Document.*");

        assertUpdate("DROP TABLE test_json");
    }

    @Test
    public void testArrays()
    {
        assertUpdate("CREATE TABLE tmp_array1 AS SELECT ARRAY[1, 2, NULL] AS col", 1);
        assertQuery("SELECT col[2] FROM tmp_array1", "SELECT 2");
        assertQuery("SELECT col[3] FROM tmp_array1", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_array2 AS SELECT ARRAY[1.0E0, 2.5E0, 3.5E0] AS col", 1);
        assertQuery("SELECT col[2] FROM tmp_array2", "SELECT 2.5");

        assertUpdate("CREATE TABLE tmp_array3 AS SELECT ARRAY['puppies', 'kittens', NULL] AS col", 1);
        assertQuery("SELECT col[2] FROM tmp_array3", "SELECT 'kittens'");
        assertQuery("SELECT col[3] FROM tmp_array3", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_array4 AS SELECT ARRAY[TRUE, NULL] AS col", 1);
        assertQuery("SELECT col[1] FROM tmp_array4", "SELECT TRUE");
        assertQuery("SELECT col[2] FROM tmp_array4", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_array5 AS SELECT ARRAY[ARRAY[1, 2], NULL, ARRAY[3, 4]] AS col", 1);
        assertQuery("SELECT col[1][2] FROM tmp_array5", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_array6 AS SELECT ARRAY[ARRAY['\"hi\"'], NULL, ARRAY['puppies']] AS col", 1);
        assertQuery("SELECT col[1][1] FROM tmp_array6", "SELECT '\"hi\"'");
        assertQuery("SELECT col[3][1] FROM tmp_array6", "SELECT 'puppies'");
    }

    @Test
    public void testTemporalArrays()
    {
        assertUpdate("CREATE TABLE tmp_array7 AS SELECT ARRAY[DATE '2014-09-30'] AS col", 1);
        assertOneNotNullResult("SELECT col[1] FROM tmp_array7");
        assertUpdate("CREATE TABLE tmp_array8 AS SELECT ARRAY[TIMESTAMP '2001-08-22 03:04:05.321'] AS col", 1);
        assertOneNotNullResult("SELECT col[1] FROM tmp_array8");
    }

    @Test
    public void testSkipUnknownTypes()
    {
        Document document1 = new Document("col", Document.parse("{\"key1\": \"value1\", \"key2\": null}"));
        client.getDatabase("test").getCollection("tmp_guess_schema1").insertOne(document1);
        assertQuery("SHOW COLUMNS FROM test.tmp_guess_schema1", "SELECT 'col', 'row(key1 varchar)', '', ''");
        assertQuery("SELECT col.key1 FROM test.tmp_guess_schema1", "SELECT 'value1'");

        Document document2 = new Document("col", new Document("key1", null));
        client.getDatabase("test").getCollection("tmp_guess_schema2").insertOne(document2);
        assertQueryReturnsEmptyResult("SHOW COLUMNS FROM test.tmp_guess_schema2");
    }

    @Test(dataProvider = "dbRefProvider")
    public void testDBRef(Object objectId, String expectedValue, String expectedType)
    {
        Document document = Document.parse("{\"_id\":ObjectId(\"5126bbf64aed4daf9e2ab771\"),\"col1\":\"foo\"}");

        DBRef dbRef = new DBRef("test", "creators", objectId);
        document.append("creator", dbRef);

        assertUpdate("DROP TABLE IF EXISTS test.test_dbref");
        client.getDatabase("test").getCollection("test_dbref").insertOne(document);

        assertThat(query("SELECT creator.databaseName, creator.collectionName, creator.id FROM test.test_dbref"))
                .matches("SELECT varchar 'test', varchar 'creators', " + expectedValue);
        assertQuery(
                "SELECT typeof(creator) FROM test.test_dbref",
                "SELECT 'row(databaseName varchar, collectionName varchar, id " + expectedType + ")'");

        assertUpdate("DROP TABLE test.test_dbref");
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
    public void testMaps()
    {
        assertUpdate("CREATE TABLE tmp_map1 AS SELECT MAP(ARRAY[0,1], ARRAY[2,NULL]) AS col", 1);
        assertQuery("SELECT col[0] FROM tmp_map1", "SELECT 2");
        assertQuery("SELECT col[1] FROM tmp_map1", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_map2 AS SELECT MAP(ARRAY[1.0E0], ARRAY[2.5E0]) AS col", 1);
        assertQuery("SELECT col[1.0] FROM tmp_map2", "SELECT 2.5");

        assertUpdate("CREATE TABLE tmp_map3 AS SELECT MAP(ARRAY['puppies'], ARRAY['kittens']) AS col", 1);
        assertQuery("SELECT col['puppies'] FROM tmp_map3", "SELECT 'kittens'");

        assertUpdate("CREATE TABLE tmp_map4 AS SELECT MAP(ARRAY[TRUE], ARRAY[FALSE]) AS col", "SELECT 1");
        assertQuery("SELECT col[TRUE] FROM tmp_map4", "SELECT FALSE");

        assertUpdate("CREATE TABLE tmp_map5 AS SELECT MAP(ARRAY[1.0E0], ARRAY[ARRAY[1, 2]]) AS col", 1);
        assertQuery("SELECT col[1.0][2] FROM tmp_map5", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_map6 AS SELECT MAP(ARRAY[DATE '2014-09-30'], ARRAY[DATE '2014-09-29']) AS col", 1);
        assertOneNotNullResult("SELECT col[DATE '2014-09-30'] FROM tmp_map6");
        assertUpdate("CREATE TABLE tmp_map7 AS SELECT MAP(ARRAY[TIMESTAMP '2001-08-22 03:04:05.321'], ARRAY[TIMESTAMP '2001-08-22 03:04:05.321']) AS col", 1);
        assertOneNotNullResult("SELECT col[TIMESTAMP '2001-08-22 03:04:05.321'] FROM tmp_map7");

        assertUpdate("CREATE TABLE test.tmp_map8 (col MAP<VARCHAR, VARCHAR>)");
        client.getDatabase("test").getCollection("tmp_map8").insertOne(new Document(
                ImmutableMap.of("col", new Document(ImmutableMap.of("key1", "value1", "key2", "value2")))));
        assertQuery("SELECT col['key1'] FROM test.tmp_map8", "SELECT 'value1'");

        assertUpdate("CREATE TABLE test.tmp_map9 (col VARCHAR)");
        client.getDatabase("test").getCollection("tmp_map9").insertOne(new Document(
                ImmutableMap.of("col", new Document(ImmutableMap.of("key1", "value1", "key2", "value2")))));
        assertQuery("SELECT col FROM test.tmp_map9", "SELECT '{\"key1\": \"value1\", \"key2\": \"value2\"}'");

        assertUpdate("CREATE TABLE test.tmp_map10 (col VARCHAR)");
        client.getDatabase("test").getCollection("tmp_map10").insertOne(new Document(
                ImmutableMap.of("col", ImmutableList.of(new Document(ImmutableMap.of("key1", "value1", "key2", "value2")),
                        new Document(ImmutableMap.of("key3", "value3", "key4", "value4"))))));
        assertQuery("SELECT col FROM test.tmp_map10", "SELECT '[{\"key1\": \"value1\", \"key2\": \"value2\"}, {\"key3\": \"value3\", \"key4\": \"value4\"}]'");

        assertUpdate("CREATE TABLE test.tmp_map11 (col VARCHAR)");
        client.getDatabase("test").getCollection("tmp_map11").insertOne(new Document(
                ImmutableMap.of("col", 10)));
        assertQuery("SELECT col FROM test.tmp_map11", "SELECT '10'");

        assertUpdate("CREATE TABLE test.tmp_map12 (col VARCHAR)");
        client.getDatabase("test").getCollection("tmp_map12").insertOne(new Document(
                ImmutableMap.of("col", Arrays.asList(10, null, 11))));
        assertQuery("SELECT col FROM test.tmp_map12", "SELECT '[10, null, 11]'");
    }

    @Test
    public void testCollectionNameContainsDots()
    {
        assertUpdate("CREATE TABLE \"tmp.dot1\" AS SELECT 'foo' _varchar", 1);
        assertQuery("SELECT _varchar FROM \"tmp.dot1\"", "SELECT 'foo'");
        assertUpdate("DROP TABLE \"tmp.dot1\"");
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

        assertUpdate("DROP TABLE IF EXISTS tmp_objectid");
        assertUpdate("CREATE TABLE tmp_objectid AS SELECT * FROM " + inlineTable, 6);

        // IS NULL
        assertQuery("SELECT i FROM " + inlineTable + " WHERE one IS NULL", "VALUES 10, 15");
        assertQuery("SELECT i FROM tmp_objectid WHERE one IS NULL", "VALUES 10, 15");

        // CAST AS varchar
        assertQuery(
                "SELECT i, CAST(one AS varchar) FROM " + inlineTable + " WHERE i <= 13",
                "VALUES (10, NULL), (11, 'ffffffffffffffffffffffff'), (12, 'ffffffffffffffffffffffff'), (13, '000000000000000000000000')");

        // EQUAL
        assertQuery("SELECT i FROM tmp_objectid WHERE one = two", "VALUES 11, 13");
        assertQuery("SELECT i FROM tmp_objectid WHERE one = ObjectId('ffffffffffffffffffffffff')", "VALUES 11, 12, 14");

        // IS DISTINCT FROM
        assertQuery("SELECT i FROM " + inlineTable + " WHERE one IS DISTINCT FROM two", "VALUES 12, 14, 15");
        assertQuery("SELECT i FROM " + inlineTable + " WHERE one IS NOT DISTINCT FROM two", "VALUES 10, 11, 13");

        assertQuery("SELECT i FROM tmp_objectid WHERE one IS DISTINCT FROM two", "VALUES 12, 14, 15");
        assertQuery("SELECT i FROM tmp_objectid WHERE one IS NOT DISTINCT FROM two", "VALUES 10, 11, 13");

        // Join on ObjectId
        assertQuery(
                format("SELECT l.i, r.i FROM (%1$s) AS l(i, one, two) JOIN (%1$s) AS r(i, one, two) ON l.one = r.two", values),
                "VALUES (11, 11), (14, 11), (11, 15), (12, 15), (12, 11), (14, 15), (13, 13)");

        // Group by ObjectId (IS DISTINCT FROM)
        assertQuery("SELECT array_agg(i ORDER BY i) FROM " + inlineTable + " GROUP BY one", "VALUES ((10, 15)), ((11, 12, 14)), ((13))");
        assertQuery("SELECT i FROM " + inlineTable + " GROUP BY one, i", "VALUES 10, 11, 12, 13, 14, 15");

        // Group by Row(ObjectId) (ID DISTINCT FROM in @OperatorDependency)
        assertQuery(
                "SELECT r.i, count(*) FROM (SELECT CAST(row(one, i) AS row(one ObjectId, i bigint)) r FROM " + inlineTable + ") GROUP BY r",
                "VALUES (10, 1), (11, 1), (12, 1), (13, 1), (14, 1), (15, 1)");
        assertQuery(
                "SELECT r.x, CAST(r.one AS varchar), count(*) FROM (SELECT CAST(row(one, i / 3 * 3) AS row(one ObjectId, x bigint)) r FROM " + inlineTable + ") GROUP BY r",
                "VALUES (9, NULL, 1), (9, 'ffffffffffffffffffffffff', 1), (12, 'ffffffffffffffffffffffff', 2), (12, '000000000000000000000000', 1), (15, NULL, 1)");

        assertUpdate("DROP TABLE tmp_objectid");
    }

    @Test
    public void testCaseInsensitive()
            throws Exception
    {
        MongoCollection<Document> collection = client.getDatabase("testCase").getCollection("testInsensitive");
        collection.insertOne(new Document(ImmutableMap.of("Name", "abc", "Value", 1)));

        assertQuery("SHOW SCHEMAS IN mongodb LIKE 'testcase'", "SELECT 'testcase'");
        assertQuery("SHOW TABLES IN testcase", "SELECT 'testinsensitive'");
        assertQuery(
                "SHOW COLUMNS FROM testcase.testInsensitive",
                "VALUES ('name', 'varchar', '', ''), ('value', 'bigint', '', '')");

        assertQuery("SELECT name, value FROM testcase.testinsensitive", "SELECT 'abc', 1");
        assertUpdate("INSERT INTO testcase.testinsensitive VALUES('def', 2)", 1);

        assertQuery("SELECT value FROM testcase.testinsensitive WHERE name = 'def'", "SELECT 2");
        assertUpdate("DROP TABLE testcase.testinsensitive");
    }

    @Test
    public void testNonLowercaseViewName()
    {
        // Case insensitive schema name
        MongoCollection<Document> collection = client.getDatabase("NonLowercaseSchema").getCollection("test_collection");
        collection.insertOne(new Document(ImmutableMap.of("Name", "abc", "Value", 1)));

        client.getDatabase("NonLowercaseSchema").createView("lowercase_view", "test_collection", ImmutableList.of());
        assertQuery("SELECT value FROM nonlowercaseschema.lowercase_view WHERE name = 'abc'", "SELECT 1");

        // Case insensitive view name
        collection = client.getDatabase("test_database").getCollection("test_collection");
        collection.insertOne(new Document(ImmutableMap.of("Name", "abc", "Value", 1)));

        client.getDatabase("test_database").createView("NonLowercaseView", "test_collection", ImmutableList.of());
        assertQuery("SELECT value FROM test_database.nonlowercaseview WHERE name = 'abc'", "SELECT 1");

        // Case insensitive schema and view name
        client.getDatabase("NonLowercaseSchema").createView("NonLowercaseView", "test_collection", ImmutableList.of());
        assertQuery("SELECT value FROM nonlowercaseschema.nonlowercaseview WHERE name = 'abc'", "SELECT 1");

        assertUpdate("DROP TABLE nonlowercaseschema.lowercase_view");
        assertUpdate("DROP TABLE test_database.nonlowercaseview");
        assertUpdate("DROP TABLE nonlowercaseschema.test_collection");
        assertUpdate("DROP TABLE test_database.test_collection");
        assertUpdate("DROP TABLE nonlowercaseschema.nonlowercaseview");
    }

    @Test
    public void testSelectView()
    {
        assertUpdate("CREATE TABLE test.view_base AS SELECT 'foo' _varchar", 1);
        client.getDatabase("test").createView("test_view", "view_base", ImmutableList.of());
        assertQuery("SELECT * FROM test.view_base", "SELECT 'foo'");
        assertUpdate("DROP TABLE test.test_view");
        assertUpdate("DROP TABLE test.view_base");
    }

    @Test
    public void testDropTable()
    {
        assertUpdate("CREATE TABLE test.drop_table(col bigint)");
        assertUpdate("DROP TABLE test.drop_table");
        assertQueryFails("SELECT * FROM test.drop_table", ".*Table 'mongodb.test.drop_table' does not exist");
    }

    @Test
    public void testNullPredicates()
    {
        assertUpdate("CREATE TABLE test.null_predicates(name varchar, value integer)");

        MongoCollection<Document> collection = client.getDatabase("test").getCollection("null_predicates");
        collection.insertOne(new Document(ImmutableMap.of("name", "abc", "value", 1)));
        collection.insertOne(new Document(ImmutableMap.of("name", "abcd")));
        collection.insertOne(new Document(Document.parse("{\"name\": \"abcde\", \"value\": null}")));

        assertQuery("SELECT count(*) FROM test.null_predicates WHERE value IS NULL OR rand() = 42", "SELECT 2");
        assertQuery("SELECT count(*) FROM test.null_predicates WHERE value IS NULL", "SELECT 2");
        assertQuery("SELECT count(*) FROM test.null_predicates WHERE value IS NOT NULL", "SELECT 1");

        assertUpdate("DROP TABLE test.null_predicates");
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

    private void assertOneNotNullResult(String query)
    {
        MaterializedResult results = getQueryRunner().execute(getSession(), query).toTestTypes();
        assertEquals(results.getRowCount(), 1);
        assertEquals(results.getMaterializedRows().get(0).getFieldCount(), 1);
        assertNotNull(results.getMaterializedRows().get(0).getField(0));
    }
}
