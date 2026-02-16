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
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Map;

import static io.trino.plugin.mongodb.MongoQueryRunner.createMongoClient;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestMongoCaseInsensitiveMapping
        extends AbstractTestQueryFramework
{
    private final MongoServer server;
    private final MongoClient client;

    public TestMongoCaseInsensitiveMapping()
    {
        server = new MongoServer();
        client = createMongoClient(server);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return MongoQueryRunner.builder(server)
                .addConnectorProperties(Map.of("mongodb.case-insensitive-name-matching", "true"))
                .build();
    }

    @AfterAll
    public final void destroy()
    {
        server.close();
        client.close();
    }

    @Test
    public void testCaseInsensitive()
    {
        MongoCollection<Document> collection = client.getDatabase("testCase").getCollection("testInsensitive");
        collection.insertOne(new Document(ImmutableMap.of("Name", "abc", "Value", 1)));

        assertQuery("SHOW SCHEMAS IN mongodb LIKE 'testCase'", "SELECT 'testCase'");
        assertQuery("SHOW TABLES IN testCase", "SELECT 'testInsensitive'");
        assertQuery(
                "SHOW COLUMNS FROM testCase.testInsensitive",
                "VALUES ('Name', 'varchar', '', ''), ('Value', 'bigint', '', '')");

        assertQuery("SELECT name, value FROM testCase.testInsensitive", "SELECT 'abc', 1");
        assertUpdate("INSERT INTO testCase.testInsensitive VALUES('def', 2)", 1);

        assertQuery("SELECT value FROM testCase.testInsensitive WHERE name = 'def'", "SELECT 2");
        assertUpdate("DROP TABLE testCase.testInsensitive");
        assertQueryReturnsEmptyResult("SHOW TABLES IN testCase");

        assertUpdate("DROP SCHEMA testCase");
        assertQueryReturnsEmptyResult("SHOW SCHEMAS IN mongodb LIKE 'testCase'");
    }

    @Test
    public void testCaseInsensitiveRenameTable()
    {
        MongoCollection<Document> collection = client.getDatabase("testCase_RenameTable").getCollection("testInsensitive_RenameTable");
        collection.insertOne(new Document(ImmutableMap.of("value", 1)));
        assertQuery("SHOW TABLES IN testCase_RenameTable", "SELECT 'testInsensitive_RenameTable'");
        assertQuery("SELECT value FROM testCase_RenameTable.testInsensitive_RenameTable", "SELECT 1");

        assertUpdate("ALTER TABLE testCase_RenameTable.testInsensitive_RenameTable RENAME TO testCase_RenameTable.testinsensitive_renamed_table");

        assertQuery("SHOW TABLES IN testCase_RenameTable", "SELECT 'testinsensitive_renamed_table'");
        assertQuery("SELECT value FROM testCase_RenameTable.testinsensitive_renamed_table", "SELECT 1");
        assertUpdate("DROP TABLE testCase_RenameTable.testinsensitive_renamed_table");
    }

    @Test
    public void testNonLowercaseViewName()
    {
        // Case sensitive schema name
        MongoCollection<Document> collection = client.getDatabase("NonLowercaseSchema").getCollection("test_collection");
        collection.insertOne(new Document(ImmutableMap.of("Name", "abc", "Value", 1)));

        client.getDatabase("NonLowercaseSchema").createView("lowercase_view", "test_collection", ImmutableList.of());
        assertQuery("SELECT value FROM NonLowercaseSchema.lowercase_view WHERE name = 'abc'", "SELECT 1");

        // Case sensitive view name
        collection = client.getDatabase("test_database").getCollection("test_collection");
        collection.insertOne(new Document(ImmutableMap.of("Name", "abc", "Value", 1)));

        client.getDatabase("test_database").createView("NonLowercaseView", "test_collection", ImmutableList.of());
        assertQuery("SELECT value FROM test_database.NonLowercaseView WHERE name = 'abc'", "SELECT 1");

        // Case sensitive schema and view name
        client.getDatabase("NonLowercaseSchema").createView("NonLowercaseView", "test_collection", ImmutableList.of());
        assertQuery("SELECT value FROM NonLowercaseSchema.NonLowercaseView WHERE name = 'abc'", "SELECT 1");

        assertUpdate("DROP TABLE NonLowercaseSchema.lowercase_view");
        assertUpdate("DROP TABLE test_database.NonLowercaseView");
        assertUpdate("DROP TABLE NonLowercaseSchema.test_collection");
        assertUpdate("DROP TABLE test_database.test_collection");
        assertUpdate("DROP TABLE NonLowercaseSchema.NonLowercaseView");
    }

    @Test
    public void testNativeQueryWithCaseInSensitiveNameMatch()
    {
        String tableName = "Test_Case_Insensitive" + randomNameSuffix();
        String schemaName = "Test_Case_Insensitive_Schema" + randomNameSuffix();
        client.getDatabase(schemaName).getCollection(tableName).insertOne(new Document("field", "hello"));

        assertThat(query("SELECT * FROM TABLE(mongodb.system.query(database => '" + schemaName.toLowerCase(ENGLISH) + "', collection => '" + tableName.toLowerCase(ENGLISH) + "', filter => '{}'))"))
                .matches("VALUES CAST('hello' AS VARCHAR)");
    }
}
