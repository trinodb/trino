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
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.bson.Document;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.mongodb.AuthenticatedMongoServer.createRole;
import static io.trino.plugin.mongodb.AuthenticatedMongoServer.createUser;
import static io.trino.plugin.mongodb.AuthenticatedMongoServer.privilege;
import static io.trino.plugin.mongodb.AuthenticatedMongoServer.resource;
import static io.trino.plugin.mongodb.AuthenticatedMongoServer.role;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMongoPrivileges
        extends AbstractTestQueryFramework
{
    private static final List<String> DATABASES = ImmutableList.of("db", "MixedCaseDB", "UPPERCASEDB");
    private static final String TEST_COLLECTION = "testCollection";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        AuthenticatedMongoServer mongoServer = closeAfterClass(setupMongoServer());
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog(Optional.empty())
                        .setSchema(Optional.empty())
                        .build())
                .build();
        try {
            queryRunner.installPlugin(new MongoPlugin());
            DATABASES.forEach(database -> {
                String connectionUrl = mongoServer.testUserConnectionString(database, getUsername(database), getPassword(database)).getConnectionString();
                queryRunner.createCatalog(getCatalogName(database), "mongodb", ImmutableMap.of(
                        "mongodb.case-insensitive-name-matching", "true",
                        "mongodb.connection-url", connectionUrl));
            });
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Test
    public void testSchemasVisibility()
    {
        for (String database : DATABASES) {
            assertQuery("SHOW SCHEMAS FROM " + getCatalogName(database), "VALUES 'information_schema','%s'".formatted(database.toLowerCase(ENGLISH)));
        }
    }

    @Test
    public void testTablesVisibility()
    {
        for (String database : DATABASES) {
            assertQuery("SHOW TABLES FROM %s.%s".formatted(getCatalogName(database), database), "VALUES '%s'".formatted(TEST_COLLECTION.toLowerCase(ENGLISH)));
        }
    }

    @Test
    public void testSelectFromTable()
    {
        for (String database : DATABASES) {
            assertQuery("SELECT * from %s.%s.%s".formatted(getCatalogName(database), database, TEST_COLLECTION), "VALUES ('abc', 1)");
        }
    }

    private static AuthenticatedMongoServer setupMongoServer()
    {
        AuthenticatedMongoServer mongoServer = new AuthenticatedMongoServer("4.2.0");
        try (MongoClient client = MongoClients.create(mongoServer.rootUserConnectionString())) {
            DATABASES.forEach(database -> createDatabase(client, database));
            client.getDatabase("another").createCollection("_schema"); // this database/schema should not be visible
        }
        return mongoServer;
    }

    private static void createDatabase(MongoClient client, String database)
    {
        MongoDatabase testDatabase = client.getDatabase(database);
        runCommand(testDatabase, createTestRole(database));
        runCommand(testDatabase, createTestUser(database));
        testDatabase.createCollection("_schema");
        testDatabase.getCollection(TEST_COLLECTION).insertOne(new Document(ImmutableMap.of("Name", "abc", "Value", 1)));
        testDatabase.createCollection("anotherCollection"); // this collection/table should not be visible
    }

    private static void runCommand(MongoDatabase database, Document document)
    {
        Double commandStatus = database.runCommand(document)
                .get("ok", Double.class);
        assertThat(commandStatus).isEqualTo(1.0);
    }

    private static Document createTestRole(String database)
    {
        return createRole(
                getRoleName(database),
                ImmutableList.of(
                        privilege(
                                resource(database, "_schema"),
                                ImmutableList.of("find", "listIndexes", "createIndex", "insert")),
                        privilege(
                                resource(database, TEST_COLLECTION),
                                ImmutableList.of("find", "listIndexes"))),
                ImmutableList.of());
    }

    private static String getCatalogName(String database)
    {
        return "mongodb_" + database.toLowerCase(ENGLISH);
    }

    private static Document createTestUser(String database)
    {
        return createUser(
                getUsername(database),
                getPassword(database),
                ImmutableList.of(role(database, getRoleName(database))));
    }

    private static String getRoleName(String database)
    {
        return database + "testRole";
    }

    private static String getUsername(String database)
    {
        return database + "testUser";
    }

    private static String getPassword(String database)
    {
        return database + "pass";
    }
}
