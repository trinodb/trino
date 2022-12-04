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

import com.google.common.collect.ImmutableMap;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.bson.Document;
import org.testng.annotations.Test;

import java.util.Locale;
import java.util.Optional;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.mongodb.AuthenticatedMongoServer.TEST_COLLECTION;
import static io.trino.plugin.mongodb.AuthenticatedMongoServer.TEST_DATABASE;
import static io.trino.plugin.mongodb.AuthenticatedMongoServer.createTestRole;
import static io.trino.plugin.mongodb.AuthenticatedMongoServer.createTestUser;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMongoPrivileges
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        AuthenticatedMongoServer mongoServer = closeAfterClass(setupMongoServer());
        return createMongoQueryRunner(mongoServer.testUserConnectionString().getConnectionString());
    }

    @Test
    public void testSchemasVisibility()
    {
        assertQuery("SHOW SCHEMAS FROM mongodb", "VALUES 'information_schema','%s'".formatted(TEST_DATABASE));
    }

    @Test
    public void testTablesVisibility()
    {
        assertQuery("SHOW TABLES FROM mongodb." + TEST_DATABASE, "VALUES '%s'".formatted(TEST_COLLECTION.toLowerCase(Locale.ENGLISH)));
    }

    private static AuthenticatedMongoServer setupMongoServer()
    {
        AuthenticatedMongoServer mongoServer = new AuthenticatedMongoServer("4.2.0");
        try (MongoClient client = MongoClients.create(mongoServer.rootUserConnectionString())) {
            MongoDatabase testDatabase = client.getDatabase(TEST_DATABASE);
            runCommand(testDatabase, createTestRole());
            runCommand(testDatabase, createTestUser());
            testDatabase.createCollection("_schema");
            testDatabase.createCollection(TEST_COLLECTION);
            testDatabase.createCollection("anotherCollection"); // this collection/table should not be visible
            client.getDatabase("another").createCollection("_schema"); // this database/schema should not be visible
        }
        return mongoServer;
    }

    private static void runCommand(MongoDatabase database, Document document)
    {
        Double commandStatus = database.runCommand(document)
                .get("ok", Double.class);
        assertThat(commandStatus).isEqualTo(1.0);
    }

    private static DistributedQueryRunner createMongoQueryRunner(String connectionUrl)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(testSessionBuilder()
                            .setCatalog(Optional.empty())
                            .setSchema(Optional.empty())
                            .build())
                    .build();
            queryRunner.installPlugin(new MongoPlugin());
            queryRunner.createCatalog("mongodb", "mongodb", ImmutableMap.of(
                    "mongodb.case-insensitive-name-matching", "true",
                    "mongodb.connection-url", connectionUrl));
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }
}
