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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.airlift.log.Logger;
import io.trino.tpch.TpchTable;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.mongodb.TestingMongoAtlasInfoProvider.getConnectionString;

public class TestMongoAtlasInstanceCleaner
{
    private static final Logger LOG = Logger.get(TestMongoAtlasInstanceCleaner.class);
    private static final String TABLE_NAME_KEY = "table";
    private static final Set<String> SYSTEM_DATABASES = ImmutableSet.of("admin", "local", "config");
    private static final String SCHEMA_COLLECTION = "_schema";
    // These database(s) and/or collection(s) must be dropped in case the collection(s) definition or data changes. Required database(s)
    // and/or collection(s) will be created as part of corresponding test setup.
    private final Multimap<String, String> retainedCollections = new ImmutableSetMultimap.Builder<String, String>()
            .putAll("tpch", TpchTable.getTables().stream().map(TpchTable::getTableName).toList())
            .putAll("test", Arrays.stream(TestMongoFederatedDatabaseConnectorSmokeTest.predicatePushdownProvider()).map(element -> (String) element[0]).collect(Collectors.toList()))
            .put("galaxy_integration_tests_db", "galaxy_integration_tests_collection") // Used by Galaxy integration test
            .build();
    private final MongoClient client;

    public TestMongoAtlasInstanceCleaner()
    {
        String connectionString = getConnectionString().getConnectionString();
        checkArgument(connectionString.matches("^mongodb(\\+srv)?://.*"));
        this.client = MongoClients.create(connectionString);
    }

    /**
     * Cleans up the collections created before a day with the limitation that previously created databases will not be cleaned up
     * as there is no way to identify when the database was created.
     * Empty databases should not be deleted as it could be created by recently running tests.
     */
    @Test
    public void cleanUpDatasets()
    {
        client.listDatabaseNames().forEach(databaseName -> {
            if (SYSTEM_DATABASES.contains(databaseName)) {
                return;
            }
            MongoDatabase db = client.getDatabase(databaseName);
            for (Document document : db.getCollection(SCHEMA_COLLECTION).find()) {
                String collectionName = (String) document.get(TABLE_NAME_KEY);
                if (retainedCollections.containsKey(databaseName) && retainedCollections.get(databaseName).contains(collectionName)) {
                    // Skip reserved collections
                    continue;
                }
                long createdTime = ((ObjectId) document.get("_id")).getTimestamp();
                long daysDiff = TimeUnit.DAYS.convert(Instant.now().getEpochSecond() - createdTime, TimeUnit.SECONDS);
                if (daysDiff > 0) {
                    // Delete collection
                    LOG.info("Deleting %s collection from %s database", collectionName, databaseName);
                    db.getCollection(collectionName).drop();
                    // Delete metadata
                    db.getCollection(SCHEMA_COLLECTION).deleteOne(new Document(TABLE_NAME_KEY, collectionName));
                }
            }
        });
    }
}
