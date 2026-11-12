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
import com.google.common.collect.Streams;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoSocketException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCursor;
import io.trino.spi.TrinoException;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.mongodb.MongoErrorCode.MONGODB_CLUSTER_ERROR;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestMongoNetworkErrorTranslator
{
    private static final String DATABASE = "test";
    private static final int DOCUMENT_COUNT = 10;
    private static final int BATCH_SIZE = 2;

    private MongoServer server;
    private MongoClient adminClient;

    @BeforeAll
    void setUp()
    {
        server = new MongoServer();
        adminClient = MongoClients.create(server.getConnectionString());
    }

    @AfterAll
    void tearDown()
    {
        adminClient.close();
        adminClient = null;
        server.close();
        server = null;
    }

    @Test
    void testNetworkErrorWhileIteratingCursor()
    {
        String applicationName = "test_cursor_" + randomNameSuffix();
        RemoteTableName table = createCollectionWithDocuments();
        try (MongoClient client = createClient(applicationName)) {
            MongoSession session = createSession(client);
            try (MongoCursor<Document> cursor = session.getCollection(table).find().batchSize(BATCH_SIZE).iterator()) {
                // Consume the first batch, so the next call fetches more documents from the server
                for (int i = 0; i < BATCH_SIZE; i++) {
                    cursor.next();
                }
                closeConnectionOnNextCommand(applicationName, "getMore");
                assertTrinoExceptionThrownBy(cursor::next)
                        .hasErrorCode(MONGODB_CLUSTER_ERROR)
                        .hasCauseInstanceOf(MongoSocketException.class);
            }
        }
    }

    @Test
    void testNetworkErrorWhileStreamingIterable()
    {
        String applicationName = "test_stream_" + randomNameSuffix();
        RemoteTableName table = createCollectionWithDocuments();
        try (MongoClient client = createClient(applicationName)) {
            MongoSession session = createSession(client);
            // Creating the stream executes the query and fetches the first batch
            Stream<Document> stream = Streams.stream(session.getCollection(table).find().batchSize(BATCH_SIZE));
            closeConnectionOnNextCommand(applicationName, "getMore");
            assertTrinoExceptionThrownBy(stream::toList)
                    .hasErrorCode(MONGODB_CLUSTER_ERROR)
                    .hasCauseInstanceOf(MongoSocketException.class);
        }
    }

    @Test
    void testCommandErrorIsNotTranslated()
    {
        String applicationName = "test_command_error_" + randomNameSuffix();
        RemoteTableName table = createCollectionWithDocuments();
        try (MongoClient client = createClient(applicationName)) {
            MongoSession session = createSession(client);
            failNextCommand(applicationName, "find");
            assertThatThrownBy(() -> session.getCollection(table).find().first())
                    .isInstanceOf(MongoCommandException.class)
                    .isNotInstanceOf(TrinoException.class);
        }
    }

    private RemoteTableName createCollectionWithDocuments()
    {
        RemoteTableName table = new RemoteTableName(DATABASE, "test_network_error_" + randomNameSuffix());
        adminClient.getDatabase(table.databaseName()).getCollection(table.collectionName())
                .insertMany(IntStream.range(0, DOCUMENT_COUNT)
                        .mapToObj(i -> new Document("value", i))
                        .collect(toImmutableList()));
        return table;
    }

    private MongoClient createClient(String applicationName)
    {
        return MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(server.getConnectionString())
                .applicationName(applicationName)
                .build());
    }

    private static MongoSession createSession(MongoClient client)
    {
        return new MongoSession(TESTING_TYPE_MANAGER, () -> client, new MongoClientConfig());
    }

    private void closeConnectionOnNextCommand(String applicationName, String command)
    {
        configureFailCommand(applicationName, command, "closeConnection", true);
    }

    private void failNextCommand(String applicationName, String command)
    {
        // 2 is BadValue, a plain server-side command error
        configureFailCommand(applicationName, command, "errorCode", 2);
    }

    // Scoped to the client application name, so concurrent tests do not affect each other
    private void configureFailCommand(String applicationName, String command, String failureKey, Object failureValue)
    {
        adminClient.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand")
                .append("mode", new Document("times", 1))
                .append("data", new Document("failCommands", ImmutableList.of(command))
                        .append("appName", applicationName)
                        .append(failureKey, failureValue)));
    }
}
