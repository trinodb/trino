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
import com.mongodb.ConnectionString;
import org.bson.Document;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.Closeable;

import static java.util.Objects.requireNonNull;

public class AuthenticatedMongoServer
        implements Closeable
{
    private static final int MONGODB_INTERNAL_PORT = 27017;
    private static final String ROOT_USER = "root";
    private static final String ROOT_PASSWORD = "password";
    private static final String TEST_USER = "testUser";
    private static final String TEST_PASSWORD = "pass";
    private static final String TEST_ROLE = "testRole";
    public static final String TEST_DATABASE = "test";
    public static final String TEST_COLLECTION = "testCollection";
    private final GenericContainer<?> dockerContainer;

    public AuthenticatedMongoServer(String mongoVersion)
    {
        dockerContainer = new GenericContainer<>("mongo:" + requireNonNull(mongoVersion, "mongoVersion is null"))
                .withStartupAttempts(3)
                .waitingFor(Wait.forLogMessage(".*Listening on 0\\.0\\.0\\.0.*", 1))
                .withEnv("MONGO_INITDB_ROOT_USERNAME", ROOT_USER)
                .withEnv("MONGO_INITDB_ROOT_PASSWORD", ROOT_PASSWORD)
                .withEnv("MONGO_INITDB_DATABASE", "admin")
                .withExposedPorts(MONGODB_INTERNAL_PORT)
                .withCommand("--auth --bind_ip 0.0.0.0");
        dockerContainer.start();
    }

    public ConnectionString rootUserConnectionString()
    {
        return new ConnectionString("mongodb://%s:%s@%s:%d".formatted(
                ROOT_USER,
                ROOT_PASSWORD,
                dockerContainer.getHost(),
                dockerContainer.getMappedPort(MONGODB_INTERNAL_PORT)));
    }

    public ConnectionString testUserConnectionString()
    {
        return new ConnectionString("mongodb://%s:%s@%s:%d/%s".formatted(
                TEST_USER,
                TEST_PASSWORD,
                dockerContainer.getHost(),
                dockerContainer.getMappedPort(MONGODB_INTERNAL_PORT),
                TEST_DATABASE));
    }

    public static Document createTestRole()
    {
        return new Document(ImmutableMap.of(
                "createRole", TEST_ROLE,
                "privileges", ImmutableList.of(privilege("_schema"), privilege(TEST_COLLECTION)),
                "roles", ImmutableList.of()));
    }

    private static Document privilege(String collectionName)
    {
        return new Document(ImmutableMap.of(
                "resource", resource(collectionName),
                "actions", ImmutableList.of("find")));
    }

    private static Document resource(String collectionName)
    {
        return new Document(ImmutableMap.of(
                "db", TEST_DATABASE,
                "collection", collectionName));
    }

    public static Document createTestUser()
    {
        return new Document(ImmutableMap.of(
                "createUser", TEST_USER,
                "pwd", TEST_PASSWORD,
                "roles", ImmutableList.of(role())));
    }

    private static Document role()
    {
        return new Document(ImmutableMap.of(
                "role", TEST_ROLE,
                "db", TEST_DATABASE));
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
