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
import java.util.List;

import static java.util.Objects.requireNonNull;

public class AuthenticatedMongoServer
        implements Closeable
{
    private static final int MONGODB_INTERNAL_PORT = 27017;
    private static final String ROOT_USER = "root";
    private static final String ROOT_PASSWORD = "password";
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

    public ConnectionString testUserConnectionString(String database, String user, String password)
    {
        return new ConnectionString("mongodb://%s:%s@%s:%d/%s".formatted(
                user,
                password,
                dockerContainer.getHost(),
                dockerContainer.getMappedPort(MONGODB_INTERNAL_PORT),
                database));
    }

    public static Document createRole(String role, ImmutableList<Document> privileges, ImmutableList<Object> roles)
    {
        return new Document(ImmutableMap.of(
                "createRole", role,
                "privileges", privileges,
                "roles", roles));
    }

    public static Document privilege(Document resource, List<String> actions)
    {
        return new Document(ImmutableMap.of(
                "resource", resource,
                "actions", actions));
    }

    public static Document resource(String database, String collectionName)
    {
        return new Document(ImmutableMap.of(
                "db", database,
                "collection", collectionName));
    }

    public static Document createUser(String user, String password, ImmutableList<Document> roles)
    {
        return new Document(ImmutableMap.of(
                "createUser", user,
                "pwd", password,
                "roles", roles));
    }

    public static Document role(String database, String role)
    {
        return new Document(ImmutableMap.of(
                "role", role,
                "db", database));
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
