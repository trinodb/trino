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

import com.mongodb.ConnectionString;
import org.testcontainers.containers.MongoDBContainer;

import java.io.Closeable;

public class MongoServer
        implements Closeable
{
    private final MongoDBContainer dockerContainer;

    public MongoServer()
    {
        this("6.0.14");
    }

    public MongoServer(String mongoVersion)
    {
        this.dockerContainer = new MongoDBContainer("mongo:" + mongoVersion)
                .withStartupAttempts(3)
                .withEnv("MONGO_INITDB_DATABASE", "tpch")
                .withCommand("--bind_ip 0.0.0.0 --replSet rs0");
        this.dockerContainer.start();

        // MongoDB 8.x requires manual replica set initialization
        if (mongoVersion.startsWith("8.")) {
            initializeReplicaSet();
        }
    }

    public ConnectionString getConnectionString()
    {
        return new ConnectionString(dockerContainer.getReplicaSetUrl());
    }

    private void initializeReplicaSet()
    {
        try {
            // Wait for MongoDB to be ready
            waitForMongoToBeReady();

            // Initialize replica set
            try {
                this.dockerContainer.execInContainer("mongosh", "--eval", "rs.initiate()");
            }
            catch (Exception e) {
                this.dockerContainer.execInContainer("mongo", "--eval", "rs.initiate()");
            }

            // Wait a bit more for replica set to be ready
            Thread.sleep(2000);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to initialize MongoDB replica set", e);
        }
    }

    private void waitForMongoToBeReady()
    {
        int maxAttempts = 30;
        int attempt = 0;

        while (attempt < maxAttempts) {
            try {
                this.dockerContainer.execInContainer("mongosh", "--eval", "db.runCommand('ping')");
                return; // MongoDB is ready
            }
            catch (Exception e) {
                attempt++;
                if (attempt >= maxAttempts) {
                    throw new RuntimeException("MongoDB did not become ready after " + maxAttempts + " attempts", e);
                }
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for MongoDB", ie);
                }
            }
        }
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
