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

import com.google.common.net.HostAndPort;
import org.testcontainers.containers.MongoDBContainer;

import java.io.Closeable;

public class MongoServer
        implements Closeable
{
    private static final int MONGO_PORT = 27017;

    private final MongoDBContainer dockerContainer;

    public MongoServer()
    {
        this("3.4.0");
    }

    public MongoServer(String mongoVersion)
    {
        this.dockerContainer = new MongoDBContainer("mongo:" + mongoVersion)
                .withStartupAttempts(3)
                .withEnv("MONGO_INITDB_DATABASE", "tpch")
                .withCommand("--bind_ip 0.0.0.0");
        this.dockerContainer.start();
    }

    public HostAndPort getAddress()
    {
        return HostAndPort.fromParts(dockerContainer.getContainerIpAddress(), dockerContainer.getMappedPort(MONGO_PORT));
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
