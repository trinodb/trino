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
package io.prestosql.plugin.redis.util;

import com.google.common.net.HostAndPort;
import org.testcontainers.containers.GenericContainer;
import redis.clients.jedis.JedisPool;

import java.io.Closeable;

public class RedisServer
        implements Closeable
{
    private static final int PORT = 6379;

    private final GenericContainer<?> container;
    private final JedisPool jedisPool;

    public RedisServer()
    {
        container = new GenericContainer<>("redis:2.8.9")
                .withExposedPorts(PORT);
        container.start();

        jedisPool = new JedisPool(container.getContainerIpAddress(), container.getMappedPort(PORT));
    }

    public JedisPool getJedisPool()
    {
        return jedisPool;
    }

    public void destroyJedisPool()
    {
        jedisPool.destroy();
    }

    public HostAndPort getHostAndPort()
    {
        return HostAndPort.fromParts(container.getContainerIpAddress(), container.getMappedPort(PORT));
    }

    @Override
    public void close()
    {
        jedisPool.destroy();
        container.close();
    }
}
