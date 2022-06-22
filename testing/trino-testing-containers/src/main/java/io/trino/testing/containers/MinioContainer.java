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
package io.trino.testing.containers;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.trino.testing.containers.wait.strategy.SelectedPortWaitStrategy;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import java.io.Closeable;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import static com.google.common.net.HostAndPort.fromParts;
import static java.lang.String.format;

public final class MinioContainer
        implements Closeable
{
    public static final String MINIO_ACCESS_KEY = "minio-access-key";
    public static final String MINIO_SECRET_KEY = "minio-secret-key";
    private static final int MINIO_PORT = 9080;

    private final GenericContainer<?> minio;

    private MinioContainer(Map<String, String> env, String hostname, String[] networkAliases, Optional<Network> network)
    {
        minio = new GenericContainer("minio/minio:RELEASE.2021-07-15T22-27-34Z")
                .withEnv(env)
                .withNetworkAliases(networkAliases)
                .withCommand("server", "--address", format("0.0.0.0:%d", MINIO_PORT), "/data")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(new SelectedPortWaitStrategy(MINIO_PORT))
                .withStartupTimeout(Duration.ofMinutes(1));

        minio.withCreateContainerCmdModifier(c -> c.withHostName(hostname));
        network.ifPresent(minio::withNetwork);
        minio.addExposedPort(MINIO_PORT);
    }

    public void start()
    {
        this.minio.start();
    }

    @Override
    public void close()
    {
        minio.close();
    }

    public String getMinioAddress()
    {
        return "http://" + minio.getHost() + ":" + minio.getMappedPort(MINIO_PORT);
    }

    public HostAndPort getMinioApiEndpoint()
    {
        return fromParts(minio.getHost(), minio.getMappedPort(MINIO_PORT));
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Map<String, String> env = ImmutableMap.<String, String>builder()
                .put("MINIO_ACCESS_KEY", MINIO_ACCESS_KEY)
                .put("MINIO_SECRET_KEY", MINIO_SECRET_KEY)
                .buildOrThrow();
        private Optional<Network> network = Optional.empty();

        private String[] networkAliases = new String[] {"minio"};

        private String hostname = "minio";

        public Builder withEnvVars(Map<String, String> env)
        {
            this.env = env;
            return this;
        }

        public Builder withNetwork(Network network)
        {
            this.network = Optional.of(network);
            return this;
        }

        public Builder withHostname(String hostname)
        {
            this.hostname = hostname;
            return this;
        }

        public MinioContainer build()
        {
            return new MinioContainer(env, hostname, networkAliases, network);
        }
    }
}
