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
package io.trino.plugin.deltalake.util;

import com.google.common.collect.ImmutableMap;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import java.io.Closeable;
import java.time.Duration;

import static java.lang.String.format;

public final class MinioContainer
        implements Closeable
{
    public static final String MINIO_ACCESS_KEY = "minio-access-key";
    public static final String MINIO_SECRET_KEY = "minio-secret-key";
    private static final int MINIO_PORT = 9080;

    private final DockerContainer minio;

    public MinioContainer(Network network)
    {
        minio = new DockerContainer("minio/minio:RELEASE.2021-07-15T22-27-34Z")
                .withEnv(ImmutableMap.<String, String>builder()
                        .put("MINIO_ACCESS_KEY", MINIO_ACCESS_KEY)
                        .put("MINIO_SECRET_KEY", MINIO_SECRET_KEY)
                        .buildOrThrow())
                .withNetwork(network)
                .withNetworkAliases("minio")
                .withCommand("server", "--address", format("0.0.0.0:%d", MINIO_PORT), "/data")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(new SelectedPortWaitStrategy(MINIO_PORT))
                .withStartupTimeout(Duration.ofMinutes(1));

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
}
