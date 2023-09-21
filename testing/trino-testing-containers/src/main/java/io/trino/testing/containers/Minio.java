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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.google.common.reflect.ClassPath;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.trino.testing.minio.MinioClient;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.regex.Matcher.quoteReplacement;

public class Minio
        extends BaseTestContainer
{
    private static final Logger log = Logger.get(Minio.class);

    public static final String DEFAULT_IMAGE = "minio/minio:RELEASE.2023-05-18T00-05-36Z";
    public static final String DEFAULT_HOST_NAME = "minio";

    public static final int MINIO_API_PORT = 4566;
    public static final int MINIO_CONSOLE_PORT = 4567;

    // defaults
    public static final String MINIO_ACCESS_KEY = "accesskey";
    public static final String MINIO_SECRET_KEY = "secretkey";
    public static final String MINIO_REGION = "us-east-1";

    public static Builder builder()
    {
        return new Builder();
    }

    private Minio(
            String image,
            String hostName,
            Set<Integer> exposePorts,
            Map<String, String> filesToMount,
            Map<String, String> envVars,
            Optional<Network> network,
            int retryLimit)
    {
        super(
                image,
                hostName,
                exposePorts,
                filesToMount,
                envVars,
                network,
                retryLimit);
    }

    @Override
    protected void setupContainer()
    {
        super.setupContainer();
        withRunCommand(
                ImmutableList.of(
                        "server",
                        "--address", "0.0.0.0:" + MINIO_API_PORT,
                        "--console-address", "0.0.0.0:" + MINIO_CONSOLE_PORT,
                        "/data"));
    }

    @Override
    public void start()
    {
        super.start();
        log.info("MinIO container started with address for api: http://%s and console: http://%s", getMinioApiEndpoint(), getMinioConsoleEndpoint());
    }

    public HostAndPort getMinioApiEndpoint()
    {
        return getMappedHostAndPortForExposedPort(MINIO_API_PORT);
    }

    public String getMinioAddress()
    {
        return "http://" + getMinioApiEndpoint();
    }

    public HostAndPort getMinioConsoleEndpoint()
    {
        return getMappedHostAndPortForExposedPort(MINIO_CONSOLE_PORT);
    }

    public void createBucket(String bucketName)
    {
        try (MinioClient minioClient = createMinioClient()) {
            // use retry loop for minioClient.makeBucket as minio container tends to return "Server not initialized, please try again" error
            // for some time after starting up
            RetryPolicy<Object> retryPolicy = RetryPolicy.builder()
                    .withMaxDuration(Duration.of(2, MINUTES))
                    .withMaxAttempts(Integer.MAX_VALUE) // limited by MaxDuration
                    .withDelay(Duration.of(10, SECONDS))
                    .build();
            Failsafe.with(retryPolicy).run(() -> minioClient.makeBucket(bucketName));
        }
    }

    public void copyResources(String resourcePath, String bucketName, String target)
    {
        try (MinioClient minioClient = createMinioClient()) {
            for (ClassPath.ResourceInfo resourceInfo : ClassPath.from(MinioClient.class.getClassLoader())
                    .getResources()) {
                if (resourceInfo.getResourceName().startsWith(resourcePath)) {
                    String fileName = resourceInfo.getResourceName().replaceFirst("^" + Pattern.quote(resourcePath), quoteReplacement(target));
                    minioClient.putObject(bucketName, resourceInfo.asByteSource().read(), fileName);
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void writeFile(byte[] contents, String bucketName, String path)
    {
        try (MinioClient minioClient = createMinioClient()) {
            minioClient.putObject(bucketName, contents, path);
        }
    }

    public MinioClient createMinioClient()
    {
        return new MinioClient(getMinioAddress(), MINIO_ACCESS_KEY, MINIO_SECRET_KEY);
    }

    public static class Builder
            extends BaseTestContainer.Builder<Minio.Builder, Minio>
    {
        private Builder()
        {
            this.image = DEFAULT_IMAGE;
            this.hostName = DEFAULT_HOST_NAME;
            this.exposePorts =
                    ImmutableSet.of(
                            MINIO_API_PORT,
                            MINIO_CONSOLE_PORT);
            this.envVars = ImmutableMap.<String, String>builder()
                    .put("MINIO_ACCESS_KEY", MINIO_ACCESS_KEY)
                    .put("MINIO_SECRET_KEY", MINIO_SECRET_KEY)
                    .buildOrThrow();
        }

        @Override
        public Minio build()
        {
            return new Minio(image, hostName, exposePorts, filesToMount, envVars, network, startupRetryLimit);
        }
    }
}
