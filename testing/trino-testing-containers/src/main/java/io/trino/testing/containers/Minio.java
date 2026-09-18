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
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.SECONDS;

public class Minio
        extends BaseTestContainer
{
    private static final Logger log = Logger.get(Minio.class);

    public static final String DEFAULT_IMAGE = DockerImageName.parse("cgr.dev/chainguard/minio@sha256:f767919bd003062ac69713cdce920eb922c9fa3388efe96264e78b763342ca1a")
            .asCanonicalNameString();
    public static final String DEFAULT_HOST_NAME = "minio";

    public static final int MINIO_API_PORT = 4566;
    public static final int MINIO_CONSOLE_PORT = 4567;

    // defaults
    public static final String MINIO_ROOT_USER = "accesskey";
    public static final String MINIO_ROOT_PASSWORD = "secretkey";
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
        super(image,
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
        withCreateContainerModifier(cmd -> cmd.withUser("root")); // Required to create buckets externally
        withRunCommand(
                ImmutableList.of(
                        "server",
                        "--address",
                        "0.0.0.0:" + MINIO_API_PORT,
                        "--console-address",
                        "0.0.0.0:" + MINIO_CONSOLE_PORT,
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
        try (S3Client client = createS3Client()) {
            // MinIO can return "Server not initialized, please try again" for some time after the container starts.
            RetryPolicy<Object> retryPolicy = RetryPolicy.builder()
                    .withMaxDuration(Duration.of(2, MINUTES))
                    .withMaxAttempts(Integer.MAX_VALUE) // limited by MaxDuration
                    .withDelay(Duration.of(10, SECONDS))
                    .build();
            Failsafe.with(retryPolicy).run(() -> client.createBucket(builder -> builder.bucket(bucketName)));
        }
    }

    private S3Client createS3Client()
    {
        return S3Client.builder()
                .endpointOverride(URI.create(getMinioAddress()))
                .region(Region.of(MINIO_REGION))
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(MINIO_ROOT_USER, MINIO_ROOT_PASSWORD)))
                .build();
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
                    .put("MINIO_ROOT_USER", MINIO_ROOT_USER)
                    .put("MINIO_ROOT_PASSWORD", MINIO_ROOT_PASSWORD)
                    .buildOrThrow();
        }

        @Override
        public Minio build()
        {
            return new Minio(image, hostName, exposePorts, filesToMount, envVars, network, startupRetryLimit);
        }
    }
}
