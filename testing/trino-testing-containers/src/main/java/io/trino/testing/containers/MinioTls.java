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
import io.airlift.log.Logger;
import org.testcontainers.containers.Network;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.testing.containers.Minio.DEFAULT_IMAGE;
import static io.trino.testing.containers.Minio.MINIO_API_PORT;
import static io.trino.testing.containers.Minio.MINIO_CONSOLE_PORT;
import static io.trino.testing.containers.Minio.MINIO_ROOT_PASSWORD;
import static io.trino.testing.containers.Minio.MINIO_ROOT_USER;
import static java.util.Objects.requireNonNull;

/**
 * MinIO test container configured for HTTPS using a caller-supplied
 * {@link TlsCertificate}. Modeled after {@link Minio} but exposes an
 * {@code https://} endpoint, as required by features such as S3 server-side
 * encryption with customer-provided keys (SSE-C).
 */
public class MinioTls
        extends BaseTestContainer
{
    private static final Logger log = Logger.get(MinioTls.class);

    private static final String CONTAINER_CERTS_DIR = "/certs";
    private static final String CONTAINER_CERTS_PUBLIC = CONTAINER_CERTS_DIR + "/public.crt";
    private static final String CONTAINER_CERTS_PRIVATE = CONTAINER_CERTS_DIR + "/private.key";

    public static Builder builder()
    {
        return new Builder();
    }

    private MinioTls(
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
        withRunCommand(ImmutableList.of(
                "server",
                "--address",
                "0.0.0.0:" + MINIO_API_PORT,
                "--console-address",
                "0.0.0.0:" + MINIO_CONSOLE_PORT,
                "--certs-dir",
                CONTAINER_CERTS_DIR,
                "/data"));
    }

    @Override
    public void start()
    {
        super.start();
        log.info("MinIO TLS container started with address for api: %s and console: %s", getMinioHttpsEndpoint(), getMinioConsoleEndpoint());
    }

    public String getMinioHttpsEndpoint()
    {
        return "https://" + getMappedHostAndPortForExposedPort(MINIO_API_PORT);
    }

    public String getMinioConsoleEndpoint()
    {
        return getMappedHostAndPortForExposedPort(MINIO_CONSOLE_PORT).toString();
    }

    public static class Builder
            extends BaseTestContainer.Builder<Builder, MinioTls>
    {
        private TlsCertificate certificate;

        private Builder()
        {
            this.image = DEFAULT_IMAGE;
            this.hostName = "minio-tls";
            this.exposePorts = ImmutableSet.of(MINIO_API_PORT, MINIO_CONSOLE_PORT);
            this.envVars = ImmutableMap.<String, String>builder()
                    .put("MINIO_ROOT_USER", MINIO_ROOT_USER)
                    .put("MINIO_ROOT_PASSWORD", MINIO_ROOT_PASSWORD)
                    .buildOrThrow();
        }

        public Builder withCertificate(TlsCertificate certificate)
        {
            this.certificate = requireNonNull(certificate, "certificate is null");
            return self;
        }

        @Override
        public MinioTls build()
        {
            requireNonNull(certificate, "certificate must be set via withCertificate()");
            Map<String, String> mounts = ImmutableMap.<String, String>builder()
                    .putAll(filesToMount)
                    .put(CONTAINER_CERTS_PUBLIC, certificate.publicCertificate().toString())
                    .put(CONTAINER_CERTS_PRIVATE, certificate.privateKey().toString())
                    .buildOrThrow();
            return new MinioTls(image, hostName, exposePorts, mounts, envVars, network, startupRetryLimit);
        }
    }
}
