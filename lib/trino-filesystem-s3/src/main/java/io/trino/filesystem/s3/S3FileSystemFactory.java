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
package io.trino.filesystem.s3;

import com.google.inject.Inject;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import jakarta.annotation.PreDestroy;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import static io.trino.filesystem.s3.S3FileSystemUtils.createS3PreSigner;

public final class S3FileSystemFactory
        implements TrinoFileSystemFactory
{
    private final S3FileSystemLoader loader;
    private final S3Client client;
    private final S3Context context;
    private final Executor uploadExecutor;
    private final S3Presigner preSigner;
    private final String staticRegion;
    private final String staticEndpoint;
    private final boolean staticCrossRegionAccessEnabled;
    private final Map<ClientKey, S3Client> dynamicClients;

    @Inject
    public S3FileSystemFactory(OpenTelemetry openTelemetry, S3FileSystemConfig config, S3FileSystemStats stats)
    {
        this.loader = new S3FileSystemLoader(openTelemetry, config, stats);
        this.client = loader.createClient();
        this.preSigner = createS3PreSigner(config, client);
        this.context = loader.context();
        this.uploadExecutor = loader.uploadExecutor();
        this.staticRegion = config.getRegion();
        this.staticEndpoint = config.getEndpoint();
        this.staticCrossRegionAccessEnabled = config.isCrossRegionAccessEnabled();
        this.dynamicClients = new ConcurrentHashMap<>();
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        Optional<AwsCredentials> vendedCredentials = S3FileSystemLoader.extractCredentialsFromIdentity(identity);
        String region = S3FileSystemLoader.extractRegionFromIdentity(identity).orElse(staticRegion);
        String endpoint = S3FileSystemLoader.extractEndpointFromIdentity(identity).orElse(staticEndpoint);

        boolean crossRegionAccessEnabled;
        if (staticCrossRegionAccessEnabled) {
            crossRegionAccessEnabled = true;
        }
        else {
            crossRegionAccessEnabled = S3FileSystemLoader.extractCrossRegionAccessEnabledFromIdentity(identity).orElse(false);
        }

        boolean hasOverrides = vendedCredentials.isPresent()
                || (region != null && !region.equals(staticRegion))
                || (endpoint != null && !endpoint.equals(staticEndpoint))
                || crossRegionAccessEnabled != staticCrossRegionAccessEnabled;

        if (hasOverrides) {
            ClientKey key = new ClientKey(vendedCredentials.orElse(null), region, endpoint, crossRegionAccessEnabled);
            S3Client s3Client = dynamicClients.computeIfAbsent(key, _ ->
                    loader.createClientWithOverrides(vendedCredentials, region, endpoint, crossRegionAccessEnabled));
            return new S3FileSystem(uploadExecutor, s3Client, preSigner, context.withCredentials(identity));
        }

        return new S3FileSystem(uploadExecutor, client, preSigner, context.withCredentials(identity));
    }

    @PreDestroy
    public void destroy()
    {
        for (S3Client dynamicClient : dynamicClients.values()) {
            try (var _ = dynamicClient) {
                // Resource automatically closed
            }
        }
        dynamicClients.clear();
        try (var _ = client) {
            // Resource automatically closed
        }
        loader.destroy();
    }

    private record ClientKey(AwsCredentials credentials, String region, String endpoint, boolean crossRegionAccessEnabled) {}
}
