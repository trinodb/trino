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
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import jakarta.annotation.PreDestroy;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

final class S3BucketCredentialFileSystemLoader
        implements Function<Location, TrinoFileSystemFactory>
{
    private final S3FileSystemLoader delegate;
    private final Map<String, BucketFileSystemFactory> factoriesByBucket = new ConcurrentHashMap<>();

    @Inject
    S3BucketCredentialFileSystemLoader(OpenTelemetry openTelemetry, S3FileSystemConfig config, S3FileSystemStats stats, S3SecretsCredentialResolver secretsCredentialResolver)
    {
        this.delegate = new S3FileSystemLoader(openTelemetry, config, stats, secretsCredentialResolver);
    }

    @Override
    public TrinoFileSystemFactory apply(Location location)
    {
        String bucket = new S3Location(location).bucket();
        return factoriesByBucket.computeIfAbsent(bucket, delegate::createFactoryForBucket);
    }

    @PreDestroy
    public void destroy()
    {
        factoriesByBucket.values().forEach(BucketFileSystemFactory::destroy);
        delegate.destroy();
    }

    static final class BucketFileSystemFactory
            implements TrinoFileSystemFactory
    {
        private final S3Client client;
        private final S3Presigner preSigner;
        private final S3Context context;
        private final Executor uploadExecutor;

        BucketFileSystemFactory(S3Client client, S3Presigner preSigner, S3Context context, Executor uploadExecutor)
        {
            this.client = requireNonNull(client, "client is null");
            this.preSigner = requireNonNull(preSigner, "preSigner is null");
            this.context = requireNonNull(context, "context is null");
            this.uploadExecutor = requireNonNull(uploadExecutor, "uploadExecutor is null");
        }

        @Override
        public TrinoFileSystem create(ConnectorIdentity identity)
        {
            return new S3FileSystem(uploadExecutor, client, preSigner, context.withCredentials(identity));
        }

        void destroy()
        {
            try (client) {
                preSigner.close();
            }
        }
    }
}
