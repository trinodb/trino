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

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemLoader.S3ClientFactory;
import io.trino.spi.security.ConnectorIdentity;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import static java.util.Objects.requireNonNull;

final class S3SecurityMappingFileSystemFactory
        implements TrinoFileSystemFactory
{
    private final S3SecurityMappingProvider mappingProvider;
    private final S3ClientFactory clientFactory;
    private final S3Presigner preSigner;
    private final S3Context context;
    private final Location location;
    private final Executor uploadExecutor;
    private final Map<Optional<S3SecurityMappingResult>, S3Client> clients = new ConcurrentHashMap<>();

    public S3SecurityMappingFileSystemFactory(
            S3SecurityMappingProvider mappingProvider,
            S3ClientFactory clientFactory,
            S3Presigner preSigner,
            S3Context context,
            Location location,
            Executor uploadExecutor)
    {
        this.mappingProvider = requireNonNull(mappingProvider, "mappingProvider is null");
        this.uploadExecutor = requireNonNull(uploadExecutor, "uploadExecutor is null");
        this.clientFactory = requireNonNull(clientFactory, "clientFactory is null");
        this.preSigner = requireNonNull(preSigner, "preSigner is null");
        this.location = requireNonNull(location, "location is null");
        this.context = requireNonNull(context, "context is null");
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        Optional<S3SecurityMappingResult> mapping = mappingProvider.getMapping(identity, location);

        S3Client client = clients.computeIfAbsent(mapping, _ -> clientFactory.create(mapping));
        S3Context context = this.context.withCredentials(identity);

        if (mapping.isPresent() && mapping.get().kmsKeyId().isPresent()) {
            context = context.withKmsKeyId(mapping.get().kmsKeyId().get());
        }

        return new S3FileSystem(uploadExecutor, client, preSigner, context);
    }
}
