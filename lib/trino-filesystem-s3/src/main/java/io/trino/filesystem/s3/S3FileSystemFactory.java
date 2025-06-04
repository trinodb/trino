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
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

import java.util.concurrent.Executor;

public final class S3FileSystemFactory
        implements TrinoFileSystemFactory
{
    private final S3FileSystemLoader loader;
    private final S3Client client;
    private final S3Context context;
    private final Executor uploadExecutor;
    private final S3Presigner preSigner;

    @Inject
    public S3FileSystemFactory(OpenTelemetry openTelemetry, S3FileSystemConfig config, S3FileSystemStats stats)
    {
        this.loader = new S3FileSystemLoader(openTelemetry, config, stats);
        this.client = loader.createClient();
        this.preSigner = loader.createPreSigner();
        this.context = loader.context();
        this.uploadExecutor = loader.uploadExecutor();
    }

    @PreDestroy
    public void destroy()
    {
        try (client) {
            loader.destroy();
        }
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        return new S3FileSystem(uploadExecutor, client, preSigner, context.withCredentials(identity));
    }
}
