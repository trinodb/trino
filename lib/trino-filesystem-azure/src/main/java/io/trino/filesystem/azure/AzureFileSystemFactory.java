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
package io.trino.filesystem.azure;

import com.azure.core.http.HttpClient;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class AzureFileSystemFactory
        implements TrinoFileSystemFactory
{
    // All file systems share a single HTTP client
    private final HttpClient httpClient = HttpClient.createDefault();

    private final AzureAuth auth;
    private final DataSize readBlockSize;
    private final DataSize writeBlockSize;
    private final int maxWriteConcurrency;
    private final DataSize maxSingleUploadSize;

    @Inject
    public AzureFileSystemFactory(AzureAuth azureAuth, AzureFileSystemConfig config)
    {
        this(
                azureAuth,
                config.getReadBlockSize(),
                config.getWriteBlockSize(),
                config.getMaxWriteConcurrency(),
                config.getMaxSingleUploadSize());
    }

    public AzureFileSystemFactory(AzureAuth azureAuth, DataSize readBlockSize, DataSize writeBlockSize, int maxWriteConcurrency, DataSize maxSingleUploadSize)
    {
        this.auth = requireNonNull(azureAuth, "azureAuth is null");
        this.readBlockSize = requireNonNull(readBlockSize, "readBlockSize is null");
        this.writeBlockSize = requireNonNull(writeBlockSize, "writeBlockSize is null");
        checkArgument(maxWriteConcurrency >= 0, "maxWriteConcurrency is negative");
        this.maxWriteConcurrency = maxWriteConcurrency;
        this.maxSingleUploadSize = requireNonNull(maxSingleUploadSize, "maxSingleUploadSize is null");
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        return new AzureFileSystem(httpClient, auth, readBlockSize, writeBlockSize, maxWriteConcurrency, maxSingleUploadSize);
    }
}
