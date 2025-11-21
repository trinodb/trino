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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import static java.lang.Math.max;

public class AzureFileSystemConfig
{
    public enum AuthType
    {
        ACCESS_KEY,
        OAUTH,
        DEFAULT,
    }

    private AuthType authType = AuthType.DEFAULT;
    private String endpoint = "core.windows.net";
    private DataSize readBlockSize = DataSize.of(4, Unit.MEGABYTE);
    private DataSize writeBlockSize = DataSize.of(4, Unit.MEGABYTE);
    private int maxWriteConcurrency = 8;
    private DataSize maxSingleUploadSize = DataSize.of(4, Unit.MEGABYTE);
    private Integer maxHttpRequests = 2 * Runtime.getRuntime().availableProcessors();
    /**
     * Matches {@link reactor.netty.resources.ConnectionProvider#DEFAULT_POOL_MAX_CONNECTIONS}
     */
    private int maxHttpConnections = 2 * max(Runtime.getRuntime().availableProcessors(), 8);
    private String applicationId = "Trino";
    private boolean multipartWriteEnabled;

    @NotNull
    public AuthType getAuthType()
    {
        return authType;
    }

    @Config("azure.auth-type")
    public AzureFileSystemConfig setAuthType(AuthType authType)
    {
        this.authType = authType;
        return this;
    }

    @NotEmpty
    public String getEndpoint()
    {
        return endpoint;
    }

    @Config("azure.endpoint")
    public AzureFileSystemConfig setEndpoint(String endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    @NotNull
    public DataSize getReadBlockSize()
    {
        return readBlockSize;
    }

    @Config("azure.read-block-size")
    public AzureFileSystemConfig setReadBlockSize(DataSize readBlockSize)
    {
        this.readBlockSize = readBlockSize;
        return this;
    }

    @NotNull
    public DataSize getWriteBlockSize()
    {
        return writeBlockSize;
    }

    @Config("azure.write-block-size")
    public AzureFileSystemConfig setWriteBlockSize(DataSize writeBlockSize)
    {
        this.writeBlockSize = writeBlockSize;
        return this;
    }

    public int getMaxWriteConcurrency()
    {
        return maxWriteConcurrency;
    }

    @Config("azure.max-write-concurrency")
    public AzureFileSystemConfig setMaxWriteConcurrency(int maxWriteConcurrency)
    {
        this.maxWriteConcurrency = maxWriteConcurrency;
        return this;
    }

    @NotNull
    public DataSize getMaxSingleUploadSize()
    {
        return maxSingleUploadSize;
    }

    @Config("azure.max-single-upload-size")
    public AzureFileSystemConfig setMaxSingleUploadSize(DataSize maxSingleUploadSize)
    {
        this.maxSingleUploadSize = maxSingleUploadSize;
        return this;
    }

    @Min(1)
    public int getMaxHttpRequests()
    {
        return maxHttpRequests;
    }

    @Config("azure.max-http-requests")
    @ConfigDescription("Maximum number of concurrent HTTP requests to Azure on every node")
    public AzureFileSystemConfig setMaxHttpRequests(int maxHttpRequests)
    {
        this.maxHttpRequests = maxHttpRequests;
        return this;
    }

    @Min(16)
    @Max(1024)
    public int getMaxHttpConnections()
    {
        return maxHttpConnections;
    }

    @Config("azure.max-http-connections")
    @ConfigDescription("Maximum number of pooled HTTP connections")
    public AzureFileSystemConfig setMaxHttpConnections(int maxHttpConnections)
    {
        this.maxHttpConnections = maxHttpConnections;
        return this;
    }

    @Size(max = 50)
    @NotNull
    public String getApplicationId()
    {
        return applicationId;
    }

    @Config("azure.application-id")
    @ConfigDescription("Suffix that will be added to HTTP User-Agent header to identify the application")
    public AzureFileSystemConfig setApplicationId(String applicationId)
    {
        this.applicationId = applicationId;
        return this;
    }

    public boolean isMultipartWriteEnabled()
    {
        return multipartWriteEnabled;
    }

    @Config("azure.multipart-write-enabled")
    @ConfigDescription("Enable multipart writes for large files")
    public AzureFileSystemConfig setMultipartWriteEnabled(boolean multipartWriteEnabled)
    {
        this.multipartWriteEnabled = multipartWriteEnabled;
        return this;
    }
}
