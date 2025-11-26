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
package io.trino.filesystem.gcs;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.AssertFalse;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class GcsFileSystemConfig
{
    public enum AuthType
    {
        ACCESS_TOKEN,
        SERVICE_ACCOUNT,
        APPLICATION_DEFAULT;
    }

    private DataSize readBlockSize = DataSize.of(2, MEGABYTE);
    private DataSize writeBlockSize = DataSize.of(16, MEGABYTE);
    private int pageSize = 100;
    private int batchSize = 100;

    private String projectId;
    private Optional<String> endpoint = Optional.empty();

    private Optional<Boolean> useGcsAccessToken = Optional.empty();
    private Optional<AuthType> authType = Optional.empty();
    private int maxRetries = 20;
    private double backoffScaleFactor = 3.0;
    private Duration maxRetryTime = new Duration(25, TimeUnit.SECONDS);
    private Duration minBackoffDelay = new Duration(10, TimeUnit.MILLISECONDS);
    // Note: there is no benefit to setting this much higher as the rpc quota is 1x per second: https://cloud.google.com/storage/docs/retry-strategy#java
    private Duration maxBackoffDelay = new Duration(2000, TimeUnit.MILLISECONDS);
    private String applicationId = "Trino";

    @NotNull
    public DataSize getReadBlockSize()
    {
        return readBlockSize;
    }

    @Config("gcs.read-block-size")
    @ConfigDescription("Minimum size that will be read in one RPC. The default size is 2MiB, see com.google.cloud.BaseStorageReadChannel.")
    public GcsFileSystemConfig setReadBlockSize(DataSize readBlockSize)
    {
        this.readBlockSize = readBlockSize;
        return this;
    }

    @NotNull
    public DataSize getWriteBlockSize()
    {
        return writeBlockSize;
    }

    @Config("gcs.write-block-size")
    @ConfigDescription("Minimum size that will be written in one RPC. The default size is 16MiB, see com.google.cloud.BaseStorageWriteChannel.")
    public GcsFileSystemConfig setWriteBlockSize(DataSize writeBlockSize)
    {
        this.writeBlockSize = writeBlockSize;
        return this;
    }

    @Min(1)
    public int getPageSize()
    {
        return pageSize;
    }

    @Config("gcs.page-size")
    @ConfigDescription("The maximum number of blobs to return per page.")
    public GcsFileSystemConfig setPageSize(int pageSize)
    {
        this.pageSize = pageSize;
        return this;
    }

    @Min(1)
    public int getBatchSize()
    {
        return batchSize;
    }

    @Config("gcs.batch-size")
    @ConfigDescription("Number of blobs to delete per batch. Recommended batch size is 100: https://cloud.google.com/storage/docs/batch")
    public GcsFileSystemConfig setBatchSize(int batchSize)
    {
        this.batchSize = batchSize;
        return this;
    }

    @Nullable
    public String getProjectId()
    {
        return projectId;
    }

    @Config("gcs.project-id")
    public GcsFileSystemConfig setProjectId(String projectId)
    {
        this.projectId = projectId;
        return this;
    }

    public Optional<String> getEndpoint()
    {
        return endpoint;
    }

    @ConfigDescription("Endpoint to use for GCS requests")
    @Config("gcs.endpoint")
    public GcsFileSystemConfig setEndpoint(Optional<String> endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    @NotNull
    public AuthType getAuthType()
    {
        if (useGcsAccessToken.isPresent() && useGcsAccessToken.get()) {
            return AuthType.ACCESS_TOKEN;
        }
        return authType.orElse(AuthType.SERVICE_ACCOUNT);
    }

    @Config("gcs.auth-type")
    public GcsFileSystemConfig setAuthType(AuthType authType)
    {
        this.authType = Optional.of(authType);
        return this;
    }

    @Deprecated
    public boolean isUseGcsAccessToken()
    {
        return useGcsAccessToken.orElse(false);
    }

    @Deprecated
    @Config("gcs.use-access-token")
    public GcsFileSystemConfig setUseGcsAccessToken(boolean useGcsAccessToken)
    {
        this.useGcsAccessToken = Optional.of(useGcsAccessToken);
        return this;
    }

    @Min(0)
    public int getMaxRetries()
    {
        return maxRetries;
    }

    @Config("gcs.client.max-retries")
    @ConfigDescription("Maximum number of RPC attempts")
    public GcsFileSystemConfig setMaxRetries(int maxRetries)
    {
        this.maxRetries = maxRetries;
        return this;
    }

    @Min(1)
    public double getBackoffScaleFactor()
    {
        return backoffScaleFactor;
    }

    @Config("gcs.client.backoff-scale-factor")
    @ConfigDescription("Scale factor for RPC retry delay")
    public GcsFileSystemConfig setBackoffScaleFactor(double backoffScaleFactor)
    {
        this.backoffScaleFactor = backoffScaleFactor;
        return this;
    }

    @NotNull
    public Duration getMaxRetryTime()
    {
        return maxRetryTime;
    }

    @Config("gcs.client.max-retry-time")
    @ConfigDescription("Total time limit for an RPC to be retried")
    public GcsFileSystemConfig setMaxRetryTime(Duration maxRetryTime)
    {
        this.maxRetryTime = maxRetryTime;
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getMinBackoffDelay()
    {
        return minBackoffDelay;
    }

    @Config("gcs.client.min-backoff-delay")
    @ConfigDescription("Minimum delay between RPC retries")
    public GcsFileSystemConfig setMinBackoffDelay(Duration minBackoffDelay)
    {
        this.minBackoffDelay = minBackoffDelay;
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getMaxBackoffDelay()
    {
        return maxBackoffDelay;
    }

    @Config("gcs.client.max-backoff-delay")
    @ConfigDescription("Maximum delay between RPC retries.")
    public GcsFileSystemConfig setMaxBackoffDelay(Duration maxBackoffDelay)
    {
        this.maxBackoffDelay = maxBackoffDelay;
        return this;
    }

    @Size(max = 50)
    @NotNull
    public String getApplicationId()
    {
        return applicationId;
    }

    @Config("gcs.application-id")
    @ConfigDescription("Suffix that will be added to HTTP User-Agent header to identify the application")
    public GcsFileSystemConfig setApplicationId(String applicationId)
    {
        this.applicationId = applicationId;
        return this;
    }

    @AssertTrue(message = "gcs.client.min-backoff-delay must be less than or equal to gcs.client.max-backoff-delay")
    public boolean isRetryDelayValid()
    {
        return minBackoffDelay.compareTo(maxBackoffDelay) <= 0;
    }

    @AssertFalse(message = "Cannot set both gcs.use-access-token and gcs.auth-type")
    public boolean isAuthTypeAndGcsAccessTokenConfigured()
    {
        return authType.isPresent() && useGcsAccessToken.isPresent();
    }
}
