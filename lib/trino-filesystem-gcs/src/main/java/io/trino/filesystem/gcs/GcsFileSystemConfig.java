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
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.DefunctConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

@DefunctConfig("gcs.use-access-token")
public class GcsFileSystemConfig
{
    public enum AuthType
    {
        ACCESS_TOKEN,
        SERVICE_ACCOUNT,
        APPLICATION_DEFAULT,
    }

    public enum FileAccessPattern
    {
        RANDOM,
        SEQUENTIAL,
        AUTO_SEQUENTIAL,
        AUTO_RANDOM,
    }

    private DataSize readBlockSize = DataSize.of(2, MEGABYTE);
    private DataSize writeBlockSize = DataSize.of(16, MEGABYTE);
    private int pageSize = 100;
    private int batchSize = 100;

    private String projectId;
    private Optional<String> userProject = Optional.empty();
    private Optional<String> endpoint = Optional.empty();
    private Optional<String> clientLibToken = Optional.empty();

    private AuthType authType = AuthType.SERVICE_ACCOUNT;
    private int maxRetries = 20;
    private double backoffScaleFactor = 3.0;
    private Duration maxRetryTime = new Duration(25, TimeUnit.SECONDS);
    private Duration minBackoffDelay = new Duration(10, TimeUnit.MILLISECONDS);
    // Note: there is no benefit to setting this much higher as the rpc quota is 1x per second: https://cloud.google.com/storage/docs/retry-strategy#java
    private Duration maxBackoffDelay = new Duration(2000, TimeUnit.MILLISECONDS);
    private String applicationId = "Trino";
    private boolean analyticsCoreEnabled;
    private FileAccessPattern analyticsCoreFileAccessPattern = FileAccessPattern.AUTO_RANDOM;
    private boolean analyticsCoreFooterPrefetchEnabled = true;
    private int analyticsCoreReadThreadCount = 16;
    private Optional<String> decryptionKey = Optional.empty();

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

    public Optional<String> getUserProject()
    {
        return userProject;
    }

    @ConfigDescription("Project ID whose Google Cloud Project's billing account should be charged for the operation being executed.")
    @Config("gcs.user-project")
    public GcsFileSystemConfig setUserProject(Optional<String> userProject)
    {
        this.userProject = userProject;
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

    public Optional<String> getClientLibToken()
    {
        return clientLibToken;
    }

    @Config("gcs.client-lib-token")
    public GcsFileSystemConfig setClientLibToken(Optional<String> clientLibToken)
    {
        this.clientLibToken = clientLibToken;
        return this;
    }

    @NotNull
    public AuthType getAuthType()
    {
        return authType;
    }

    @Config("gcs.auth-type")
    public GcsFileSystemConfig setAuthType(AuthType authType)
    {
        this.authType = authType;
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

    public boolean isAnalyticsCoreEnabled()
    {
        return analyticsCoreEnabled;
    }

    @Config("gcs.analytics-core.enabled")
    @ConfigDescription("Enable Google Cloud Storage Analytics Core, which provides read implementations optimized for analytics workloads")
    public GcsFileSystemConfig setAnalyticsCoreEnabled(boolean analyticsCoreEnabled)
    {
        this.analyticsCoreEnabled = analyticsCoreEnabled;
        return this;
    }

    @NotNull
    public FileAccessPattern getAnalyticsCoreFileAccessPattern()
    {
        return analyticsCoreFileAccessPattern;
    }

    @Config("gcs.analytics-core.file-access-pattern")
    @ConfigDescription("File access pattern hint for Analytics Core reads. RANDOM is best for Parquet/columnar formats, SEQUENTIAL for full file scans, AUTO_RANDOM adapts but prefers random access")
    public GcsFileSystemConfig setAnalyticsCoreFileAccessPattern(FileAccessPattern analyticsCoreFileAccessPattern)
    {
        this.analyticsCoreFileAccessPattern = analyticsCoreFileAccessPattern;
        return this;
    }

    public boolean isAnalyticsCoreFooterPrefetchEnabled()
    {
        return analyticsCoreFooterPrefetchEnabled;
    }

    @Config("gcs.analytics-core.footer-prefetch-enabled")
    @ConfigDescription("Enable prefetching of file footer data for Analytics Core reads")
    public GcsFileSystemConfig setAnalyticsCoreFooterPrefetchEnabled(boolean analyticsCoreFooterPrefetchEnabled)
    {
        this.analyticsCoreFooterPrefetchEnabled = analyticsCoreFooterPrefetchEnabled;
        return this;
    }

    @Min(1)
    public int getAnalyticsCoreReadThreadCount()
    {
        return analyticsCoreReadThreadCount;
    }

    @Config("gcs.analytics-core.read-thread-count")
    @ConfigDescription("Number of threads used for Analytics Core parallel reads")
    public GcsFileSystemConfig setAnalyticsCoreReadThreadCount(int analyticsCoreReadThreadCount)
    {
        this.analyticsCoreReadThreadCount = analyticsCoreReadThreadCount;
        return this;
    }

    public Optional<String> getDecryptionKey()
    {
        return decryptionKey;
    }

    @Config("gcs.client.decryption-key")
    @ConfigSecuritySensitive
    @ConfigDescription("Decryption key for Google Cloud Storage")
    public GcsFileSystemConfig setDecryptionKey(Optional<String> decryptionKey)
    {
        this.decryptionKey = decryptionKey;
        return this;
    }

    @AssertTrue(message = "gcs.client.min-backoff-delay must be less than or equal to gcs.client.max-backoff-delay")
    public boolean isRetryDelayValid()
    {
        return minBackoffDelay.compareTo(maxBackoffDelay) <= 0;
    }

    @AssertTrue(message = "gcs.client.decryption-key must be base64 encoded")
    public boolean isDecryptionKeyValid()
    {
        try {
            decryptionKey.ifPresent(key -> {
                Base64.getDecoder().decode(key);
            });
            return true;
        }
        catch (IllegalArgumentException _) {
            return false;
        }
    }
}
