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
package io.trino.plugin.exchange.filesystem.s3;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.StorageClass;

import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.Optional;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.MINUTES;

public class ExchangeS3Config
{
    private String s3AwsAccessKey;
    private String s3AwsSecretKey;
    private Optional<String> s3IamRole = Optional.empty();
    private Optional<String> s3ExternalId = Optional.empty();
    private Optional<Region> s3Region = Optional.empty();
    private Optional<String> s3Endpoint = Optional.empty();
    private int s3MaxErrorRetries = 10;
    // Default to S3 multi-part upload minimum size to avoid excessive memory consumption from buffering
    private DataSize s3UploadPartSize = DataSize.of(5, MEGABYTE);
    private StorageClass storageClass = StorageClass.STANDARD;
    private RetryMode retryMode = RetryMode.ADAPTIVE;
    private int asyncClientConcurrency = 100;
    private int asyncClientMaxPendingConnectionAcquires = 10000;
    private Duration connectionAcquisitionTimeout = new Duration(1, MINUTES);
    private boolean s3PathStyleAccess;
    private Optional<String> gcsJsonKeyFilePath = Optional.empty();
    private Optional<String> gcsJsonKey = Optional.empty();

    public String getS3AwsAccessKey()
    {
        return s3AwsAccessKey;
    }

    @Config("exchange.s3.aws-access-key")
    public ExchangeS3Config setS3AwsAccessKey(String s3AwsAccessKey)
    {
        this.s3AwsAccessKey = s3AwsAccessKey;
        return this;
    }

    public String getS3AwsSecretKey()
    {
        return s3AwsSecretKey;
    }

    @Config("exchange.s3.aws-secret-key")
    @ConfigSecuritySensitive
    public ExchangeS3Config setS3AwsSecretKey(String s3AwsSecretKey)
    {
        this.s3AwsSecretKey = s3AwsSecretKey;
        return this;
    }

    public Optional<String> getS3IamRole()
    {
        return s3IamRole;
    }

    @Config("exchange.s3.iam-role")
    @ConfigDescription("ARN of an IAM role to assume when connecting to S3")
    public ExchangeS3Config setS3IamRole(String s3IamRole)
    {
        this.s3IamRole = Optional.ofNullable(s3IamRole);
        return this;
    }

    public Optional<String> getS3ExternalId()
    {
        return s3ExternalId;
    }

    @Config("exchange.s3.external-id")
    @ConfigDescription("External ID for the IAM role trust policy when connecting to S3")
    public ExchangeS3Config setS3ExternalId(String s3ExternalId)
    {
        this.s3ExternalId = Optional.ofNullable(s3ExternalId);
        return this;
    }

    public Optional<Region> getS3Region()
    {
        return s3Region;
    }

    @Config("exchange.s3.region")
    public ExchangeS3Config setS3Region(String s3Region)
    {
        if (s3Region != null) {
            this.s3Region = Optional.of(Region.of(s3Region.toLowerCase(ENGLISH)));
        }

        return this;
    }

    public Optional<String> getS3Endpoint()
    {
        return s3Endpoint;
    }

    @Config("exchange.s3.endpoint")
    public ExchangeS3Config setS3Endpoint(String s3Endpoint)
    {
        this.s3Endpoint = Optional.ofNullable(s3Endpoint);
        return this;
    }

    @AssertTrue(message = "Either exchange.s3.region or exchange.s3.endpoint is expected to be set")
    public boolean isEndpointOrRegionSet()
    {
        return s3Region.isPresent() || s3Endpoint.isPresent();
    }

    @Min(0)
    public int getS3MaxErrorRetries()
    {
        return s3MaxErrorRetries;
    }

    @Config("exchange.s3.max-error-retries")
    public ExchangeS3Config setS3MaxErrorRetries(int s3MaxErrorRetries)
    {
        this.s3MaxErrorRetries = s3MaxErrorRetries;
        return this;
    }

    @NotNull
    @MinDataSize("5MB")
    @MaxDataSize("256MB")
    public DataSize getS3UploadPartSize()
    {
        return s3UploadPartSize;
    }

    @Config("exchange.s3.upload.part-size")
    @ConfigDescription("Part size for S3 multi-part upload")
    public ExchangeS3Config setS3UploadPartSize(DataSize s3UploadPartSize)
    {
        this.s3UploadPartSize = s3UploadPartSize;
        return this;
    }

    @NotNull
    public StorageClass getStorageClass()
    {
        return storageClass;
    }

    @Config("exchange.s3.storage-class")
    public ExchangeS3Config setStorageClass(StorageClass storageClass)
    {
        this.storageClass = storageClass;
        return this;
    }

    @NotNull
    public RetryMode getRetryMode()
    {
        return retryMode;
    }

    @Config("exchange.s3.retry-mode")
    public ExchangeS3Config setRetryMode(RetryMode retryMode)
    {
        this.retryMode = retryMode;
        return this;
    }

    @Min(1)
    public int getAsyncClientConcurrency()
    {
        return asyncClientConcurrency;
    }

    @Config("exchange.s3.async-client-concurrency")
    public ExchangeS3Config setAsyncClientConcurrency(int asyncClientConcurrency)
    {
        this.asyncClientConcurrency = asyncClientConcurrency;
        return this;
    }

    @Min(1)
    public int getAsyncClientMaxPendingConnectionAcquires()
    {
        return asyncClientMaxPendingConnectionAcquires;
    }

    @Config("exchange.s3.async-client-max-pending-connection-acquires")
    public ExchangeS3Config setAsyncClientMaxPendingConnectionAcquires(int asyncClientMaxPendingConnectionAcquires)
    {
        this.asyncClientMaxPendingConnectionAcquires = asyncClientMaxPendingConnectionAcquires;
        return this;
    }

    public Duration getConnectionAcquisitionTimeout()
    {
        return connectionAcquisitionTimeout;
    }

    @Config("exchange.s3.async-client-connection-acquisition-timeout")
    public ExchangeS3Config setConnectionAcquisitionTimeout(Duration connectionAcquisitionTimeout)
    {
        this.connectionAcquisitionTimeout = connectionAcquisitionTimeout;
        return this;
    }

    public boolean isS3PathStyleAccess()
    {
        return s3PathStyleAccess;
    }

    @Config("exchange.s3.path-style-access")
    @ConfigDescription("Use path-style access for all request to S3")
    public ExchangeS3Config setS3PathStyleAccess(boolean s3PathStyleAccess)
    {
        this.s3PathStyleAccess = s3PathStyleAccess;
        return this;
    }

    public Optional<@FileExists String> getGcsJsonKeyFilePath()
    {
        return gcsJsonKeyFilePath;
    }

    @Config("exchange.gcs.json-key-file-path")
    @ConfigDescription("Path to the JSON file that contains your Google Cloud Platform service account key. Not to be set together with `exchange.gcs.json-key`")
    public ExchangeS3Config setGcsJsonKeyFilePath(String gcsJsonKeyFilePath)
    {
        this.gcsJsonKeyFilePath = Optional.ofNullable(gcsJsonKeyFilePath);
        return this;
    }

    public Optional<String> getGcsJsonKey()
    {
        return gcsJsonKey;
    }

    @Config("exchange.gcs.json-key")
    @ConfigDescription("Your Google Cloud Platform service account key in JSON format. Not to be set together with `exchange.gcs.json-key-file-path`")
    @ConfigSecuritySensitive
    public ExchangeS3Config setGcsJsonKey(String gcsJsonKey)
    {
        this.gcsJsonKey = Optional.ofNullable(gcsJsonKey);
        return this;
    }
}
