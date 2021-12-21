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
package io.trino.plugin.exchange.s3;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.DataSize;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.max;

public class ExchangeS3Config
{
    private String s3AwsAccessKey;
    private String s3AwsSecretKey;
    private String s3Region;
    private int s3MaxErrorRetries = 10;
    private int deletionThreadCount = max(1, Runtime.getRuntime().availableProcessors() / 2);
    private DataSize s3UploadPartSize = DataSize.of(5, MEGABYTE);

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

    public String getS3Region()
    {
        return s3Region;
    }

    @Config("exchange.s3.region")
    public ExchangeS3Config setS3Region(String s3Region)
    {
        this.s3Region = s3Region;
        return this;
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

    @Min(1)
    public int getDeletionThreadCount()
    {
        return deletionThreadCount;
    }

    @Config("exchange.s3.deletion.thread-count")
    public ExchangeS3Config setDeletionThreadCount(int deletionThreadCount)
    {
        this.deletionThreadCount = deletionThreadCount;
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
}
