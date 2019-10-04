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
package io.prestosql.plugin.kinesis;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class KinesisConfig
{
    private String defaultSchema = "default";
    private String tableDescriptionLocation = "etc/kinesis/";
    private boolean hideInternalColumns = true;
    private String awsRegion = "us-east-1";
    private int batchSize = 10000;
    private int maxBatches = 600;
    private int fetchAttempts = 2;
    private Duration sleepTime = new Duration(1000, TimeUnit.MILLISECONDS);
    private boolean isIteratorFromTimestamp = true;
    private long iteratorOffsetSeconds = 86400;
    private String accessKey;
    private String secretKey;
    private boolean logKinesisBatches = true;
    private boolean checkpointEnabled;
    private long dynamoReadCapacity = 50L;
    private long dynamoWriteCapacity = 10L;
    private Duration checkpointInterval = new Duration(60000, TimeUnit.MILLISECONDS);
    private String logicalProcessName = "process1";
    private int iteratorNumber;

    @NotNull
    public String getTableDescriptionLocation()
    {
        return tableDescriptionLocation;
    }

    @Config("kinesis.table-description-location")
    @ConfigDescription("S3 or local filesystem directory location where table schema descriptions are present")
    public KinesisConfig setTableDescriptionLocation(String tableDescriptionLocation)
    {
        this.tableDescriptionLocation = tableDescriptionLocation;
        return this;
    }

    public boolean isHideInternalColumns()
    {
        return hideInternalColumns;
    }

    @Config("kinesis.hide-internal-columns")
    @ConfigDescription("Toggle to decide whether to show Kinesis internal columns or not")
    public KinesisConfig setHideInternalColumns(boolean hideInternalColumns)
    {
        this.hideInternalColumns = hideInternalColumns;
        return this;
    }

    @NotNull
    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    @Config("kinesis.default-schema")
    @ConfigDescription("Sets default schema for kinesis catalogs")
    public KinesisConfig setDefaultSchema(String defaultSchema)
    {
        this.defaultSchema = defaultSchema;
        return this;
    }

    public String getAccessKey()
    {
        return this.accessKey;
    }

    @Config("kinesis.access-key")
    @ConfigDescription("S3 Access Key to access s3 locations")
    public KinesisConfig setAccessKey(String accessKey)
    {
        this.accessKey = accessKey;
        return this;
    }

    public String getSecretKey()
    {
        return this.secretKey;
    }

    @Config("kinesis.secret-key")
    @ConfigDescription("S3 Secret Key to access s3 locations")
    public KinesisConfig setSecretKey(String secretKey)
    {
        this.secretKey = secretKey;
        return this;
    }

    public String getAwsRegion()
    {
        return awsRegion;
    }

    @Config("kinesis.aws-region")
    @ConfigDescription("Region to set while creating S3 client")
    public KinesisConfig setAwsRegion(String awsRegion)
    {
        this.awsRegion = awsRegion;
        return this;
    }

    @Min(1)
    @Max(Integer.MAX_VALUE)
    public int getBatchSize()
    {
        return this.batchSize;
    }

    @Config("kinesis.batch-size")
    @ConfigDescription("Limit maximum number of rows to return in a batch")
    public KinesisConfig setBatchSize(int batchSize)
    {
        this.batchSize = batchSize;
        return this;
    }

    @Min(1)
    public int getMaxBatches()
    {
        return this.maxBatches;
    }

    @Config("kinesis.max-batches")
    @ConfigDescription("Maximum number of calls to Kinesis per query")
    public KinesisConfig setMaxBatches(int maxBatches)
    {
        this.maxBatches = maxBatches;
        return this;
    }

    @Min(1)
    @Max(1000)
    public int getFetchAttempts()
    {
        return this.fetchAttempts;
    }

    @Config("kinesis.fetch-attempts")
    @ConfigDescription("Maximum number of attempts to fetch the next batch from a shard iterator")
    public KinesisConfig setFetchAttempts(int fetchAttempts)
    {
        this.fetchAttempts = fetchAttempts;
        return this;
    }

    public Duration getSleepTime()
    {
        return this.sleepTime;
    }

    @Config("kinesis.sleep-time")
    @ConfigDescription("Sleep time between fetch attempt retries")
    public KinesisConfig setSleepTime(Duration sleepTime)
    {
        this.sleepTime = sleepTime;
        return this;
    }

    public boolean isLogBatches()
    {
        return logKinesisBatches;
    }

    @Config("kinesis.log-batches")
    @ConfigDescription("Decides whether to log batch fetch details")
    public KinesisConfig setLogBatches(boolean logBatches)
    {
        this.logKinesisBatches = logBatches;
        return this;
    }

    public boolean isIteratorFromTimestamp()
    {
        return isIteratorFromTimestamp;
    }

    @Config("kinesis.iterator-from-timestamp")
    @ConfigDescription("Whether to use start timestamp from shard iterator")
    public KinesisConfig setIteratorFromTimestamp(boolean isIteratorFromTimestamp)
    {
        this.isIteratorFromTimestamp = isIteratorFromTimestamp;
        return this;
    }

    public long getIteratorOffsetSeconds()
    {
        return iteratorOffsetSeconds;
    }

    @Config("kinesis.iterator-offset-seconds")
    @ConfigDescription("Seconds before current time to start fetching records from")
    public KinesisConfig setIteratorOffsetSeconds(long iteratorOffsetSeconds)
    {
        this.iteratorOffsetSeconds = iteratorOffsetSeconds;
        return this;
    }

    public boolean isCheckpointEnabled()
    {
        return checkpointEnabled;
    }

    @Config("kinesis.checkpoint-enabled")
    @ConfigDescription("Whether to remember last read sequence number and use it in later requests")
    public KinesisConfig setCheckpointEnabled(boolean checkpointEnabled)
    {
        this.checkpointEnabled = checkpointEnabled;
        return this;
    }

    public long getDynamoReadCapacity()
    {
        return dynamoReadCapacity;
    }

    @Config("kinesis.dynamo-read-capacity")
    @ConfigDescription("DynamoDB read capacity to be set in client")
    public KinesisConfig setDynamoReadCapacity(long dynamoReadCapacity)
    {
        this.dynamoReadCapacity = dynamoReadCapacity;
        return this;
    }

    public long getDynamoWriteCapacity()
    {
        return dynamoWriteCapacity;
    }

    @Config("kinesis.dynamo-write-capacity")
    @ConfigDescription("DynamoDB read capacity to be set in client")
    public KinesisConfig setDynamoWriteCapacity(long dynamoWriteCapacity)
    {
        this.dynamoWriteCapacity = dynamoWriteCapacity;
        return this;
    }

    public Duration getCheckpointInterval()
    {
        return checkpointInterval;
    }

    @Config("kinesis.checkpoint-interval")
    @ConfigDescription("Intervals at which to checkpoint shard iterator details")
    public KinesisConfig setCheckpointInterval(Duration checkpointInterval)
    {
        this.checkpointInterval = checkpointInterval;
        return this;
    }

    public String getLogicalProcessName()
    {
        return logicalProcessName;
    }

    @Config("kinesis.checkpoint-logical-name")
    @ConfigDescription("Prefix to the checkpoint name")
    public KinesisConfig setLogicalProcessName(String logicalPrcessName)
    {
        this.logicalProcessName = logicalPrcessName;
        return this;
    }

    @Min(0)
    public int getIteratorNumber()
    {
        return iteratorNumber;
    }

    @Config("kinesis.iterator-number")
    @ConfigDescription("Checkpoint iteration number")
    public KinesisConfig setIteratorNumber(int iteratorNumber)
    {
        this.iteratorNumber = iteratorNumber;
        return this;
    }
}
