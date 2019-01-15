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
package com.qubole.presto.kinesis;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

/**
 * This Class handles all the configuration settings that is stored in /etc/catalog/kinesis.properties file
 */
public class KinesisConnectorConfig
{
    /**
     * The schema name to use in the connector.
     */
    private String defaultSchema = "default";

    /**
     * Folder holding the JSON description files for Kinesis streams.
     */
    private String tableDescriptionDir = "etc/kinesis/";

    /**
     * An S3 URL with JSON description files for Kinesis streams.
     * <p>
     * This is empty by default and will override tableDescriptionDir when set.
     */
    private String tableDescriptionsS3 = "";

    /**
     * Whether internal columns are shown in table metadata or not. Default is no.
     */
    private boolean hideInternalColumns = true;

    /**
     * Region to be used to read stream from.
     */
    private String awsRegion = "us-east-1";

    /**
     * Defines maximum number of records to return in one call
     */
    private int batchSize = 10000;

    /**
     * Defines the maximum number of batches read from Kinesis in one query.
     */
    private int maxBatches = 600;

    /**
     * Defines number of attempts to fetch records from a stream until received non-empty on the first batch.
     */
    private int fetchAttempts = 2;

    /**
     * Defines sleep time (in milliseconds) for the thread which is trying to fetch the records from kinesis streams
     */
    private Duration sleepTime = new Duration(1000, TimeUnit.MILLISECONDS);

    /**
     * Use an initial shard iterator type of AT_TIMESTAMP starting iterOffsetSeconds before the current time.
     * <p>
     * When false, an initial shard iterator type of TRIM_HORIZON will be used.
     */
    private boolean iterFromTimestamp = true;

    /**
     * When iterFromTimestamp is true, the shard iterator will start at iterOffsetSeconds before
     * the current time.
     */
    private long iterOffsetSeconds = 86400;

    private String accessKey;

    private String secretKey;

    private boolean logKinesisBatches = true;

    private boolean checkpointEnabled;

    private long dynamoReadCapacity = 50L;

    private long dyanamoWriteCapacity = 10L;

    private Duration checkpointIntervalMS = new Duration(60000, TimeUnit.MILLISECONDS);

    private String logicalProcessName = "process1";

    private int iterationNumber;

    @NotNull
    public String getTableDescriptionDir()
    {
        return tableDescriptionDir;
    }

    @Config("kinesis.table-description-dir")
    public KinesisConnectorConfig setTableDescriptionDir(String tableDescriptionDir)
    {
        this.tableDescriptionDir = tableDescriptionDir;
        return this;
    }

    @NotNull
    public String getTableDescriptionsS3()
    {
        return tableDescriptionsS3;
    }

    @Config("kinesis.table-descriptions-s3")
    public KinesisConnectorConfig setTableDescriptionsS3(String tableDescriptionsS3)
    {
        this.tableDescriptionsS3 = tableDescriptionsS3;
        return this;
    }

    public boolean isHideInternalColumns()
    {
        return hideInternalColumns;
    }

    @Config("kinesis.hide-internal-columns")
    public KinesisConnectorConfig setHideInternalColumns(boolean hideInternalColumns)
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
    public KinesisConnectorConfig setDefaultSchema(String defaultSchema)
    {
        this.defaultSchema = defaultSchema;
        return this;
    }

    public String getAccessKey()
    {
        return this.accessKey;
    }

    @Config("kinesis.access-key")
    public KinesisConnectorConfig setAccessKey(String accessKey)
    {
        this.accessKey = accessKey;
        return this;
    }

    public String getSecretKey()
    {
        return this.secretKey;
    }

    @Config("kinesis.secret-key")
    public KinesisConnectorConfig setSecretKey(String secretKey)
    {
        this.secretKey = secretKey;
        return this;
    }

    public String getAwsRegion()
    {
        return awsRegion;
    }

    @Config("kinesis.aws-region")
    public KinesisConnectorConfig setAwsRegion(String awsRegion)
    {
        this.awsRegion = awsRegion;
        return this;
    }

    public int getBatchSize()
    {
        return this.batchSize;
    }

    @Config("kinesis.batch-size")
    public KinesisConnectorConfig setBatchSize(int batchSize)
    {
        this.batchSize = batchSize;
        return this;
    }

    public int getMaxBatches()
    {
        return this.maxBatches;
    }

    @Config("kinesis.max-batches")
    public KinesisConnectorConfig setMaxBatches(int maxBatches)
    {
        this.maxBatches = maxBatches;
        return this;
    }

    public int getFetchAttempts()
    {
        return this.fetchAttempts;
    }

    @Config("kinesis.fetch-attempts")
    public KinesisConnectorConfig setFetchAttempts(int fetchAttempts)
    {
        this.fetchAttempts = fetchAttempts;
        return this;
    }

    public Duration getSleepTime()
    {
        return this.sleepTime;
    }

    @Config("kinesis.sleep-time")
    public KinesisConnectorConfig setSleepTime(Duration sleepTime)
    {
        this.sleepTime = sleepTime;
        return this;
    }

    public boolean isLogBatches()
    {
        return logKinesisBatches;
    }

    @Config("kinesis.log-batches")
    public KinesisConnectorConfig setLogBatches(boolean logBatches)
    {
        this.logKinesisBatches = logBatches;
        return this;
    }

    public boolean isIterFromTimestamp()
    {
        return iterFromTimestamp;
    }

    @Config("kinesis.iter-from-timestamp")
    public KinesisConnectorConfig setIterFromTimestamp(boolean iterFromTimestamp)
    {
        this.iterFromTimestamp = iterFromTimestamp;
        return this;
    }

    public long getIterOffsetSeconds()
    {
        return iterOffsetSeconds;
    }

    @Config("kinesis.iter-offset-seconds")
    public KinesisConnectorConfig setIterOffsetSeconds(long iterOffsetSeconds)
    {
        this.iterOffsetSeconds = iterOffsetSeconds;
        return this;
    }

    public boolean isCheckpointEnabled()
    {
        return checkpointEnabled;
    }

    @Config("kinesis.checkpoint-enabled")
    public KinesisConnectorConfig setCheckpointEnabled(boolean checkpointEnabled)
    {
        this.checkpointEnabled = checkpointEnabled;
        return this;
    }

    public long getDynamoReadCapacity()
    {
        return dynamoReadCapacity;
    }

    @Config("kinesis.dynamo-read-capacity")
    public KinesisConnectorConfig setDynamoReadCapacity(long dynamoReadCapacity)
    {
        this.dynamoReadCapacity = dynamoReadCapacity;
        return this;
    }

    public long getDynamoWriteCapacity()
    {
        return dyanamoWriteCapacity;
    }

    @Config("kinesis.dynamo-write-capacity")
    public KinesisConnectorConfig setDynamoWriteCapacity(long dynamoWriteCapacity)
    {
        this.dyanamoWriteCapacity = dynamoWriteCapacity;
        return this;
    }

    public Duration getCheckpointIntervalMS()
    {
        return checkpointIntervalMS;
    }

    @Config("kinesis.checkpoint-interval-ms")
    public KinesisConnectorConfig setCheckpointIntervalMS(Duration checkpointIntervalMS)
    {
        this.checkpointIntervalMS = checkpointIntervalMS;
        return this;
    }

    public String getLogicalProcessName()
    {
        return logicalProcessName;
    }

    @Config("kinesis.checkpoint-logical-name")
    public KinesisConnectorConfig setLogicalProcessName(String logicalPrcessName)
    {
        this.logicalProcessName = logicalPrcessName;
        return this;
    }

    public int getIterationNumber()
    {
        return iterationNumber;
    }

    @Config("kinesis.iteration-number")
    public KinesisConnectorConfig setIterationNumber(int iterationNumber)
    {
        this.iterationNumber = iterationNumber;
        return this;
    }
}
