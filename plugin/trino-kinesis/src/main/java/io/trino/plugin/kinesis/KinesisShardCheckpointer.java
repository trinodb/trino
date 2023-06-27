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
package io.trino.plugin.kinesis;

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseRefresher;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseSerializer;
import software.amazon.kinesis.leases.dynamodb.TableCreatorCallback;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

public class KinesisShardCheckpointer
{
    private static final Logger log = Logger.get(KinesisShardCheckpointer.class);
    private final DynamoDBLeaseRefresher dynamoDBLeaseRefresher;
    private final KinesisSplit kinesisSplit;
    private final String logicalProcessName;
    private final int currentIterationNumber;
    private final Lease kinesisClientLease;

    public KinesisShardCheckpointer(
            DynamoDbAsyncClient dynamoDBClient,
            String dynamoDBTable,
            KinesisSplit kinesisSplit,
            String logicalProcessName,
            int currentIterationNumber,
            long dynamoReadCapacity,
            long dynamoWriteCapacity)
    {
        this(new DynamoDBLeaseRefresher(dynamoDBTable,
                        dynamoDBClient,
                        new DynamoDBLeaseSerializer(),
                        false,
                        TableCreatorCallback.NOOP_TABLE_CREATOR_CALLBACK,
                        LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT,
                        BillingMode.PAY_PER_REQUEST,
                        false,
                        ImmutableSet.of()),
                kinesisSplit,
                logicalProcessName,
                currentIterationNumber,
                dynamoReadCapacity,
                dynamoWriteCapacity);
    }

    public KinesisShardCheckpointer(
            DynamoDBLeaseRefresher dynamoDBLeaseRefresher,
            KinesisSplit kinesisSplit,
            String logicalProcessName,
            int currentIterationNumber,
            long dynamoReadCapacity,
            long dynamoWriteCapacity)
    {
        this.dynamoDBLeaseRefresher = dynamoDBLeaseRefresher;
        this.kinesisSplit = kinesisSplit;
        this.logicalProcessName = logicalProcessName;
        this.currentIterationNumber = currentIterationNumber;

        try {
            this.dynamoDBLeaseRefresher.createLeaseTableIfNotExists(dynamoReadCapacity, dynamoWriteCapacity);

            Lease oldLease = this.dynamoDBLeaseRefresher.getLease(createCheckpointKey(currentIterationNumber));
            if (oldLease != null) {
                this.kinesisClientLease = oldLease;
            }
            else {
                this.kinesisClientLease = new Lease();
                this.kinesisClientLease.leaseKey(createCheckpointKey(currentIterationNumber));
            }
        }
        catch (ProvisionedThroughputException | InvalidStateException | DependencyException e) {
            throw new RuntimeException(e);
        }
    }

    private String createCheckpointKey(int iterationNo)
    {
        return "%s_%s_%s_%d".formatted(
                this.logicalProcessName,
                this.kinesisSplit.getStreamName(),
                this.kinesisSplit.getShardId(),
                iterationNo);
    }

    // storing last read sequence no. in dynamodb table
    public void checkpoint(String lastReadSequenceNumber)
    {
        log.info("Trying to checkpoint at %s", lastReadSequenceNumber);
        try {
            ExtendedSequenceNumber esn = new ExtendedSequenceNumber(lastReadSequenceNumber);
            kinesisClientLease.checkpoint(esn);
            dynamoDBLeaseRefresher.createLeaseIfNotExists(kinesisClientLease);
            if (!dynamoDBLeaseRefresher.updateLease(kinesisClientLease)) {
                log.warn("Checkpointing unsuccessful");
            }
        }
        catch (DependencyException | InvalidStateException | ProvisionedThroughputException e) {
            throw new RuntimeException(e);
        }
    }

    //return checkpoint of previous iteration if found
    public String getLastReadSeqNumber()
    {
        String lastReadSeqNumber = null;
        if (currentIterationNumber > 0) {
            Lease oldLease;
            try {
                oldLease = dynamoDBLeaseRefresher.getLease(createCheckpointKey(currentIterationNumber - 1));
            }
            catch (DependencyException | InvalidStateException | ProvisionedThroughputException e) {
                throw new RuntimeException(e);
            }
            if (oldLease != null) {
                // ExtendedSequenceNumber type in latest API:
                lastReadSeqNumber = oldLease.checkpoint().toString();
            }
        }
        if (lastReadSeqNumber == null) {
            log.info("Previous checkpoint not found. Starting from beginning of shard");
        }
        else {
            log.info("Resuming from %s", lastReadSeqNumber);
        }
        return lastReadSeqNumber;
    }
}
