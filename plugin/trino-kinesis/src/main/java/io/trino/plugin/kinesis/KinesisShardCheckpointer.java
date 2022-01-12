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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLeaseManager;
import io.airlift.log.Logger;

public class KinesisShardCheckpointer
{
    private static final Logger log = Logger.get(KinesisShardCheckpointer.class);
    private KinesisClientLeaseManager leaseManager;
    private KinesisSplit kinesisSplit;
    private String logicalProcessName;
    private int currentIterationNumber;
    private KinesisClientLease kinesisClientLease;

    public KinesisShardCheckpointer(
            AmazonDynamoDB dynamoDBClient,
            String dynamoDBTable,
            KinesisSplit kinesisSplit,
            String logicalProcessName,
            int currentIterationNumber,
            long checkpointIntervalMS,
            long dynamoReadCapacity,
            long dynamoWriteCapacity)
    {
        this(new KinesisClientLeaseManager(dynamoDBTable, dynamoDBClient),
                kinesisSplit,
                logicalProcessName,
                currentIterationNumber,
                checkpointIntervalMS,
                dynamoReadCapacity,
                dynamoWriteCapacity);
    }

    public KinesisShardCheckpointer(
            KinesisClientLeaseManager leaseManager,
            KinesisSplit kinesisSplit,
            String logicalProcessName,
            int currentIterationNumber,
            long checkpointIntervalMS,
            long dynamoReadCapacity,
            long dynamoWriteCapacity)
    {
        this.leaseManager = leaseManager;
        this.kinesisSplit = kinesisSplit;
        this.logicalProcessName = logicalProcessName;
        this.currentIterationNumber = currentIterationNumber;

        try {
            this.leaseManager.createLeaseTableIfNotExists(dynamoReadCapacity, dynamoWriteCapacity);

            KinesisClientLease oldLease = this.leaseManager.getLease(createCheckpointKey(currentIterationNumber));
            if (oldLease != null) {
                this.kinesisClientLease = oldLease;
            }
            else {
                this.kinesisClientLease = new KinesisClientLease();
                this.kinesisClientLease.setLeaseKey(createCheckpointKey(currentIterationNumber));
            }
        }
        catch (ProvisionedThroughputException | InvalidStateException | DependencyException e) {
            throw new RuntimeException(e);
        }
    }

    private String createCheckpointKey(int iterationNo)
    {
        return new StringBuilder(this.logicalProcessName)
                .append("_")
                .append(this.kinesisSplit.getStreamName())
                .append("_")
                .append(this.kinesisSplit.getShardId())
                .append("_")
                .append(String.valueOf(iterationNo))
                .toString();
    }

    // storing last read sequence no. in dynamodb table
    public void checkpoint(String lastReadSequenceNumber)
    {
        log.info("Trying to checkpoint at %s", lastReadSequenceNumber);
        try {
            ExtendedSequenceNumber esn = new ExtendedSequenceNumber(lastReadSequenceNumber);
            kinesisClientLease.setCheckpoint(esn);
            leaseManager.createLeaseIfNotExists(kinesisClientLease);
            if (!leaseManager.updateLease(kinesisClientLease)) {
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
        KinesisClientLease oldLease = null;
        if (currentIterationNumber > 0) {
            try {
                oldLease = leaseManager.getLease(createCheckpointKey(currentIterationNumber - 1));
            }
            catch (DependencyException | InvalidStateException | ProvisionedThroughputException e) {
                throw new RuntimeException(e);
            }
            if (oldLease != null) {
                // ExtendedSequenceNumber type in latest API:
                lastReadSeqNumber = oldLease.getCheckpoint().toString();
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
