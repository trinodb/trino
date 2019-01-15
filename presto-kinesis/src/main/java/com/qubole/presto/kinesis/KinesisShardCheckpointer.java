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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLeaseManager;
import com.google.common.base.Throwables;
import io.airlift.log.Logger;

public class KinesisShardCheckpointer
{
    private static final Logger log = Logger.get(KinesisShardCheckpointer.class);
    private KinesisClientLeaseManager kinesisClientLeaseManager;
    private KinesisSplit kinesisSplit;
    private String logicalProcessName;
    private int curIterationNumber;
    private long dynamoReadCapacity;
    private long dynamoWriteCapacity;
    private KinesisClientLease kinesisClientLease;
    private long checkpointIntervalMs;
    private long nextCheckpointTimeMs;

    public KinesisShardCheckpointer(AmazonDynamoDB dynamoDBClient,
            String dynamoDBTable,
            KinesisSplit kinesisSplit,
            String logicalProcessName,
            int curIterationNumber,
            long checkpointIntervalMS,
            long dynamoReadCapacity,
            long dynamoWriteCapacity)
    {
        this(new KinesisClientLeaseManager(dynamoDBTable, dynamoDBClient),
                kinesisSplit,
                logicalProcessName,
                curIterationNumber,
                checkpointIntervalMS,
                dynamoReadCapacity,
                dynamoWriteCapacity);
    }

    public KinesisShardCheckpointer(KinesisClientLeaseManager kinesisClientLeaseManager,
            KinesisSplit kinesisSplit,
            String logicalProcessName,
            int curIterationNumber,
            long checkpointIntervalMS,
            long dynamoReadCapacity,
            long dynamoWriteCapacity)
    {
        this.kinesisClientLeaseManager = kinesisClientLeaseManager;
        this.kinesisSplit = kinesisSplit;
        this.logicalProcessName = logicalProcessName;
        this.curIterationNumber = curIterationNumber;
        this.checkpointIntervalMs = checkpointIntervalMS;
        this.dynamoReadCapacity = dynamoReadCapacity;
        this.dynamoWriteCapacity = dynamoWriteCapacity;

        try {
            this.kinesisClientLeaseManager.createLeaseTableIfNotExists(this.dynamoReadCapacity, this.dynamoWriteCapacity);

            KinesisClientLease oldLease = this.kinesisClientLeaseManager.getLease(createCheckpointKey(curIterationNumber));
            if (oldLease != null) {
                this.kinesisClientLease = oldLease;
            }
            else {
                this.kinesisClientLease = new KinesisClientLease();
                this.kinesisClientLease.setLeaseKey(createCheckpointKey(curIterationNumber));
            }
        }
        catch (DependencyException e) {
            throw Throwables.propagate(e);
        }
        catch (ProvisionedThroughputException e) {
            throw Throwables.propagate(e);
        }
        catch (InvalidStateException e) {
            throw Throwables.propagate(e);
        }

        resetNextCheckpointTime();
    }

    private void resetNextCheckpointTime()
    {
        nextCheckpointTimeMs = System.currentTimeMillis() + checkpointIntervalMs;
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
    public void checkpoint(String lastReadSequenceNo)
    {
        log.info("Trying to checkpoint at " + lastReadSequenceNo);
        try {
            ExtendedSequenceNumber esn = new ExtendedSequenceNumber(lastReadSequenceNo);
            kinesisClientLease.setCheckpoint(esn);
            kinesisClientLeaseManager.createLeaseIfNotExists(kinesisClientLease);
            if (!kinesisClientLeaseManager.updateLease(kinesisClientLease)) {
                log.info("Checkpointing unsucessful");
            }
        }
        catch (DependencyException e) {
            throw Throwables.propagate(e);
        }
        catch (InvalidStateException e) {
            throw Throwables.propagate(e);
        }
        catch (ProvisionedThroughputException e) {
            throw Throwables.propagate(e);
        }

        resetNextCheckpointTime();
    }

    //return checkpoint of previous iteration if found
    public String getLastReadSeqNo()
    {
        String lastReadSeqNo = null;
        KinesisClientLease oldLease = null;
        if (curIterationNumber > 0) {
            try {
                oldLease = kinesisClientLeaseManager.getLease(createCheckpointKey(curIterationNumber - 1));
            }
            catch (DependencyException e) {
                throw Throwables.propagate(e);
            }
            catch (InvalidStateException e) {
                throw Throwables.propagate(e);
            }
            catch (ProvisionedThroughputException e) {
                throw Throwables.propagate(e);
            }
            if (oldLease != null) {
                // ExtendedSequenceNumber type in latest API:
                lastReadSeqNo = oldLease.getCheckpoint().toString();
            }
        }
        if (lastReadSeqNo == null) {
            log.info("Previous checkpoint not found. Starting from beginning of shard");
        }
        else {
            log.info("Resuming from " + lastReadSeqNo);
        }
        return lastReadSeqNo;
    }

    public void checkpointIfTimeUp(String lastReadSeqNo)
    {
        if (System.currentTimeMillis() >= nextCheckpointTimeMs) {
            checkpoint(lastReadSeqNo);
        }
    }
}
