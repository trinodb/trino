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
package io.trino.plugin.pulsar;

import io.airlift.stats.CounterStat;
import io.airlift.stats.TimeStat;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

/**
 * This class helps to track metrics related to the connector.
 */
public class PulsarConnectorMetricsTracker
{
    // stats loggers
    private final TimeStat messageQueueDequeueWaitTime = new TimeStat();
    private final CounterStat bytesRead = new CounterStat();
    private final TimeStat entryDeserializeTime = new TimeStat();
    private final TimeStat messageQueueEnqueueWaitTime = new TimeStat();
    private final CounterStat numMessagesDeserialized = new CounterStat();
    private final CounterStat numMessagesDeserializedPerEntry = new CounterStat();
    private final CounterStat readAttempts = new CounterStat();
    private final CounterStat readSuccess = new CounterStat();
    private final CounterStat readFailure = new CounterStat();
    private final TimeStat readLatencyPerBatch = new TimeStat();
    private final CounterStat numEntriesPerBatch = new CounterStat();
    private final TimeStat recordDeserializeTime = new TimeStat();
    private final CounterStat numRecordDeserialized = new CounterStat();
    private final TimeStat totalExecutionTime = new TimeStat();

    public PulsarConnectorMetricsTracker()
    { }

    @Managed
    @Nested
    public TimeStat getMessageQueueDequeueWaitTime()
    {
        return messageQueueDequeueWaitTime;
    }

    @Managed
    @Nested
    public CounterStat getBytesRead()
    {
        return bytesRead;
    }

    @Managed
    @Nested
    public TimeStat getEntryDeserializeTime()
    {
        return entryDeserializeTime;
    }

    @Managed
    @Nested
    public TimeStat getMessageQueueEnqueueWaitTime()
    {
        return messageQueueEnqueueWaitTime;
    }

    @Managed
    @Nested
    public CounterStat getNumMessagesDeserialized()
    {
        return numMessagesDeserialized;
    }

    @Managed
    @Nested
    public CounterStat getNumMessagesDeserializedPerEntry()
    {
        return numMessagesDeserializedPerEntry;
    }

    @Managed
    @Nested
    public CounterStat getReadAttempts()
    {
        return readAttempts;
    }

    @Managed
    @Nested
    public CounterStat getReadSuccess()
    {
        return readSuccess;
    }

    @Managed
    @Nested
    public CounterStat getReadFailure()
    {
        return readFailure;
    }

    @Managed
    @Nested
    public TimeStat getReadLatencyPerBatch()
    {
        return readLatencyPerBatch;
    }

    @Managed
    @Nested
    public CounterStat getNumEntriesPerBatch()
    {
        return numEntriesPerBatch;
    }

    @Managed
    @Nested
    public TimeStat getRecordDeserializeTime()
    {
        return recordDeserializeTime;
    }

    @Managed
    @Nested
    public CounterStat getNumRecordDeserialized()
    {
        return numRecordDeserialized;
    }

    @Managed
    @Nested
    public TimeStat getTotalExecutionTime()
    {
        return totalExecutionTime;
    }

    public void recordByteRead(long bytes)
    {
        bytesRead.update(bytes);
    }

    public void recordEntryDeserializeTime(Duration duration)
    {
        entryDeserializeTime.add(duration);
    }

    public void recordMessageQueueEnqueueWaitTime(Duration duration)
    {
        messageQueueEnqueueWaitTime.add(duration);
    }

    public void recordMessageQueueDequeueWaitTime()
    {
        messageQueueDequeueWaitTime.add(Duration.succinctNanos(1));
    }

    public void incrNumberOfMessagesDeserialized()
    {
        numMessagesDeserialized.update(1);
    }

    public void recordNumberOfMessageDeserializedPerEntry(long count)
    {
        numMessagesDeserializedPerEntry.update(count);
    }

    public void recordReadSuccess()
    {
        readAttempts.update(1);
        readSuccess.update(1);
    }

    public void recordReadFailure()
    {
        readAttempts.update(1);
        readFailure.update(1);
    }

    public void recordReadLatency(Duration latency)
    {
        readLatencyPerBatch.add(latency);
    }

    public void incrNumberOfEntriesPerBatch(long count)
    {
        numEntriesPerBatch.update(count);
    }

    public void recordRecordDeserializeStart(Duration duration)
    {
        recordDeserializeTime.add(duration);
    }

    public void incrNumberOfRecordDeserialized()
    {
        numRecordDeserialized.update(1);
    }

    public void recordTotalExecutionTime(Duration duration)
    {
        totalExecutionTime.add(duration);
    }
}
