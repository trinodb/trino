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

package io.trino.plugin.eventlistener.kafka.metrics;

import io.airlift.stats.CounterStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class KafkaEventListenerJmxStats
{
    private final CounterStat initializationFailure = new CounterStat();

    // Query Completed Event stats
    private final CounterStat completedEventReceived = new CounterStat();
    private final CounterStat completedEventBuildFailure = new CounterStat();
    private final CounterStat completedEventSuccessfulDispatch = new CounterStat();
    private final CounterStat completedEventSendFailureTimeout = new CounterStat();
    private final CounterStat completedEventSendFailureTooLarge = new CounterStat();
    private final CounterStat completedEventSendFailureInvalidRecord = new CounterStat();
    private final CounterStat completedEventSendFailureOther = new CounterStat();

    // Query Created Event stats
    private final CounterStat createdEventReceived = new CounterStat();
    private final CounterStat createdEventBuildFailure = new CounterStat();
    private final CounterStat createdEventSuccessfulDispatch = new CounterStat();
    private final CounterStat createdEventSendFailureTimeout = new CounterStat();
    private final CounterStat createdEventSendFailureTooLarge = new CounterStat();
    private final CounterStat createdEventSendFailureInvalidRecord = new CounterStat();
    private final CounterStat createdEventSendFailureOther = new CounterStat();

    // Split Completed Event stats
    private final CounterStat splitCompletedEventReceived = new CounterStat();
    private final CounterStat splitCompletedEventBuildFailure = new CounterStat();
    private final CounterStat splitCompletedEventSuccessfulDispatch = new CounterStat();
    private final CounterStat splitCompletedEventSendFailureTimeout = new CounterStat();
    private final CounterStat splitCompletedEventSendFailureTooLarge = new CounterStat();
    private final CounterStat splitCompletedEventSendFailureInvalidRecord = new CounterStat();
    private final CounterStat splitCompletedEventSendFailureOther = new CounterStat();

    @Managed
    @Nested
    public CounterStat getInitializationFailure()
    {
        return initializationFailure;
    }

    @Managed
    @Nested
    public CounterStat getCompletedEventReceived()
    {
        return completedEventReceived;
    }

    @Managed
    @Nested
    public CounterStat getCompletedEventBuildFailure()
    {
        return completedEventBuildFailure;
    }

    @Managed
    @Nested
    public CounterStat getCompletedEventSendFailureTimeout()
    {
        return completedEventSendFailureTimeout;
    }

    @Managed
    @Nested
    public CounterStat getCompletedEventSendFailureTooLarge()
    {
        return completedEventSendFailureTooLarge;
    }

    @Managed
    @Nested
    public CounterStat getCompletedEventSendFailureInvalidRecord()
    {
        return completedEventSendFailureInvalidRecord;
    }

    @Managed
    @Nested
    public CounterStat getCompletedEventSendFailureOther()
    {
        return completedEventSendFailureOther;
    }

    @Managed
    @Nested
    public CounterStat getCreatedEventReceived()
    {
        return createdEventReceived;
    }

    @Managed
    @Nested
    public CounterStat getCreatedEventBuildFailure()
    {
        return createdEventBuildFailure;
    }

    @Managed
    @Nested
    public CounterStat getCreatedEventSendFailureTimeout()
    {
        return createdEventSendFailureTimeout;
    }

    @Managed
    @Nested
    public CounterStat getCreatedEventSendFailureTooLarge()
    {
        return createdEventSendFailureTooLarge;
    }

    @Managed
    @Nested
    public CounterStat getCreatedEventSendFailureInvalidRecord()
    {
        return createdEventSendFailureInvalidRecord;
    }

    @Managed
    @Nested
    public CounterStat getCreatedEventSendFailureOther()
    {
        return createdEventSendFailureOther;
    }

    @Managed
    @Nested
    public CounterStat getCreatedEventSuccessfulDispatch()
    {
        return createdEventSuccessfulDispatch;
    }

    @Managed
    @Nested
    public CounterStat getCompletedEventSuccessfulDispatch()
    {
        return completedEventSuccessfulDispatch;
    }

    @Managed
    @Nested
    public CounterStat getSplitCompletedEventReceived()
    {
        return splitCompletedEventReceived;
    }

    @Managed
    @Nested
    public CounterStat getSplitCompletedEventBuildFailure()
    {
        return splitCompletedEventBuildFailure;
    }

    @Managed
    @Nested
    public CounterStat getSplitCompletedEventSendFailureTimeout()
    {
        return splitCompletedEventSendFailureTimeout;
    }

    @Managed
    @Nested
    public CounterStat getSplitCompletedEventSendFailureTooLarge()
    {
        return splitCompletedEventSendFailureTooLarge;
    }

    @Managed
    @Nested
    public CounterStat getSplitCompletedEventSendFailureInvalidRecord()
    {
        return splitCompletedEventSendFailureInvalidRecord;
    }

    @Managed
    @Nested
    public CounterStat getSplitCompletedEventSendFailureOther()
    {
        return splitCompletedEventSendFailureOther;
    }

    public void kafkaPublisherFailedToInitialize()
    {
        initializationFailure.update(1);
    }

    public void completedEventReceived()
    {
        completedEventReceived.update(1);
    }

    public void completedEventBuildFailure()
    {
        completedEventBuildFailure.update(1);
    }

    public void completedEventSendFailureTimeout()
    {
        completedEventSendFailureTimeout.update(1);
    }

    public void completedEventSendFailureTooLarge()
    {
        completedEventSendFailureTooLarge.update(1);
    }

    public void completedEventSendFailureInvalidRecord()
    {
        completedEventSendFailureInvalidRecord.update(1);
    }

    public void completedEventSendFailureOther()
    {
        completedEventSendFailureOther.update(1);
    }

    public void completedEventSuccessfulDispatch()
    {
        completedEventSuccessfulDispatch.update(1);
    }

    public void createdEventReceived()
    {
        createdEventReceived.update(1);
    }

    public void createdEventBuildFailure()
    {
        createdEventBuildFailure.update(1);
    }

    public void createdEventSendFailureTimeout()
    {
        createdEventSendFailureTimeout.update(1);
    }

    public void createdEventSendFailureTooLarge()
    {
        createdEventSendFailureTooLarge.update(1);
    }

    public void createdEventSendFailureInvalidRecord()
    {
        createdEventSendFailureInvalidRecord.update(1);
    }

    public void createdEventSendFailureOther()
    {
        createdEventSendFailureOther.update(1);
    }

    public void createdEventSuccessfulDispatch()
    {
        createdEventSuccessfulDispatch.update(1);
    }

    public void splitCompletedEventReceived()
    {
        splitCompletedEventReceived.update(1);
    }

    public void splitCompletedEventBuildFailure()
    {
        splitCompletedEventBuildFailure.update(1);
    }

    public void splitCompletedEventSendFailureTimeout()
    {
        splitCompletedEventSendFailureTimeout.update(1);
    }

    public void splitCompletedEventSendFailureTooLarge()
    {
        splitCompletedEventSendFailureTooLarge.update(1);
    }

    public void splitCompletedEventSendFailureInvalidRecord()
    {
        splitCompletedEventSendFailureInvalidRecord.update(1);
    }

    public void splitCompletedEventSendFailureOther()
    {
        splitCompletedEventSendFailureOther.update(1);
    }

    public void splitCompletedEventSuccessfulDispatch()
    {
        splitCompletedEventSuccessfulDispatch.update(1);
    }
}
