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
package io.trino.event;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.eventlistener.EventListenerManager;
import io.trino.execution.TaskId;
import io.trino.operator.DriverStats;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.eventlistener.SplitCompletedEvent;
import io.trino.spi.eventlistener.SplitFailureInfo;
import io.trino.spi.eventlistener.SplitStatistics;
import jakarta.annotation.Nullable;

import java.time.Duration;
import java.util.Optional;

import static java.time.Duration.ofMillis;
import static java.util.Objects.requireNonNull;

public class SplitMonitor
{
    private static final Logger log = Logger.get(SplitMonitor.class);

    private final ObjectMapper objectMapper;
    private final EventListenerManager eventListenerManager;

    @Inject
    public SplitMonitor(EventListenerManager eventListenerManager, ObjectMapper objectMapper)
    {
        this.eventListenerManager = requireNonNull(eventListenerManager, "eventListenerManager is null");
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
    }

    public void splitCompletedEvent(TaskId taskId, DriverStats driverStats, Optional<CatalogName> catalogName)
    {
        splitCompletedEvent(taskId, driverStats, catalogName, null, null);
    }

    public void splitFailedEvent(TaskId taskId, DriverStats driverStats, Optional<CatalogName> catalogName, Throwable cause)
    {
        splitCompletedEvent(taskId, driverStats, catalogName, cause.getClass().getName(), cause.getMessage());
    }

    private void splitCompletedEvent(TaskId taskId, DriverStats driverStats, Optional<CatalogName> catalogName, @Nullable String failureType, @Nullable String failureMessage)
    {
        Duration queuedTime = ofMillis(driverStats.getQueuedTime().toMillis());
        Optional<Duration> queuedTimeIfSplitRan = Optional.empty();
        if (driverStats.getStartTime() != null) {
            queuedTimeIfSplitRan = Optional.of(queuedTime);
        }

        Duration elapsedTime = ofMillis(driverStats.getElapsedTime().toMillis());
        Optional<Duration> elapsedTimeIfSplitRan = Optional.empty();
        if (driverStats.getEndTime() != null) {
            elapsedTimeIfSplitRan = Optional.of(elapsedTime);
        }

        Optional<SplitFailureInfo> splitFailureMetadata = Optional.empty();
        if (failureType != null) {
            splitFailureMetadata = Optional.of(new SplitFailureInfo(failureType, failureMessage != null ? failureMessage : ""));
        }

        try {
            eventListenerManager.splitCompleted(
                    new SplitCompletedEvent(
                            taskId.getQueryId().toString(),
                            taskId.getStageId().toString(),
                            taskId.toString(),
                            catalogName.map(CatalogName::toString),
                            driverStats.getCreateTime(),
                            Optional.ofNullable(driverStats.getStartTime()),
                            Optional.ofNullable(driverStats.getEndTime()),
                            new SplitStatistics(
                                    ofMillis(driverStats.getTotalCpuTime().toMillis()),
                                    elapsedTime,
                                    queuedTime,
                                    ofMillis(driverStats.getRawInputReadTime().toMillis()),
                                    driverStats.getRawInputPositions(),
                                    driverStats.getRawInputDataSize().toBytes(),
                                    queuedTimeIfSplitRan,
                                    elapsedTimeIfSplitRan),
                            splitFailureMetadata,
                            objectMapper.writeValueAsString(driverStats)));
        }
        catch (JsonProcessingException e) {
            log.error(e, "Error processing split completion event for task %s", taskId);
        }
    }
}
