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
package io.presto.eventlog.listener;

import io.presto.eventlog.EventLogProcessor;
import io.prestosql.spi.eventlistener.EventListener;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class EventLogListener
        implements EventListener
{
    private final EventLogProcessor eventLogProcessor;

    @Inject
    public EventLogListener(EventLogProcessor eventLogProcessor)
    {
        this.eventLogProcessor = requireNonNull(eventLogProcessor, "eventLogListener is null");
    }

    @Override
    public void queryCompleted(String queryInfoJson, String queryId)
    {
        if (eventLogProcessor.isEnableEventLog()) {
            eventLogProcessor.writeEventLog(queryInfoJson, queryId);
        }
    }
}
