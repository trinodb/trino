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
package io.trino.testing;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import static com.google.common.base.Preconditions.checkArgument;

public class TestingEventCollectorConfig
{
    private int preloadedWarnings;
    private boolean addWarnings;

    private int preloadedEvents;
    private boolean addEvents;

    @Config("testing-event-collector.preloaded-warnings")
    @ConfigDescription("Preloads event collector with test warnings")
    public TestingEventCollectorConfig setPreloadedWarnings(int preloadedWarnings)
    {
        checkArgument(preloadedWarnings >= 0, "preloadedWarnings must be >= 0");
        this.preloadedWarnings = preloadedWarnings;
        return this;
    }

    public int getPreloadedWarnings()
    {
        return preloadedWarnings;
    }

    @Config("testing-event-collector.add-warnings")
    @ConfigDescription("Adds a warning each time getWarnings is called")
    public TestingEventCollectorConfig setAddWarnings(boolean addWarnings)
    {
        this.addWarnings = addWarnings;
        return this;
    }

    public boolean getAddWarnings()
    {
        return addWarnings;
    }

    @Config("testing-event-collector.preloaded-events")
    @ConfigDescription("Preloads event collector with test events")
    public TestingEventCollectorConfig setPreloadedEvents(int preloadedEvents)
    {
        checkArgument(preloadedEvents >= 0, "preloadedEvents must be >= 0");
        this.preloadedEvents = preloadedEvents;
        return this;
    }

    public int getPreloadedEvents()
    {
        return preloadedEvents;
    }

    @Config("testing-event-collector.add-events")
    @ConfigDescription("Adds an event each time getEvents is called")
    public TestingEventCollectorConfig setAddEvents(boolean addEvents)
    {
        this.addEvents = addEvents;
        return this;
    }

    public boolean getAddEvents()
    {
        return addEvents;
    }
}
