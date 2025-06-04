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
package io.trino.plugin.httpquery;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

public class HttpServerEventListenerConfig
{
    private int eventBufferSize = 50;
    private Duration eventTTL = new Duration(10, TimeUnit.MINUTES);

    @ConfigDescription("Event buffer size")
    @Config("http-server-event-listener.event-buffer-size")
    public HttpServerEventListenerConfig setEventBufferSize(int eventBufferSize)
    {
        this.eventBufferSize = eventBufferSize;
        return this;
    }

    public int getEventBufferSize()
    {
        return eventBufferSize;
    }

    @ConfigDescription("Event TTL")
    @Config("http-server-event-listener.event-ttl")
    public HttpServerEventListenerConfig setEventTTL(Duration eventTTL)
    {
        this.eventTTL = eventTTL;
        return this;
    }

    public Duration getEventTTL()
    {
        return eventTTL;
    }
}
