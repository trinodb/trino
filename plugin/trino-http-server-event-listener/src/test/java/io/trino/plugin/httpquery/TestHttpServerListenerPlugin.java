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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;
import io.trino.testing.TestingEventListenerContext;
import org.junit.jupiter.api.Test;

import static com.google.common.collect.Iterables.getOnlyElement;

final class TestHttpServerListenerPlugin
{
    @Test
    void testCreateEventListener()
    {
        HttpServerEventListenerPlugin plugin = new HttpServerEventListenerPlugin();
        EventListenerFactory factory = getOnlyElement(plugin.getEventListenerFactories());
        EventListener eventListener = factory.create(ImmutableMap.of("http-server.http.port", "0"), new TestingEventListenerContext());
        eventListener.shutdown();
    }
}
