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
package io.trino.plugin.eventlistener.querylog;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestQueryLogEventListenerPlugin
{
    @Test
    public void testPluginInitialization()
    {
        QueryLogEventListenerPlugin plugin = new QueryLogEventListenerPlugin();
        assertThat(plugin).isNotNull();
    }

    @Test
    public void testGetEventListenerFactories()
    {
        QueryLogEventListenerPlugin plugin = new QueryLogEventListenerPlugin();
        Iterable<io.trino.spi.eventlistener.EventListenerFactory> factories = plugin.getEventListenerFactories();

        assertThat(factories).isNotNull();
        assertThat(factories.iterator().hasNext()).isTrue();

        io.trino.spi.eventlistener.EventListenerFactory factory = factories.iterator().next();
        assertThat(factory).isNotNull();
        assertThat(factory).isInstanceOf(QueryLogEventListenerFactory.class);
    }

    @Test
    public void testGetFactoryName()
    {
        QueryLogEventListenerPlugin plugin = new QueryLogEventListenerPlugin();
        Iterable<io.trino.spi.eventlistener.EventListenerFactory> factories = plugin.getEventListenerFactories();
        io.trino.spi.eventlistener.EventListenerFactory factory = factories.iterator().next();

        assertThat(factory.getName()).isEqualTo("querylog");
    }
}
