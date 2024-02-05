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
package io.trino.eventlistener;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Key;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.spi.eventlistener.EventListener;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

public class TestConnectorEventListener
{
    @Test
    public void testConnectorWithoutEventListener()
    {
        QueryRunner queryRunner = new StandaloneQueryRunner(testSessionBuilder().build());

        queryRunner.getCoordinator().getInstance(Key.get(EventListenerManager.class)).loadEventListeners();

        assertThatCode(() -> queryRunner.execute("SELECT 1"))
                .doesNotThrowAnyException();
    }

    @Test
    public void testConnectorWithEventListener()
    {
        MockEventListenerFactory listenerFactory = new MockEventListenerFactory();
        QueryRunner queryRunner = new StandaloneQueryRunner(testSessionBuilder().build());
        queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withEventListener(listenerFactory)
                .build()));
        queryRunner.createCatalog("event_listening", "mock", ImmutableMap.of());

        queryRunner.getCoordinator().getInstance(Key.get(EventListenerManager.class)).loadEventListeners();

        assertThat(listenerFactory.getEventListenerInvocationCounter).hasValue(1);
    }

    private static class MockEventListenerFactory
            implements Supplier<EventListener>
    {
        private final AtomicLong getEventListenerInvocationCounter = new AtomicLong(0);

        @Override
        public EventListener get()
        {
            getEventListenerInvocationCounter.incrementAndGet();
            return new EventListener() {};
        }
    }
}
