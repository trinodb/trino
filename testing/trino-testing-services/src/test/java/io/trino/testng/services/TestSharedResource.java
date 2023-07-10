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
package io.trino.testng.services;

import io.trino.testing.SharedResource;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSharedResource
{
    @Test
    public void testSharedResourceReusing()
            throws Exception
    {
        MockFactory factory = new MockFactory();
        SharedResource<AutoCloseable> sharedResource = new SharedResource<>(factory);

        try (SharedResource.Lease<AutoCloseable> lease = sharedResource.getInstanceLease();
                SharedResource.Lease<AutoCloseable> lease1 = sharedResource.getInstanceLease()) {
            assertThat(lease.get()).isNotNull();
            assertThat(lease1.get()).isNotNull();
        }
        assertThat(factory.getCreationCount()).isOne();
        assertThat(factory.getDestructionCount()).isOne();
    }

    @Test
    public void testSharedResourceNotReusing()
            throws Exception
    {
        MockFactory factory = new MockFactory();
        SharedResource<AutoCloseable> sharedResource = new SharedResource<>(factory);

        for (int i = 0; i < 10; i++) {
            try (SharedResource.Lease<AutoCloseable> lease = sharedResource.getInstanceLease()) {
                assertThat(lease.get()).isNotNull();
            }
        }
        assertThat(factory.getCreationCount()).isEqualTo(10);
        assertThat(factory.getDestructionCount()).isEqualTo(10);
    }

    private static class MockFactory
            implements Callable<AutoCloseable>
    {
        private final AtomicLong creationCounter = new AtomicLong();
        private final AtomicLong destructionCounter = new AtomicLong();

        public long getCreationCount()
        {
            return creationCounter.get();
        }

        public long getDestructionCount()
        {
            return destructionCounter.get();
        }

        @Override
        public AutoCloseable call()
                throws Exception
        {
            creationCounter.incrementAndGet();
            return destructionCounter::incrementAndGet;
        }
    }
}
