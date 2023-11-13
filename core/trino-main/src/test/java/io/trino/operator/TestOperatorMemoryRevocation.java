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
package io.trino.operator;

import io.trino.memory.context.LocalMemoryContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestOperatorMemoryRevocation
{
    private ScheduledExecutorService scheduledExecutor;

    @BeforeAll
    public void setUp()
    {
        scheduledExecutor = newSingleThreadScheduledExecutor();
    }

    @AfterAll
    public void tearDown()
    {
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testOperatorMemoryRevocation()
    {
        AtomicInteger counter = new AtomicInteger();
        OperatorContext operatorContext = TestingOperatorContext.create(scheduledExecutor);
        LocalMemoryContext revocableMemoryContext = operatorContext.localRevocableMemoryContext();
        revocableMemoryContext.setBytes(1000);
        operatorContext.setMemoryRevocationRequestListener(counter::incrementAndGet);
        operatorContext.requestMemoryRevoking();
        assertTrue(operatorContext.isMemoryRevokingRequested());
        assertEquals(counter.get(), 1);

        // calling resetMemoryRevokingRequested() should clear the memory revoking requested flag
        operatorContext.resetMemoryRevokingRequested();
        assertFalse(operatorContext.isMemoryRevokingRequested());

        operatorContext.requestMemoryRevoking();
        assertEquals(counter.get(), 2);
        assertTrue(operatorContext.isMemoryRevokingRequested());
    }

    @Test
    public void testRevocationAlreadyRequested()
    {
        AtomicInteger counter = new AtomicInteger();
        OperatorContext operatorContext = TestingOperatorContext.create(scheduledExecutor);
        LocalMemoryContext revocableMemoryContext = operatorContext.localRevocableMemoryContext();
        revocableMemoryContext.setBytes(1000);

        // when memory revocation is already requested setting a listener should immediately execute it
        operatorContext.requestMemoryRevoking();
        operatorContext.setMemoryRevocationRequestListener(counter::incrementAndGet);
        assertTrue(operatorContext.isMemoryRevokingRequested());
        assertEquals(counter.get(), 1);
    }

    @Test
    public void testSingleListenerEnforcement()
    {
        OperatorContext operatorContext = TestingOperatorContext.create(scheduledExecutor);
        operatorContext.setMemoryRevocationRequestListener(() -> {});
        assertThatThrownBy(() -> operatorContext.setMemoryRevocationRequestListener(() -> {}))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("listener already set");
    }
}
