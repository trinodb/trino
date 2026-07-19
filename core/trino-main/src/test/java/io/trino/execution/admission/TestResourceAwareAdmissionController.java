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
package io.trino.execution.admission;

import com.google.common.base.Ticker;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.memory.MemoryInfo;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.memory.MemoryPoolInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestResourceAwareAdmissionController
{
    private static final QueryId QUERY_ID = new QueryId("test_query");

    private final AtomicLong now = new AtomicLong();
    private final Ticker ticker = new Ticker()
    {
        @Override
        public long read()
        {
            return now.get();
        }
    };

    private final AtomicReference<Map<String, Optional<MemoryInfo>>> workers = new AtomicReference<>(Map.of());

    private ScheduledExecutorService executor;

    @BeforeEach
    public void setUp()
    {
        executor = newSingleThreadScheduledExecutor();
    }

    @AfterEach
    public void tearDown()
    {
        executor.shutdownNow();
    }

    // --- pure decision logic ---

    @Test
    public void testProceedsWhenMemoryAndVcpuAvailable()
    {
        ResourceAwareAdmissionController policy = policy(new ResourceAwareAdmissionConfig());
        WaitDecision decision = policy.shouldQueryWait(
                context(DataSize.of(100, MEGABYTE), 4),
                new ClusterCapacity(DataSize.of(200, MEGABYTE).toBytes(), 8));
        assertThat(decision).isInstanceOf(WaitDecision.ProceedNow.class);
    }

    @Test
    public void testWaitsWhenMemoryInsufficient()
    {
        ResourceAwareAdmissionController policy = policy(new ResourceAwareAdmissionConfig());
        QueryAdmissionContext context = context(DataSize.of(100, MEGABYTE), 0);
        WaitDecision decision = policy.shouldQueryWait(context, new ClusterCapacity(DataSize.of(50, MEGABYTE).toBytes(), 8));
        assertThat(decision).isInstanceOf(WaitDecision.Wait.class);
        assertThat(((WaitDecision.Wait) decision).maxWait()).isEqualTo(context.maxWait());
    }

    @Test
    public void testWaitsWhenVcpuInsufficient()
    {
        ResourceAwareAdmissionController policy = policy(new ResourceAwareAdmissionConfig());
        WaitDecision decision = policy.shouldQueryWait(
                context(DataSize.of(0, MEGABYTE), 8),
                new ClusterCapacity(DataSize.of(200, MEGABYTE).toBytes(), 2));
        assertThat(decision).isInstanceOf(WaitDecision.Wait.class);
    }

    @Test
    public void testProceedsAtExactThreshold()
    {
        ResourceAwareAdmissionController policy = policy(new ResourceAwareAdmissionConfig());
        WaitDecision decision = policy.shouldQueryWait(
                context(DataSize.of(100, MEGABYTE), 4),
                new ClusterCapacity(DataSize.of(100, MEGABYTE).toBytes(), 4));
        assertThat(decision).isInstanceOf(WaitDecision.ProceedNow.class);
    }

    // --- rolling-average required memory ---

    @Test
    public void testEffectiveRequiredMemoryColdStartUsesFallback()
    {
        ResourceAwareAdmissionController policy = policy(new ResourceAwareAdmissionConfig());
        DataSize fallback = DataSize.of(42, MEGABYTE);
        assertThat(policy.effectiveRequiredMemory(fallback)).isEqualTo(fallback);
    }

    @Test
    public void testEffectiveRequiredMemoryUsesRollingAverage()
    {
        ResourceAwareAdmissionController policy = policy(new ResourceAwareAdmissionConfig());
        policy.recordPeakMemory(DataSize.of(100, MEGABYTE).toBytes());
        policy.recordPeakMemory(DataSize.of(300, MEGABYTE).toBytes());
        // average 200MB; ignores the fallback once samples exist
        assertThat(policy.effectiveRequiredMemory(DataSize.of(42, MEGABYTE)).toBytes())
                .isEqualTo(DataSize.of(200, MEGABYTE).toBytes());
    }

    @Test
    public void testEffectiveRequiredMemoryAppliesHeadroomMultiplier()
    {
        ResourceAwareAdmissionController policy = policy(new ResourceAwareAdmissionConfig().setRequiredMemoryMultiplier(1.5));
        policy.recordPeakMemory(DataSize.of(200, MEGABYTE).toBytes());
        // 200MB * 1.5 = 300MB
        assertThat(policy.effectiveRequiredMemory(DataSize.of(42, MEGABYTE)).toBytes())
                .isEqualTo(DataSize.of(300, MEGABYTE).toBytes());
    }

    @Test
    public void testHeadroomMultiplierAppliesToColdStartFallback()
    {
        ResourceAwareAdmissionController policy = policy(new ResourceAwareAdmissionConfig().setRequiredMemoryMultiplier(2.0));
        // no samples recorded: fallback is scaled too
        assertThat(policy.effectiveRequiredMemory(DataSize.of(50, MEGABYTE)).toBytes())
                .isEqualTo(DataSize.of(100, MEGABYTE).toBytes());
    }

    // --- gating behavior ---

    @Test
    public void testAdmitsWhenCapacityAvailable()
            throws Exception
    {
        setCapacity(DataSize.of(200, MEGABYTE).toBytes(), 8);
        ResourceAwareAdmissionController policy = enabledPolicy();

        ListenableFuture<Void> future = policy.enqueue(context(DataSize.of(100, MEGABYTE), 0));
        assertThat(future).isNotDone();

        policy.process();
        assertThat(future).isDone();
        future.get();
        assertThat(policy.waitingQueryCount()).isZero();
    }

    @Test
    public void testHoldsUntilCapacityAppears()
            throws Exception
    {
        setCapacity(DataSize.of(50, MEGABYTE).toBytes(), 0);
        ResourceAwareAdmissionController policy = enabledPolicy();

        ListenableFuture<Void> future = policy.enqueue(context(DataSize.of(100, MEGABYTE), 0));

        policy.process();
        assertThat(future).isNotDone();
        assertThat(policy.waitingQueryCount()).isEqualTo(1);

        setCapacity(DataSize.of(150, MEGABYTE).toBytes(), 0);
        policy.process();
        assertThat(future).isDone();
        future.get();
    }

    @Test
    public void testFailsAfterMaxWait()
    {
        setCapacity(0, 0);
        ResourceAwareAdmissionController policy = enabledPolicy();

        ListenableFuture<Void> future = policy.enqueue(context(DataSize.of(100, MEGABYTE), 0, new Duration(1, MINUTES)));

        policy.process();
        assertThat(future).isNotDone();

        now.addAndGet(new Duration(2, MINUTES).roundTo(NANOSECONDS));
        policy.process();

        assertThat(future).isDone();
        assertThatThrownBy(future::get)
                .cause()
                .isInstanceOf(TrinoException.class)
                .satisfies(t -> assertThat(((TrinoException) t).getErrorCode()).isEqualTo(GENERIC_INSUFFICIENT_RESOURCES.toErrorCode()));
    }

    @Test
    public void testReleasesOldestFirst()
            throws Exception
    {
        setCapacity(DataSize.of(150, MEGABYTE).toBytes(), 0);
        ResourceAwareAdmissionController policy = enabledPolicy();

        ListenableFuture<Void> first = policy.enqueue(context(DataSize.of(100, MEGABYTE), 0));
        ListenableFuture<Void> second = policy.enqueue(context(DataSize.of(100, MEGABYTE), 0));

        // one release per cycle, oldest first
        policy.process();
        assertThat(first).isDone();
        assertThat(second).isNotDone();

        policy.process();
        assertThat(second).isDone();
        first.get();
        second.get();
    }

    private ResourceAwareAdmissionController enabledPolicy()
    {
        // Poll interval is set very large so the executor never auto-fires; tests drive process() directly.
        return policy(new ResourceAwareAdmissionConfig()
                .setEnabled(true)
                .setPollInterval(new Duration(1, HOURS)));
    }

    private ResourceAwareAdmissionController policy(ResourceAwareAdmissionConfig config)
    {
        return new ResourceAwareAdmissionController(workers::get, config, ticker, executor);
    }

    private void setCapacity(long freeBytes, int processors)
    {
        // getFreeBytes() == maxBytes - reservedBytes - reservedRevocableBytes
        MemoryPoolInfo pool = new MemoryPoolInfo(freeBytes, 0, 0, Map.of(), Map.of(), Map.of(), Map.of());
        MemoryInfo info = new MemoryInfo(processors, 0.0, pool);
        workers.set(Map.of("worker1", Optional.of(info)));
    }

    private static QueryAdmissionContext context(DataSize requiredMemory, int requiredVcpu)
    {
        return context(requiredMemory, requiredVcpu, new Duration(5, MINUTES));
    }

    private static QueryAdmissionContext context(DataSize requiredMemory, int requiredVcpu, Duration maxWait)
    {
        return new QueryAdmissionContext(QUERY_ID, requiredMemory, requiredVcpu, maxWait);
    }
}
