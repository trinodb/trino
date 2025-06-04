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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.tracing.Tracing;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.FeaturesConfig.DataIntegrityVerification;
import io.trino.exchange.DirectExchangeInput;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.metadata.Split;
import io.trino.operator.ExchangeOperator.ExchangeOperatorFactory;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.split.RemoteSplit;
import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.cache.SafeCaches.buildNonEvictableCacheWithWeakInvalidateAll;
import static io.trino.execution.buffer.CompressionCodec.LZ4;
import static io.trino.execution.buffer.TestingPagesSerdes.createTestingPagesSerdeFactory;
import static io.trino.operator.ExchangeOperator.REMOTE_CATALOG_HANDLE;
import static io.trino.operator.PageAssertions.assertPageEquals;
import static io.trino.operator.TestingTaskBuffer.PAGE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestExchangeOperator
{
    private static final List<Type> TYPES = ImmutableList.of(VARCHAR);
    private static final PagesSerdeFactory SERDE_FACTORY = createTestingPagesSerdeFactory(LZ4);

    private static final TaskId TASK_1_ID = new TaskId(new StageId("query", 0), 0, 0);
    private static final TaskId TASK_2_ID = new TaskId(new StageId("query", 0), 1, 0);
    private static final TaskId TASK_3_ID = new TaskId(new StageId("query", 0), 2, 0);

    private final LoadingCache<TaskId, TestingTaskBuffer> taskBuffers = buildNonEvictableCacheWithWeakInvalidateAll(
            CacheBuilder.newBuilder(), CacheLoader.from(TestingTaskBuffer::new));

    private ScheduledExecutorService scheduler;
    private ScheduledExecutorService scheduledExecutor;
    private HttpClient httpClient;
    private DirectExchangeClientSupplier directExchangeClientSupplier;
    private ExecutorService pageBufferClientCallbackExecutor;

    @SuppressWarnings("resource")
    @BeforeAll
    public void setUp()
    {
        scheduler = newScheduledThreadPool(4, daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
        pageBufferClientCallbackExecutor = Executors.newSingleThreadExecutor();
        httpClient = new TestingHttpClient(new TestingExchangeHttpClientHandler(taskBuffers, SERDE_FACTORY), scheduler);

        directExchangeClientSupplier = (queryId, exchangeId, span, memoryContext, taskFailureListener, retryPolicy) -> new DirectExchangeClient(
                "localhost",
                DataIntegrityVerification.ABORT,
                new StreamingDirectExchangeBuffer(scheduler, DataSize.of(32, MEGABYTE)),
                DataSize.of(10, MEGABYTE),
                3,
                new Duration(1, TimeUnit.MINUTES),
                true,
                httpClient,
                scheduler,
                memoryContext,
                pageBufferClientCallbackExecutor,
                taskFailureListener);
    }

    @AfterAll
    public void tearDown()
    {
        httpClient.close();
        httpClient = null;

        scheduler.shutdownNow();
        scheduler = null;

        scheduledExecutor.shutdownNow();
        scheduledExecutor = null;

        pageBufferClientCallbackExecutor.shutdownNow();
        pageBufferClientCallbackExecutor = null;
    }

    @Test
    public void testSimple()
            throws Exception
    {
        taskBuffers.invalidateAll();

        SourceOperator operator = createExchangeOperator();

        operator.addSplit(newRemoteSplit(TASK_1_ID));
        operator.addSplit(newRemoteSplit(TASK_2_ID));
        operator.addSplit(newRemoteSplit(TASK_3_ID));
        operator.noMoreSplits();

        // add pages and close the buffers
        taskBuffers.getUnchecked(TASK_1_ID).addPages(10, true);
        taskBuffers.getUnchecked(TASK_2_ID).addPages(10, true);
        taskBuffers.getUnchecked(TASK_3_ID).addPages(10, true);

        // read the pages
        waitForPages(operator, 30);

        // wait for finished
        waitForFinished(operator);
    }

    private static Split newRemoteSplit(TaskId taskId)
    {
        return new Split(REMOTE_CATALOG_HANDLE, new RemoteSplit(new DirectExchangeInput(taskId, "http://localhost/" + taskId)));
    }

    @Test
    public void testWaitForClose()
            throws Exception
    {
        taskBuffers.invalidateAll();

        SourceOperator operator = createExchangeOperator();

        operator.addSplit(newRemoteSplit(TASK_1_ID));
        operator.addSplit(newRemoteSplit(TASK_2_ID));
        operator.addSplit(newRemoteSplit(TASK_3_ID));
        operator.noMoreSplits();

        // add pages and leave buffers open
        taskBuffers.getUnchecked(TASK_1_ID).addPages(1, false);
        taskBuffers.getUnchecked(TASK_2_ID).addPages(1, false);
        taskBuffers.getUnchecked(TASK_3_ID).addPages(1, false);

        // read 3 pages
        waitForPages(operator, 3);

        // verify state
        assertThat(operator.isFinished()).isEqualTo(false);
        assertThat(operator.needsInput()).isEqualTo(false);
        assertThat(operator.getOutput()).isEqualTo(null);

        // add more pages and close the buffers
        taskBuffers.getUnchecked(TASK_1_ID).addPages(2, true);
        taskBuffers.getUnchecked(TASK_2_ID).addPages(2, true);
        taskBuffers.getUnchecked(TASK_3_ID).addPages(2, true);

        // read all pages
        waitForPages(operator, 6);

        // wait for finished
        waitForFinished(operator);
    }

    @Test
    public void testWaitForNoMoreSplits()
            throws Exception
    {
        taskBuffers.invalidateAll();

        SourceOperator operator = createExchangeOperator();

        // add a buffer location containing one page and close the buffer
        operator.addSplit(newRemoteSplit(TASK_1_ID));
        // add pages and leave buffers open
        taskBuffers.getUnchecked(TASK_1_ID).addPages(1, true);

        // read page
        waitForPages(operator, 1);

        // verify state
        assertThat(operator.isFinished()).isEqualTo(false);
        assertThat(operator.needsInput()).isEqualTo(false);
        assertThat(operator.getOutput()).isEqualTo(null);

        // add a buffer location
        operator.addSplit(newRemoteSplit(TASK_2_ID));
        // set no more splits (buffer locations)
        operator.noMoreSplits();
        // add two pages and close the last buffer
        taskBuffers.getUnchecked(TASK_2_ID).addPages(2, true);

        // read all pages
        waitForPages(operator, 2);

        // wait for finished
        waitForFinished(operator);
    }

    @Test
    public void testFinish()
            throws Exception
    {
        taskBuffers.invalidateAll();

        SourceOperator operator = createExchangeOperator();

        operator.addSplit(newRemoteSplit(TASK_1_ID));
        operator.addSplit(newRemoteSplit(TASK_2_ID));
        operator.addSplit(newRemoteSplit(TASK_3_ID));
        operator.noMoreSplits();

        // add pages and leave buffers open
        taskBuffers.getUnchecked(TASK_1_ID).addPages(1, false);
        taskBuffers.getUnchecked(TASK_2_ID).addPages(1, false);
        taskBuffers.getUnchecked(TASK_3_ID).addPages(1, false);

        // read 3 pages
        waitForPages(operator, 3);

        // verify state
        assertThat(operator.isFinished()).isEqualTo(false);
        assertThat(operator.needsInput()).isEqualTo(false);
        assertThat(operator.getOutput()).isEqualTo(null);

        // finish without closing buffers
        operator.finish();

        // wait for finished
        waitForFinished(operator);
    }

    private SourceOperator createExchangeOperator()
    {
        ExchangeOperatorFactory operatorFactory = new ExchangeOperatorFactory(
                0,
                new PlanNodeId("test"),
                directExchangeClientSupplier,
                SERDE_FACTORY,
                RetryPolicy.NONE,
                new ExchangeManagerRegistry(OpenTelemetry.noop(), Tracing.noopTracer(), new SecretsResolver(ImmutableMap.of())));

        DriverContext driverContext = createTaskContext(scheduler, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        SourceOperator operator = operatorFactory.createOperator(driverContext);
        operatorFactory.noMoreOperators();
        return operator;
    }

    private static List<Page> waitForPages(Operator operator, int expectedPageCount)
            throws InterruptedException
    {
        // read expected pages or until 10 seconds has passed
        long endTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        List<Page> outputPages = new ArrayList<>();

        boolean greaterThanZero = false;
        while (System.nanoTime() - endTime < 0) {
            if (operator.isFinished()) {
                break;
            }

            if (operator.getOperatorContext().getDriverContext().getPipelineContext().getPipelineStats().getUserMemoryReservation().toBytes() > 0) {
                greaterThanZero = true;
                break;
            }
            Thread.sleep(10);
        }
        assertThat(greaterThanZero).isTrue();

        while (outputPages.size() < expectedPageCount && System.nanoTime() < endTime) {
            assertThat(operator.needsInput()).isEqualTo(false);
            if (operator.isFinished()) {
                break;
            }

            Page outputPage = operator.getOutput();
            if (outputPage != null) {
                outputPages.add(outputPage);
            }
            else {
                Thread.sleep(10);
            }
        }

        // sleep for a bit to make sure that there aren't extra pages on the way
        Thread.sleep(10);

        // verify state
        assertThat(operator.needsInput()).isEqualTo(false);
        assertThat(operator.getOutput()).isNull();

        // verify pages
        assertThat(outputPages).hasSize(expectedPageCount);
        for (Page page : outputPages) {
            assertPageEquals(TYPES, page, PAGE);
        }

        return outputPages;
    }

    private static void waitForFinished(Operator operator)
            throws Exception
    {
        // wait for finished or until 10 seconds has passed
        long endTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() - endTime < 0) {
            assertThat(operator.needsInput()).isEqualTo(false);
            assertThat(operator.getOutput()).isNull();
            if (operator.isFinished()) {
                break;
            }
            Thread.sleep(10);
        }

        // verify final state
        assertThat(operator.isFinished()).isEqualTo(true);
        assertThat(operator.needsInput()).isEqualTo(false);
        assertThat(operator.getOutput()).isNull();

        operator.close();
        operator.getOperatorContext().destroy();

        assertThat(operator.getOperatorContext().getOperatorStats().getUserMemoryReservation().toBytes()).isEqualTo(0);
    }
}
