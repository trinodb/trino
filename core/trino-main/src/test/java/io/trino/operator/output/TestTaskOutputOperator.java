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
package io.trino.operator.output;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.execution.buffer.ArbitraryOutputBuffer;
import io.trino.execution.buffer.OutputBufferStateMachine;
import io.trino.execution.buffer.PageSerializer;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.memory.context.SimpleLocalMemoryContext;
import io.trino.operator.OperatorContext;
import io.trino.plugin.base.util.Lazy;
import io.trino.spi.Page;
import io.trino.spi.QueryId;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.TestingTaskContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.SequencePageBuilder.createSequencePage;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.buffer.CompressionCodec.LZ4;
import static io.trino.execution.buffer.TestingPagesSerdes.createTestingPagesSerdeFactory;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestTaskOutputOperator
{
    private static final List<Type> TYPES = ImmutableList.of(BIGINT);

    private ScheduledExecutorService scheduledExecutor;

    @BeforeAll
    public void setUp()
    {
        scheduledExecutor = newScheduledThreadPool(0, threadsNamed("TestTaskOutputOperator-%s"));
    }

    @AfterAll
    public void tearDown()
    {
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testMemoryReservationTracksSerializer()
    {
        PagesSerdeFactory serdeFactory = createTestingPagesSerdeFactory(LZ4);
        OperatorContext operatorContext = TestingTaskContext.builder(directExecutor(), scheduledExecutor, TEST_SESSION)
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext()
                .addOperatorContext(0, new PlanNodeId("test"), TaskOutputOperator.class.getSimpleName());
        ArbitraryOutputBuffer outputBuffer = new ArbitraryOutputBuffer(
                0,
                new OutputBufferStateMachine(new TaskId(new StageId(new QueryId("query"), 0), 0, 0), directExecutor()),
                DataSize.of(32, MEGABYTE),
                Lazy.from(() -> new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test")),
                directExecutor());
        TaskOutputOperator operator = new TaskOutputOperator(operatorContext, outputBuffer, Function.identity(), serdeFactory);

        PageSerializer serializer = serdeFactory.createSerializer(Optional.empty());
        for (Page page : ImmutableList.of(createSequencePage(TYPES, 10), createSequencePage(TYPES, 10_000))) {
            operator.addInput(page);
            serializer.serialize(page);
            assertThat(operatorContext.getOperatorStats().getUserMemoryReservation().toBytes())
                    .isEqualTo(serializer.getRetainedSizeInBytes());
        }
    }
}
