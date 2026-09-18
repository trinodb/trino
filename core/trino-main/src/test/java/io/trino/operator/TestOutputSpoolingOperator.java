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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.client.spooling.DataAttributes;
import io.trino.metadata.SessionPropertyManager;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.server.protocol.spooling.SpooledMetadataBlock;
import io.trino.server.protocol.spooling.SpoolingConfig;
import io.trino.server.protocol.spooling.SpoolingSessionProperties;
import io.trino.spi.Page;
import io.trino.spi.spool.SpooledLocation;
import io.trino.spi.spool.SpooledLocation.DirectLocation;
import io.trino.spi.spool.SpooledSegmentHandle;
import io.trino.spi.spool.SpoolingContext;
import io.trino.spi.spool.SpoolingManager;
import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SequencePageBuilder.createSequencePage;
import static io.trino.client.spooling.DataAttribute.ROWS_COUNT;
import static io.trino.client.spooling.DataAttribute.SEGMENT_SIZE;
import static io.trino.operator.OutputSpoolingOperatorFactory.OutputSpoolingOperator.SPOOLING_THRESHOLD;
import static io.trino.server.protocol.spooling.SpooledMetadataBlockSerde.deserialize;
import static io.trino.server.protocol.spooling.SpoolingSessionProperties.INITIAL_SEGMENT_SIZE;
import static io.trino.server.protocol.spooling.SpoolingSessionProperties.INLINING_ENABLED;
import static io.trino.server.protocol.spooling.SpoolingSessionProperties.MAX_SEGMENT_SIZE;
import static io.trino.spi.spool.SpooledLocation.coordinatorLocation;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestOutputSpoolingOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeEach
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
    }

    @AfterEach
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    // ~6 MB of BIGINT data: too many rows/bytes to be inlined proactively, and several times the 2 MB
    // inlining threshold so the spooled segment is split into multiple inlined segments on fallback.
    private static final int LARGE_ROW_COUNT = 700_000;

    @Test
    public void testSpoolsWhenStorageIsAvailable()
    {
        // The segment is spooled as a single segment when the spooling storage works
        List<SpooledMetadataBlock> segments = process(new TestingSpoolingManager(), largeInput(), true);

        assertThat(segments).hasSize(1);
        assertThat(segments).allMatch(SpooledMetadataBlock.Spooled.class::isInstance);
    }

    @Test
    public void testFallbackSplitsLargeSegmentIntoSmallerInlinedSegments()
    {
        // When the spooling storage is transiently down, the segment that failed to spool is split into
        // several smaller inlined segments, each below the 2 MB inlining threshold
        List<SpooledMetadataBlock> segments = process(new RecoverableFailingSpoolingManager(), largeInput(), true);

        assertThat(segments).hasSize(4);
        assertThat(segments).allMatch(SpooledMetadataBlock.Inlined.class::isInstance);
        assertThat(segments).allMatch(segment -> segment.attributes().get(SEGMENT_SIZE, Integer.class) < SPOOLING_THRESHOLD);
        assertThat(segments).allMatch(segment -> segment.attributes().get(ROWS_COUNT, Long.class) < 235_000); // ~SPOOLING_THRESHOLD of BIGINT values
        // No data is lost: the rows of the failed segment are preserved across the inlined segments
        assertThat(totalRows(segments)).isEqualTo(LARGE_ROW_COUNT);
    }

    @Test
    public void testFailsWhenSpoolingFailsWithUnrecoverableError()
    {
        // An unrecoverable failure (e.g. invalid credentials) is not masked by inlining even when
        // inlining is enabled - the query fails so the underlying problem is surfaced
        assertThatThrownBy(() -> process(new UnrecoverableFailingSpoolingManager(), largeInput(), true))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("invalid credentials");
    }

    @Test
    public void testFailsWhenSpoolingFailsAndInliningDisabled()
    {
        // With inlining disabled there is no fallback, so even a recoverable spooling failure fails the query
        assertThatThrownBy(() -> process(new RecoverableFailingSpoolingManager(), largeInput(), false))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("spooling storage is unavailable");
    }

    private static List<Page> largeInput()
    {
        return List.of(createSequencePage(List.of(BIGINT), LARGE_ROW_COUNT));
    }

    private List<SpooledMetadataBlock> process(SpoolingManager spoolingManager, List<Page> input, boolean inliningEnabled)
    {
        Session session = testSessionBuilder(spoolingSessionPropertyManager())
                .setSystemProperty(INLINING_ENABLED, Boolean.toString(inliningEnabled))
                // Keep all input in a single segment so the fallback has to split it
                .setSystemProperty(INITIAL_SEGMENT_SIZE, "64MB")
                .setSystemProperty(MAX_SEGMENT_SIZE, "64MB")
                .build();

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, session)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        OutputSpoolingOperatorFactory factory = new OutputSpoolingOperatorFactory(0, new PlanNodeId("test"), TestOutputSpoolingOperator::rawSizeEncoder, spoolingManager);
        Operator operator = factory.createOperator(driverContext);

        ImmutableList.Builder<SpooledMetadataBlock> segments = ImmutableList.builder();
        for (Page page : input) {
            assertThat(operator.needsInput()).isTrue();
            operator.addInput(page);
            collect(operator, segments);
        }

        operator.finish();
        while (!operator.isFinished()) {
            collect(operator, segments);
        }
        return segments.build();
    }

    private static void collect(Operator operator, ImmutableList.Builder<SpooledMetadataBlock> segments)
    {
        Page output = operator.getOutput();
        if (output != null) {
            segments.addAll(deserialize(output));
        }
    }

    private static long totalRows(List<SpooledMetadataBlock> segments)
    {
        return segments.stream()
                .mapToLong(segment -> segment.attributes().get(ROWS_COUNT, Long.class))
                .sum();
    }

    private static SessionPropertyManager spoolingSessionPropertyManager()
    {
        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager();
        sessionPropertyManager.addSystemSessionProperties(new SpoolingSessionProperties(new SpoolingConfig()).getSessionProperties());
        return sessionPropertyManager;
    }

    /**
     * Minimal encoder that writes one byte per row and reports that as the segment size. The actual
     * bytes are irrelevant for these tests - only the segment sizing and row counts matter.
     */
    private static QueryDataEncoder rawSizeEncoder()
    {
        return new QueryDataEncoder()
        {
            @Override
            public DataAttributes encodeTo(OutputStream output, List<Page> pages)
                    throws IOException
            {
                int rows = toIntExact(pages.stream().mapToLong(Page::getPositionCount).sum());
                output.write(new byte[rows]);
                return DataAttributes.builder()
                        .set(SEGMENT_SIZE, rows)
                        .build();
            }

            @Override
            public String encoding()
            {
                return "test";
            }

            @Override
            public void close() {}
        };
    }

    private static class TestingSpoolingManager
            implements SpoolingManager
    {
        @Override
        public SpooledSegmentHandle create(SpoolingContext context)
        {
            return new TestingSpooledSegmentHandle("segment", context.encoding(), Instant.ofEpochSecond(0));
        }

        @Override
        public OutputStream createOutputStream(SpooledSegmentHandle handle)
                throws IOException
        {
            return OutputStream.nullOutputStream();
        }

        @Override
        public SpooledLocation location(SpooledSegmentHandle handle)
        {
            return coordinatorLocation(utf8Slice(handle.identifier()), Map.of());
        }

        @Override
        public InputStream openInputStream(SpooledSegmentHandle handle)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void acknowledge(SpooledSegmentHandle handle)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<DirectLocation> directLocation(SpooledSegmentHandle handle)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public SpooledSegmentHandle handle(Slice identifier, Map<String, List<String>> headers)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isRecoverableException(IOException exception)
        {
            return false;
        }
    }

    private static class RecoverableFailingSpoolingManager
            extends TestingSpoolingManager
    {
        @Override
        public OutputStream createOutputStream(SpooledSegmentHandle handle)
                throws IOException
        {
            throw new IOException("spooling storage is unavailable");
        }

        @Override
        public boolean isRecoverableException(IOException exception)
        {
            return true;
        }
    }

    private static class UnrecoverableFailingSpoolingManager
            extends TestingSpoolingManager
    {
        @Override
        public OutputStream createOutputStream(SpooledSegmentHandle handle)
                throws IOException
        {
            throw new IOException("invalid credentials");
        }
    }

    private record TestingSpooledSegmentHandle(String identifier, String encoding, Instant expirationTime)
            implements SpooledSegmentHandle {}
}
