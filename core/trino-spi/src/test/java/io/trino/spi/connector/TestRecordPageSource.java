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
package io.trino.spi.connector;

import io.trino.spi.PageBuilder;
import io.trino.spi.connector.InMemoryRecordSet.InMemoryRecordCursor;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestRecordPageSource
{
    @Test
    public void testCloseOnInitialMemoryReservationFailure()
    {
        RuntimeException reservationFailure = new RuntimeException("reservation failed");
        RuntimeException closeFailure = new RuntimeException("close failed");
        MemoryTrackingRecordCursor cursor = new MemoryTrackingRecordCursor(List.of(BIGINT), List.of())
        {
            @Override
            public void close()
            {
                super.close();
                throw closeFailure;
            }
        };

        assertThatThrownBy(() -> new RecordPageSource(List.of(BIGINT), cursor, _ -> {
            throw reservationFailure;
        })).isSameAs(reservationFailure);
        assertThat(cursor.closed).isTrue();
        assertThat(reservationFailure).hasSuppressedException(closeFailure);
    }

    @Test
    public void testMemoryUsageUpdates()
    {
        List<List<Object>> records = LongStream.range(0, 5000)
                .mapToObj(value -> List.<Object>of(value))
                .toList();
        MemoryTrackingRecordCursor cursor = new MemoryTrackingRecordCursor(List.of(BIGINT), records);
        AtomicLong reportedBytes = new AtomicLong(-1);
        long emptyBuilderBytes = new PageBuilder(List.of(BIGINT)).getRetainedSizeInBytes();
        try (RecordPageSource source = new RecordPageSource(List.of(BIGINT), cursor, reportedBytes::set)) {
            assertThat(reportedBytes).hasValue(cursor.getMemoryUsage() + emptyBuilderBytes);
            assertThat(source.getMemoryUsage()).isZero();

            // A request can grow both the cursor and the builder without returning a page.
            assertThat(source.getNextSourcePage()).isNull();
            assertThat(reportedBytes.get()).isGreaterThan(cursor.getMemoryUsage() + 4096L * Long.BYTES);
            assertThat(source.isFinished()).isFalse();

            SourcePage page = source.getNextSourcePage();
            assertThat(page.getPositionCount()).isEqualTo(5000);
            assertThat(BIGINT.getLong(page.getBlock(0), 0)).isZero();
            assertThat(BIGINT.getLong(page.getBlock(0), 4999)).isEqualTo(4999);
            assertThat(source.isFinished()).isTrue();
            assertThat(reportedBytes).hasValue(cursor.getMemoryUsage() + emptyBuilderBytes);
            assertThat(source.getMemoryUsage()).isZero();
            long beforeClose = reportedBytes.get();
            long cursorBytes = cursor.getMemoryUsage();

            source.close();
            assertThat(reportedBytes).hasValue(beforeClose - cursorBytes);
        }
        // The builder is still retained. Its reservation is released by the context's owner.
        assertThat(reportedBytes).hasValue(emptyBuilderBytes);
        assertThat(reportedBytes.get()).isPositive();
    }

    @Test
    public void testFullPages()
    {
        InMemoryRecordSet recordSet = new InMemoryRecordSet(List.of(VARCHAR), nCopies(3000, List.of("x".repeat(1024))));
        AtomicLong reportedBytes = new AtomicLong(-1);
        long emptyBuilderBytes = new PageBuilder(List.of(VARCHAR)).getRetainedSizeInBytes();
        try (RecordPageSource source = new RecordPageSource(recordSet, reportedBytes::set)) {
            assertThat(reportedBytes).hasValue(emptyBuilderBytes);
            int positions = 0;
            int pages = 0;
            while (!source.isFinished()) {
                SourcePage page = source.getNextSourcePage();
                assertThat(page).isNotNull();
                assertThat(reportedBytes).hasValue(emptyBuilderBytes);
                assertThat(source.getMemoryUsage()).isZero();
                positions += page.getPositionCount();
                pages++;
            }
            assertThat(positions).isEqualTo(3000);
            assertThat(pages).isGreaterThan(1);
        }
    }

    @Test
    public void testCloseWithBufferedRows()
    {
        List<List<Object>> records = nCopies(5000, List.of(42L));
        AtomicLong reportedBytes = new AtomicLong(-1);
        MemoryTrackingRecordCursor cursor = new MemoryTrackingRecordCursor(List.of(BIGINT), records);
        long emptyBuilderBytes = new PageBuilder(List.of(BIGINT)).getRetainedSizeInBytes();
        try (RecordPageSource source = new RecordPageSource(List.of(BIGINT), cursor, reportedBytes::set)) {
            assertThat(source.getNextSourcePage()).isNull();
            long bufferedBytes = reportedBytes.get() - cursor.getMemoryUsage();
            source.close();
            assertThat(reportedBytes).hasValue(bufferedBytes);
            assertThat(bufferedBytes).isGreaterThan(emptyBuilderBytes);
            assertThat(source.isFinished()).isFalse();

            assertThat(source.getNextSourcePage().getPositionCount()).isEqualTo(4096);
            assertThat(reportedBytes).hasValue(emptyBuilderBytes);
            assertThat(source.isFinished()).isTrue();
        }
    }

    private static class MemoryTrackingRecordCursor
            extends InMemoryRecordCursor
    {
        private boolean closed;

        public MemoryTrackingRecordCursor(List<Type> types, List<List<Object>> records)
        {
            super(types, records.iterator());
        }

        @Override
        public long getMemoryUsage()
        {
            return closed ? 0 : 1024 + getCompletedBytes();
        }

        @Override
        public void close()
        {
            super.close();
            closed = true;
        }
    }
}
