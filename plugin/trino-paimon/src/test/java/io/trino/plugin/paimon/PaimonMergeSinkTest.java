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
package io.trino.plugin.paimon;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSink;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_COMMIT_ERROR;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_WRITER_DATA_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.ConnectorMergeSink.DELETE_OPERATION_NUMBER;
import static io.trino.spi.connector.ConnectorMergeSink.INSERT_OPERATION_NUMBER;
import static io.trino.spi.connector.ConnectorMergeSink.UPDATE_DELETE_OPERATION_NUMBER;
import static io.trino.spi.connector.ConnectorMergeSink.UPDATE_INSERT_OPERATION_NUMBER;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PaimonMergeSinkTest
{
    @Test
    public void testMergeRowsAreRoutedToDeleteAndInsertPages()
    {
        CapturingPageSink pageSink = new CapturingPageSink();
        PaimonMergeSink mergeSink = new PaimonMergeSink(pageSink, 1);

        mergeSink.storeMergedRows(new Page(
                4,
                integerBlock(10, 20, 30, 40),
                tinyintBlock(
                        INSERT_OPERATION_NUMBER,
                        DELETE_OPERATION_NUMBER,
                        UPDATE_INSERT_OPERATION_NUMBER,
                        UPDATE_DELETE_OPERATION_NUMBER),
                integerBlock(0, 1, 2, 3),
                integerBlock(100, 200, 300, 400)));

        assertThat(pageSink.rowKinds).containsExactly(RowKind.DELETE, RowKind.INSERT);
        assertThat(pageSink.pages).hasSize(2);
        assertThat(pageSink.pages.get(0).getChannelCount()).isEqualTo(1);
        assertThat(pageSink.pages.get(0).getPositionCount()).isEqualTo(2);
        assertThat(INTEGER.getInt(pageSink.pages.get(0).getBlock(0), 0)).isEqualTo(20);
        assertThat(INTEGER.getInt(pageSink.pages.get(0).getBlock(0), 1)).isEqualTo(40);
        assertThat(pageSink.pages.get(1).getChannelCount()).isEqualTo(1);
        assertThat(pageSink.pages.get(1).getPositionCount()).isEqualTo(2);
        assertThat(INTEGER.getInt(pageSink.pages.get(1).getBlock(0), 0)).isEqualTo(10);
        assertThat(INTEGER.getInt(pageSink.pages.get(1).getBlock(0), 1)).isEqualTo(30);
    }

    @Test
    public void testEmptyMergePageIsNoOp()
    {
        CapturingPageSink pageSink = new CapturingPageSink();
        PaimonMergeSink mergeSink = new PaimonMergeSink(pageSink, 1);

        mergeSink.storeMergedRows(new Page(
                0,
                integerBlock(),
                tinyintBlock(),
                integerBlock(),
                integerBlock()));

        assertThat(pageSink.pages).isEmpty();
        assertThat(pageSink.rowKinds).isEmpty();
    }

    @Test
    public void testInvalidMergePageShapeFailsFast()
    {
        PaimonMergeSink mergeSink = new PaimonMergeSink(new CapturingPageSink(), 1);

        assertThatThrownBy(() -> mergeSink.storeMergedRows(new Page(
                1,
                integerBlock(1),
                tinyintBlock(INSERT_OPERATION_NUMBER))))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_DATA_ERROR.toErrorCode());
                    assertThat(exception.getCause())
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("inputPage channelCount (2) must equal dataColumns size (1) + 3");
                })
                .hasMessage("Failed to write data to Paimon: inputPage channelCount (2) must equal dataColumns size (1) + 3");
    }

    @Test
    public void testInvalidMergeOperationFailsFast()
    {
        PaimonMergeSink mergeSink = new PaimonMergeSink(new CapturingPageSink(), 1);

        assertThatThrownBy(() -> mergeSink.storeMergedRows(new Page(
                1,
                integerBlock(1),
                tinyintBlock(3),
                integerBlock(0),
                integerBlock(10))))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_DATA_ERROR.toErrorCode());
                    assertThat(exception.getCause())
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("Invalid merge operation: 3");
                })
                .hasMessage("Failed to write data to Paimon: Invalid merge operation: 3");
    }

    @Test
    public void testConstructorRequiresPaimonPageSink()
    {
        assertThatThrownBy(() -> new PaimonMergeSink(null, 1))
                .hasMessage("pageSink is null");
        assertThatThrownBy(() -> new PaimonMergeSink(new ConnectorPageSink()
        {
            @Override
            public CompletableFuture<?> appendPage(Page page)
            {
                return NOT_BLOCKED;
            }

            @Override
            public CompletableFuture<Collection<Slice>> finish()
            {
                return CompletableFuture.completedFuture(List.of());
            }

            @Override
            public void abort() {}
        }, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("PaimonMergeSink requires PaimonPageSink");
        assertThatThrownBy(() -> new PaimonMergeSink(new CapturingPageSink(), -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("dataColumnCount must be non-negative: -1");
    }

    @Test
    public void testMetadataDeleteMergeSinkAcceptsOnlyDeleteRows()
    {
        PaimonMetadataDeleteMergeSink mergeSink = new PaimonMetadataDeleteMergeSink();

        mergeSink.storeMergedRows(new Page(
                3,
                integerBlock(10, 20, 30),
                tinyintBlock(DELETE_OPERATION_NUMBER, DELETE_OPERATION_NUMBER, DELETE_OPERATION_NUMBER),
                integerBlock(0, 0, 0),
                integerBlock(100, 200, 300)));
        mergeSink.storeMergedRows(new Page(
                2,
                integerBlock(40, 50),
                tinyintBlock(DELETE_OPERATION_NUMBER, DELETE_OPERATION_NUMBER),
                integerBlock(0, 0),
                integerBlock(400, 500)));

        Collection<Slice> fragments = mergeSink.finish().join();
        assertThat(fragments).singleElement()
                .satisfies(fragment -> assertThat(PaimonMetadataDeleteMergeSink.decodeDeletedRowCount(fragment))
                        .isEqualTo(5));
    }

    @Test
    public void testMetadataDeleteMergeSinkRejectsOverflowingDeletedRowCount()
            throws ReflectiveOperationException
    {
        PaimonMetadataDeleteMergeSink mergeSink = new PaimonMetadataDeleteMergeSink();
        setDeletedRowCount(mergeSink, Long.MAX_VALUE);

        assertThatThrownBy(() -> mergeSink.storeMergedRows(new Page(
                1,
                integerBlock(10),
                tinyintBlock(DELETE_OPERATION_NUMBER),
                integerBlock(0),
                integerBlock(100))))
                .isInstanceOfSatisfying(TrinoException.class,
                        exception -> assertThat(exception.getErrorCode()).isEqualTo(PAIMON_COMMIT_ERROR.toErrorCode()))
                .hasMessage("Paimon metadata-delete merge row count exceeds the supported range");
    }

    @Test
    public void testMetadataDeleteMergeSinkEmptyPageIsNoOp()
    {
        PaimonMetadataDeleteMergeSink mergeSink = new PaimonMetadataDeleteMergeSink();

        mergeSink.storeMergedRows(new Page(
                0,
                integerBlock(),
                tinyintBlock(),
                integerBlock(),
                integerBlock()));

        assertThat(mergeSink.finish().join()).isEmpty();
    }

    @Test
    public void testMetadataDeleteMergeSinkRejectsMalformedPage()
    {
        PaimonMetadataDeleteMergeSink mergeSink = new PaimonMetadataDeleteMergeSink();

        assertThatThrownBy(() -> mergeSink.storeMergedRows(new Page(1, integerBlock(1))))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_DATA_ERROR.toErrorCode());
                    assertThat(exception.getCause())
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("inputPage channelCount (1) must include operation and rowId channels");
                })
                .hasMessage("Failed to write data to Paimon: inputPage channelCount (1) must include operation and rowId channels");
    }

    @Test
    public void testMetadataDeleteMergeSinkRejectsNonDeleteOperations()
    {
        PaimonMetadataDeleteMergeSink mergeSink = new PaimonMetadataDeleteMergeSink();

        assertThatThrownBy(() -> mergeSink.storeMergedRows(new Page(
                1,
                integerBlock(1),
                tinyintBlock(INSERT_OPERATION_NUMBER),
                integerBlock(0),
                integerBlock(10))))
                .isInstanceOfSatisfying(TrinoException.class,
                        exception -> assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessage("Paimon metadata-delete merge sink only supports DELETE rows, got merge operation: 1");
    }

    @Test
    public void testMetadataDeleteMergeFragmentValidation()
    {
        assertThat(PaimonMetadataDeleteMergeSink.decodeDeletedRowCount(
                PaimonMetadataDeleteMergeSink.encodeDeletedRowCount(17))).isEqualTo(17);

        assertThatThrownBy(() -> PaimonMetadataDeleteMergeSink.encodeDeletedRowCount(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("deletedRowCount is negative: -1");
        assertThatThrownBy(() -> PaimonMetadataDeleteMergeSink.decodeDeletedRowCount(Slices.allocate(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid Paimon metadata-delete merge fragment size: 1");
    }

    @Test
    public void testAbortClosesUnderlyingPageSink()
    {
        CapturingPageSink pageSink = new CapturingPageSink();
        PaimonMergeSink mergeSink = new PaimonMergeSink(pageSink, 1);

        mergeSink.abort();

        assertThat(pageSink.aborted).isTrue();
    }

    private static Block integerBlock(int... values)
    {
        BlockBuilder builder = INTEGER.createFixedSizeBlockBuilder(values.length);
        for (int value : values) {
            INTEGER.writeLong(builder, value);
        }
        return builder.build();
    }

    private static Block tinyintBlock(int... values)
    {
        BlockBuilder builder = TINYINT.createFixedSizeBlockBuilder(values.length);
        for (int value : values) {
            TINYINT.writeLong(builder, value);
        }
        return builder.build();
    }

    private static void setDeletedRowCount(PaimonMetadataDeleteMergeSink mergeSink, long deletedRowCount)
            throws ReflectiveOperationException
    {
        Field field = PaimonMetadataDeleteMergeSink.class.getDeclaredField("deletedRowCount");
        field.setAccessible(true);
        field.setLong(mergeSink, deletedRowCount);
    }

    private static BatchTableWrite writer()
    {
        return (BatchTableWrite) Proxy.newProxyInstance(
                PaimonMergeSinkTest.class.getClassLoader(),
                new Class<?>[] {BatchTableWrite.class},
                (_, method, _) -> switch (method.getName()) {
                    case "prepareCommit" -> List.<CommitMessage>of();
                    case "close" -> null;
                    case "toString" -> "testing-writer";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static class CapturingPageSink
            extends PaimonPageSink
    {
        private final List<Page> pages = new ArrayList<>();
        private final List<RowKind> rowKinds = new ArrayList<>();
        private boolean aborted;

        private CapturingPageSink()
        {
            super(writer(), List.of(INTEGER), List.of(DataTypes.INT()));
        }

        @Override
        public void writePage(Page page, RowKind rowKind)
        {
            pages.add(page);
            rowKinds.add(rowKind);
        }

        @Override
        public void abort()
        {
            aborted = true;
        }
    }
}
