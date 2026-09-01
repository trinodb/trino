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
package io.trino.plugin.iceberg.delete;

import com.google.common.collect.ImmutableList;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.SourcePage;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalLong;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.apache.iceberg.FileContent.POSITION_DELETES;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.apache.iceberg.FileFormat.PUFFIN;
import static org.apache.iceberg.MetadataColumns.ROW_POSITION;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDeleteManager
{
    private static final String DATA_FILE_PATH = "s3://bucket/data.parquet";
    private static final IcebergColumnHandle ROW_POSITION_HANDLE = getColumnHandle(ROW_POSITION, TESTING_TYPE_MANAGER);
    private static final Schema SCHEMA = new Schema(optional(1, "key", Types.LongType.get()));

    @Test
    void testDeletionVectorMemoryTracked()
    {
        DeletionVector deletionVector = DeletionVector.builder()
                .add(0)
                .add(3)
                .build()
                .orElseThrow();

        DeleteFile deletionVectorFile = new DeleteFile(
                POSITION_DELETES,
                "s3://bucket/deletion-vector.puffin",
                PUFFIN,
                2,
                100,
                ImmutableList.of(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                1,
                OptionalLong.of(4),
                Optional.of(40),
                Optional.empty());

        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
        Optional<PageFilter> pageFilter = newDeleteManager().getDeletePageFilter(
                DATA_FILE_PATH,
                OptionalLong.of(1),
                ImmutableList.of(deletionVectorFile),
                ImmutableList.of(ROW_POSITION_HANDLE),
                SCHEMA,
                OptionalLong.empty(),
                OptionalLong.empty(),
                _ -> deletionVector,
                (_, _, _) -> {
                    throw new UnsupportedOperationException();
                },
                memoryContext::setBytes);

        assertThat(memoryContext.getBytes()).isEqualTo(DeletionVector.builder().add(0).add(3).build().orElseThrow().retainedSizeInBytes());
        assertThat(filterRowPositions(pageFilter.orElseThrow(), 0, 1, 2, 3, 4)).isEqualTo(3);
    }

    @Test
    void testPositionDeleteMemoryTracked()
    {
        DeleteFile positionDeleteFile = new DeleteFile(
                POSITION_DELETES,
                "s3://bucket/delete-1.parquet",
                PARQUET,
                2,
                100,
                ImmutableList.of(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                1,
                OptionalLong.empty(),
                Optional.empty(),
                Optional.empty());

        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
        Optional<PageFilter> pageFilter = newDeleteManager().getDeletePageFilter(
                DATA_FILE_PATH,
                OptionalLong.of(1),
                ImmutableList.of(positionDeleteFile),
                ImmutableList.of(ROW_POSITION_HANDLE),
                SCHEMA,
                OptionalLong.empty(),
                OptionalLong.empty(),
                _ -> {
                    throw new UnsupportedOperationException();
                },
                (_, _, _) -> positionDeletePageSource(0, 3),
                memoryContext::setBytes);

        assertThat(memoryContext.getBytes()).isEqualTo(DeletionVector.builder().add(0).add(3).build().orElseThrow().retainedSizeInBytes());
        assertThat(filterRowPositions(pageFilter.orElseThrow(), 0, 1, 2, 3, 4)).isEqualTo(3);
    }

    private static DeleteManager newDeleteManager()
    {
        return new DeleteManager(TESTING_TYPE_MANAGER, Optional.empty(), () -> {});
    }

    private static ConnectorPageSource positionDeletePageSource(long... deletedPositions)
    {
        BlockBuilder pathBuilder = VARCHAR.createBlockBuilder(null, deletedPositions.length);
        BlockBuilder positionBuilder = BIGINT.createBlockBuilder(null, deletedPositions.length);
        for (long deletedPosition : deletedPositions) {
            VARCHAR.writeSlice(pathBuilder, utf8Slice(DATA_FILE_PATH));
            BIGINT.writeLong(positionBuilder, deletedPosition);
        }
        return new FixedPageSource(ImmutableList.of(new Page(pathBuilder.build(), positionBuilder.build())));
    }

    private static int filterRowPositions(PageFilter pageFilter, long... rowPositions)
    {
        BlockBuilder positionBuilder = BIGINT.createBlockBuilder(null, rowPositions.length);
        for (long rowPosition : rowPositions) {
            BIGINT.writeLong(positionBuilder, rowPosition);
        }
        SourcePage page = SourcePage.create(new Page(positionBuilder.build()));
        pageFilter.applyFilter(page);
        return page.getPositionCount();
    }
}
