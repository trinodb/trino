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
package io.trino.plugin.deltalake.delete;

import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.spi.block.Block;

import java.util.List;

import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.ROW_POSITION_COLUMN_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public final class PositionDeleteFilter
{
    private final RoaringBitmapArray deletedRows;

    public PositionDeleteFilter(RoaringBitmapArray deletedRows)
    {
        this.deletedRows = requireNonNull(deletedRows, "deletedRows is null");
    }

    public PageFilter createPredicate(List<DeltaLakeColumnHandle> columns)
    {
        int filePositionChannel = rowPositionChannel(columns);

        return page -> {
            int positionCount = page.getPositionCount();
            int[] retained = new int[positionCount];
            int retainedCount = 0;
            Block block = page.getBlock(filePositionChannel);
            for (int position = 0; position < positionCount; position++) {
                long filePosition = BIGINT.getLong(block, position);
                if (!deletedRows.contains(filePosition)) {
                    retained[retainedCount] = position;
                    retainedCount++;
                }
            }
            if (retainedCount == positionCount) {
                return page;
            }
            return page.getPositions(retained, 0, retainedCount);
        };
    }

    private static int rowPositionChannel(List<DeltaLakeColumnHandle> columns)
    {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).baseColumnName().equals(ROW_POSITION_COLUMN_NAME)) {
                return i;
            }
        }
        throw new IllegalArgumentException("No row position column");
    }
}
