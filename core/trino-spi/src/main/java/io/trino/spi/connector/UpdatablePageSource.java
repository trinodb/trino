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

import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface UpdatablePageSource
        extends ConnectorPageSource
{
    default void deleteRows(Block rowIds)
    {
        throw new UnsupportedOperationException("This connector does not support row-level delete");
    }

    /**
     * Write updated rows to the PageSource.
     * @param page Contains values for all updated columns, as well as the $row_id column. The order of these Blocks can be derived from columnValueAndRowIdChannels.
     * @param columnValueAndRowIdChannels The index of this list matches the index columns have in updatedColumns parameter of {@link ConnectorMetadata#beginUpdate}
     * The value at each index is the channel number in the given page. The last element of this list is always the channel number for the $row_id column.
     */
    default void updateRows(Page page, List<Integer> columnValueAndRowIdChannels)
    {
        throw new UnsupportedOperationException("This connector does not support row update");
    }

    CompletableFuture<Collection<Slice>> finish();

    default void abort() {}
}
