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

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

public interface ConnectorMergeSink
{
    /**
     * Store the page resulting from a merge.  The page consists n blocks, numbered 0..n-1:
     * <ul>
     *     <li>Blocks 0..n-3 in page are the data column blocks.</li>
     *     <li>Block n-2: Is the integer "operation", whose values are {@link MergeProcessorUtilities#INSERT_OPERATION_NUMBER},
     *         {@link MergeProcessorUtilities#DELETE_OPERATION_NUMBER} or {@link MergeProcessorUtilities#UPDATE_OPERATION_NUMBER}
     *     </li>
     *     <li>Block n-1 is a connector-specific rowId column, whose handle was previously returned by
     *         {@link ConnectorMetadata#getMergeRowIdColumnHandle(ConnectorSession, ConnectorTableHandle)}
     *     </li>
     * </ul>
     * @param page The page to store.
     */
    void storeMergedRows(Page page);

    CompletableFuture<Collection<Slice>> finish();
}
