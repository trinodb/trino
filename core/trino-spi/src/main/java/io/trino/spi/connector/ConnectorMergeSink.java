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
     * Represents an inserted row.
     */
    int INSERT_OPERATION_NUMBER = 1;

    /**
     * Represents a deleted row.
     */
    int DELETE_OPERATION_NUMBER = 2;

    /**
     * Represents an updated row when using {@link RowChangeParadigm#CHANGE_ONLY_UPDATED_COLUMNS}.
     */
    int UPDATE_OPERATION_NUMBER = 3;

    /**
     * Represents a new version of an updated row, to be inserted, when using
     * {@link RowChangeParadigm#DELETE_ROW_AND_INSERT_ROW}.
     */
    int UPDATE_INSERT_OPERATION_NUMBER = 4;

    /**
     * Represents an old version of an updated row, to be deleted, when using
     * {@link RowChangeParadigm#DELETE_ROW_AND_INSERT_ROW}.
     */
    int UPDATE_DELETE_OPERATION_NUMBER = 5;

    /**
     * Store the page resulting from a merge. The page consists of {@code n} channels, numbered {@code 0..n-1}:
     * <ul>
     *     <li>Blocks {@code 0..n-3} in page are the data columns</li>
     *     <li>Block {@code n-2} is the tinyint <i>operation</i>:
     *     <ul>
     *         <li>{@link #INSERT_OPERATION_NUMBER}</li>
     *         <li>{@link #DELETE_OPERATION_NUMBER}</li>
     *         <li>{@link #UPDATE_OPERATION_NUMBER}</li>
     *         <li>{@link #UPDATE_INSERT_OPERATION_NUMBER}</li>
     *         <li>{@link #UPDATE_DELETE_OPERATION_NUMBER}</li>
     *     </ul>
     *     <li>Block {@code n-1} is a connector-specific rowId column, whose handle was previously returned by
     *         {@link ConnectorMetadata#getMergeRowIdColumnHandle(ConnectorSession, ConnectorTableHandle) getMergeRowIdColumnHandle()}
     *     </li>
     * </ul>
     *
     * @param page The page to store.
     */
    void storeMergedRows(Page page);

    CompletableFuture<Collection<Slice>> finish();

    default void abort() {}
}
