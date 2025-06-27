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

import com.google.common.annotations.VisibleForTesting;
import io.trino.Session;
import io.trino.annotation.NotThreadSafe;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.List;

import static io.trino.SystemSessionProperties.isDictionaryAggregationEnabled;
import static io.trino.spi.type.BigintType.BIGINT;

@NotThreadSafe
public interface GroupByHash
{
    static GroupByHash createGroupByHash(
            Session session,
            List<Type> types,
            boolean spillable,
            int expectedSize,
            FlatHashStrategyCompiler hashStrategyCompiler,
            UpdateMemory updateMemory)
    {
        boolean dictionaryAggregationEnabled = isDictionaryAggregationEnabled(session);
        return createGroupByHash(
                types,
                selectGroupByHashMode(spillable, types),
                expectedSize,
                dictionaryAggregationEnabled,
                hashStrategyCompiler,
                updateMemory);
    }

    static GroupByHashMode selectGroupByHashMode(boolean spillable, List<Type> types)
    {
        // Spillable aggregations should always cache hash values since spilling requires sorting by the hash value
        if (spillable) {
            return GroupByHashMode.CACHED;
        }
        // When 3 or more columns are present, always cache the hash value
        if (types.size() >= 3) {
            return GroupByHashMode.CACHED;
        }

        int variableWidthTypes = 0;
        for (Type type : types) {
            // The presence of any container types should trigger hash value caching since computing the hash and
            // checking valueIdentical is so much more expensive for these values
            if (type instanceof MapType || type instanceof ArrayType || type instanceof RowType) {
                return GroupByHashMode.CACHED;
            }
            // Cache hash values when more than 2 or more variable width types are present
            if (type.isFlatVariableWidth()) {
                variableWidthTypes++;
                if (variableWidthTypes >= 2) {
                    return GroupByHashMode.CACHED;
                }
            }
        }
        // All remaining scenarios will use on-demand hashing
        return GroupByHashMode.ON_DEMAND;
    }

    static GroupByHash createGroupByHash(
            List<Type> types,
            GroupByHashMode hashMode,
            int expectedSize,
            boolean dictionaryAggregationEnabled,
            FlatHashStrategyCompiler hashStrategyCompiler,
            UpdateMemory updateMemory)
    {
        if (types.size() == 1 && types.get(0).equals(BIGINT)) {
            return new BigintGroupByHash(expectedSize, updateMemory);
        }
        return new FlatGroupByHash(
                types,
                hashMode,
                expectedSize,
                dictionaryAggregationEnabled,
                hashStrategyCompiler,
                updateMemory);
    }

    long getEstimatedSize();

    int getGroupCount();

    void appendValuesTo(int groupId, PageBuilder pageBuilder);

    /**
     * Signals that no more entries will be inserted, and that only calls to {@link GroupByHash#appendValuesTo(int, PageBuilder)}
     * with sequential groupId values will be observed after this point, allowing the implementation to potentially
     * release memory associated with structures required for inserts or associated with values that have already been
     * output.
     */
    void startReleasingOutput();

    Work<?> addPage(Page page);

    /**
     * The order of new group ids need to be the same as the order of incoming rows,
     * i.e. new group ids should be assigned in rows iteration order
     * Example:
     * rows:      A B C B D A E
     * group ids: 1 2 3 2 4 1 5
     */
    Work<int[]> getGroupIds(Page page);

    long getRawHash(int groupId);

    @VisibleForTesting
    int getCapacity();

    GroupByHash copy();
}
