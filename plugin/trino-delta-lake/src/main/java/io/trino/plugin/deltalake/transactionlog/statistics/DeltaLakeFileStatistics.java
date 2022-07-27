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
package io.trino.plugin.deltalake.transactionlog.statistics;

import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.transactionlog.CanonicalColumnName;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.function.UnaryOperator.identity;

public interface DeltaLakeFileStatistics
{
    Optional<Long> getNumRecords();

    Optional<Map<String, Object>> getMinValues();

    Optional<Map<String, Object>> getMaxValues();

    Optional<Map<String, Object>> getNullCount();

    Optional<Long> getNullCount(String columnName);

    Optional<Object> getMinColumnValue(DeltaLakeColumnHandle columnHandle);

    Optional<Object> getMaxColumnValue(DeltaLakeColumnHandle columnHandle);

    long getRetainedSizeInBytes();

    static Map<String, CanonicalColumnName> getCanonicalColumnNames(
            Optional<Map<String, Object>> minValues,
            Optional<Map<String, Object>> maxValues,
            Optional<Map<String, Object>> nullCount)
    {
        return Stream.of(minValues, maxValues, nullCount)
                .flatMap(Optional::stream)
                .flatMap(map -> map.keySet().stream())
                .collect(toImmutableMap(
                        identity(),
                        CanonicalColumnName::new,
                        // Ignore duplicate Strings
                        (name1, name2) -> name1));
    }
}
