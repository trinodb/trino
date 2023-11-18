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
package io.trino.plugin.hive.projection;

import io.trino.spi.predicate.Domain;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.projection.PartitionProjectionProperties.COLUMN_PROJECTION_VALUES;
import static io.trino.plugin.hive.projection.PartitionProjectionProperties.getProjectionPropertyRequiredValue;
import static io.trino.spi.predicate.Domain.singleValue;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

final class EnumProjection
        implements Projection
{
    private final String columnName;
    private final List<String> values;

    public EnumProjection(String columnName, Type columnType, Map<String, Object> columnProperties)
    {
        if (!(columnType instanceof VarcharType)) {
            throw new InvalidProjectionException(columnName, columnType);
        }

        this.columnName = requireNonNull(columnName, "columnName is null");
        this.values = getProjectionPropertyRequiredValue(
                columnName,
                columnProperties,
                COLUMN_PROJECTION_VALUES,
                value -> ((List<?>) value).stream()
                        .map(String::valueOf)
                        .collect(toImmutableList()));
    }

    @Override
    public List<String> getProjectedValues(Optional<Domain> partitionValueFilter)
    {
        if (partitionValueFilter.isEmpty() || partitionValueFilter.get().isAll()) {
            return values;
        }
        return values.stream()
                .filter(value -> isValueInDomain(partitionValueFilter.get(), value))
                .collect(toList());
    }

    private boolean isValueInDomain(Domain valueDomain, String value)
    {
        Type type = valueDomain.getType();
        if (type instanceof VarcharType) {
            return valueDomain.contains(singleValue(type, utf8Slice(value)));
        }
        throw new InvalidProjectionException(columnName, type);
    }
}
