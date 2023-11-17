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
package io.trino.plugin.hive.aws.athena;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_DIGITS;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_INTERVAL;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_RANGE;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.getProjectionPropertyRequiredValue;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.getProjectionPropertyValue;
import static io.trino.spi.predicate.Domain.singleValue;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

final class IntegerProjection
        implements Projection
{
    private final String columnName;
    private final int leftBound;
    private final int rightBound;
    private final int interval;
    private final Optional<Integer> digits;

    public IntegerProjection(String columnName, Type columnType, Map<String, Object> columnProperties)
    {
        if (!(columnType instanceof VarcharType) && !(columnType instanceof IntegerType) && !(columnType instanceof BigintType)) {
            throw new InvalidProjectionException(columnName, columnType);
        }

        this.columnName = requireNonNull(columnName, "columnName is null");

        List<Integer> range = getProjectionPropertyRequiredValue(
                columnName,
                columnProperties,
                COLUMN_PROJECTION_RANGE,
                value -> ((List<?>) value).stream()
                        .map(element -> Integer.valueOf((String) element))
                        .collect(toImmutableList()));
        if (range.size() != 2) {
            throw new InvalidProjectionException(columnName, format("Property: '%s' needs to be list of 2 integers", COLUMN_PROJECTION_RANGE));
        }
        this.leftBound = range.get(0);
        this.rightBound = range.get(1);

        this.interval = getProjectionPropertyValue(columnProperties, COLUMN_PROJECTION_INTERVAL, Integer.class::cast).orElse(1);
        this.digits = getProjectionPropertyValue(columnProperties, COLUMN_PROJECTION_DIGITS, Integer.class::cast);
    }

    @Override
    public List<String> getProjectedValues(Optional<Domain> partitionValueFilter)
    {
        Builder<String> builder = ImmutableList.builder();
        int current = leftBound;
        while (current <= rightBound) {
            int currentValue = current;
            String currentValueFormatted = digits
                    .map(digits -> format("%0" + digits + "d", currentValue))
                    .orElseGet(() -> Integer.toString(currentValue));
            if (isValueInDomain(partitionValueFilter, current, currentValueFormatted)) {
                builder.add(currentValueFormatted);
            }
            current += interval;
        }
        return builder.build();
    }

    private boolean isValueInDomain(Optional<Domain> valueDomain, int value, String formattedValue)
    {
        if (valueDomain.isEmpty() || valueDomain.get().isAll()) {
            return true;
        }
        Domain domain = valueDomain.get();
        Type type = domain.getType();
        if (type instanceof VarcharType) {
            return domain.contains(singleValue(type, utf8Slice(formattedValue)));
        }
        if (type instanceof IntegerType || type instanceof BigintType) {
            return domain.contains(singleValue(type, (long) value));
        }
        throw new InvalidProjectionException(columnName, type);
    }
}
