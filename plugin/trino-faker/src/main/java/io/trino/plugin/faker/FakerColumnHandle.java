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

package io.trino.plugin.faker;

import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.util.Collection;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.faker.ColumnInfo.ALLOWED_VALUES_PROPERTY;
import static io.trino.plugin.faker.ColumnInfo.GENERATOR_PROPERTY;
import static io.trino.plugin.faker.ColumnInfo.MAX_PROPERTY;
import static io.trino.plugin.faker.ColumnInfo.MIN_PROPERTY;
import static io.trino.plugin.faker.ColumnInfo.NULL_PROBABILITY_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_PROPERTY;
import static java.util.Objects.requireNonNull;

public record FakerColumnHandle(
        int columnIndex,
        String name,
        Type type,
        double nullProbability,
        String generator,
        Domain domain)
        implements ColumnHandle
{
    public FakerColumnHandle
    {
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");
    }

    public static FakerColumnHandle of(int columnId, ColumnMetadata column, double defaultNullProbability)
    {
        double nullProbability = 0;
        if (column.isNullable()) {
            nullProbability = (double) column.getProperties().getOrDefault(NULL_PROBABILITY_PROPERTY, defaultNullProbability);
        }
        String generator = (String) column.getProperties().get(GENERATOR_PROPERTY);
        if (generator != null && !isCharacterColumn(column)) {
            throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` property can only be set for CHAR, VARCHAR or VARBINARY columns".formatted(GENERATOR_PROPERTY));
        }
        Object min;
        try {
            min = Literal.parse((String) column.getProperties().get(MIN_PROPERTY), column.getType());
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` property must be a valid %s literal".formatted(MIN_PROPERTY, column.getType().getDisplayName()), e);
        }
        Object max;
        try {
            max = Literal.parse((String) column.getProperties().get(MAX_PROPERTY), column.getType());
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` property must be a valid %s literal".formatted(MAX_PROPERTY, column.getType().getDisplayName()), e);
        }
        Domain domain = Domain.all(column.getType());
        if (min != null || max != null) {
            if (isCharacterColumn(column)) {
                throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` and `%s` properties cannot be set for CHAR, VARCHAR or VARBINARY columns".formatted(MIN_PROPERTY, MAX_PROPERTY));
            }
            domain = Domain.create(ValueSet.ofRanges(range(column.getType(), min, max)), false);
        }
        if (column.getProperties().containsKey(ALLOWED_VALUES_PROPERTY)) {
            if (min != null || max != null || generator != null) {
                throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` property cannot be set together with `%s`, `%s`, and `%s` properties".formatted(ALLOWED_VALUES_PROPERTY, MIN_PROPERTY, MAX_PROPERTY, GENERATOR_PROPERTY));
            }
            ImmutableList.Builder<Object> builder = ImmutableList.builder();
            for (String value : strings((List<?>) column.getProperties().get(ALLOWED_VALUES_PROPERTY))) {
                try {
                    builder.add(Literal.parse(value, column.getType()));
                }
                catch (IllegalArgumentException | ClassCastException e) {
                    throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` property must only contain valid %s literals, failed to parse `%s`".formatted(ALLOWED_VALUES_PROPERTY, column.getType().getDisplayName(), value), e);
                }
            }
            domain = Domain.create(ValueSet.copyOf(column.getType(), builder.build()), false);
        }

        return new FakerColumnHandle(
                columnId,
                column.getName(),
                column.getType(),
                nullProbability,
                generator,
                domain);
    }

    private static boolean isCharacterColumn(ColumnMetadata column)
    {
        return column.getType() instanceof CharType || column.getType() instanceof VarcharType || column.getType() instanceof VarbinaryType;
    }

    private static Range range(Type type, Object min, Object max)
    {
        requireNonNull(type, "type is null");
        if (min == null && max == null) {
            return Range.all(type);
        }
        if (max == null) {
            return Range.greaterThanOrEqual(type, min);
        }
        if (min == null) {
            return Range.lessThanOrEqual(type, max);
        }
        return Range.range(type, min, true, max, true);
    }

    private static List<String> strings(Collection<?> values)
    {
        return values.stream()
                .map(String.class::cast)
                .collect(toImmutableList());
    }
}
