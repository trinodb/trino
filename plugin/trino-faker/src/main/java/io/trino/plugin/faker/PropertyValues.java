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

import io.airlift.units.Duration;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_PROPERTY;
import static io.trino.spi.type.DateType.DATE;

public class PropertyValues
{
    private PropertyValues() {}

    public static Object propertyValue(ColumnMetadata column, String property)
    {
        Object propertyValue = column.getProperties().get(property);
        if (propertyValue == null) {
            return null;
        }

        if (propertyValue instanceof Collection<?> propertyValues) {
            return propertyValues.stream()
                    .map(String.class::cast)
                    .map(value -> {
                        try {
                            return Literal.parse(value, column.getType());
                        }
                        catch (IllegalArgumentException | ClassCastException e) {
                            throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` property must only contain valid %s literals, failed to parse `%s`".formatted(property, column.getType().getDisplayName(), value), e);
                        }
                    })
                    .collect(toImmutableList());
        }

        if (property.equals(ColumnInfo.STEP_PROPERTY)) {
            Type type = column.getType();
            if (DATE.equals(type) || type instanceof TimestampType || type instanceof TimestampWithTimeZoneType || type instanceof TimeType || type instanceof TimeWithTimeZoneType) {
                try {
                    return Duration.valueOf((String) propertyValue).roundTo(TimeUnit.NANOSECONDS);
                }
                catch (IllegalArgumentException e) {
                    throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` property for a %s column must be a valid duration literal".formatted(property, type.getDisplayName()), e);
                }
            }
        }

        try {
            return Literal.parse((String) propertyValue, column.getType());
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` property must be a valid %s literal".formatted(property, column.getType().getDisplayName()), e);
        }
    }

    private static List<String> strings(Collection<?> values)
    {
        return values.stream()
                .map(String.class::cast)
                .collect(toImmutableList());
    }
}
