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
package io.trino.plugin.bigquery;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Enums;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ColumnMetadata;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public final class Conversions
{
    private Conversions() {}

    public static BigQueryColumnHandle toColumnHandle(Field field)
    {
        FieldList subFields = field.getSubFields();
        List<BigQueryColumnHandle> subColumns = subFields == null ?
                Collections.emptyList() :
                subFields.stream()
                        .filter(subField -> isSupportedType(subField.getType()))
                        .map(Conversions::toColumnHandle)
                        .collect(Collectors.toList());
        return new BigQueryColumnHandle(
                field.getName(),
                BigQueryType.valueOf(field.getType().name()),
                getMode(field),
                field.getPrecision(),
                field.getScale(),
                subColumns,
                field.getDescription(),
                false);
    }

    @VisibleForTesting
    public static ColumnMetadata toColumnMetadata(Field field)
    {
        return ColumnMetadata.builder()
                .setName(field.getName())
                .setType(adapt(field).getTrinoType())
                .setComment(Optional.ofNullable(field.getDescription()))
                .setNullable(getMode(field) == Field.Mode.NULLABLE)
                .build();
    }

    public static boolean isSupportedType(LegacySQLTypeName type)
    {
        return Enums.getIfPresent(BigQueryType.class, type.name()).isPresent();
    }

    static BigQueryType.Adaptor adapt(Field field)
    {
        return new BigQueryType.Adaptor()
        {
            @Override
            public BigQueryType getBigQueryType()
            {
                return BigQueryType.valueOf(field.getType().name());
            }

            @Override
            public Long getPrecision()
            {
                return field.getPrecision();
            }

            @Override
            public Long getScale()
            {
                return field.getScale();
            }

            @Override
            public ImmutableMap<String, BigQueryType.Adaptor> getBigQuerySubTypes()
            {
                FieldList subFields = field.getSubFields();
                if (subFields == null) {
                    return ImmutableMap.of();
                }
                return subFields.stream().collect(toImmutableMap(Field::getName, Conversions::adapt));
            }

            @Override
            public Field.Mode getMode()
            {
                return Conversions.getMode(field);
            }
        };
    }

    private static Field.Mode getMode(Field field)
    {
        return firstNonNull(field.getMode(), Field.Mode.NULLABLE);
    }
}
