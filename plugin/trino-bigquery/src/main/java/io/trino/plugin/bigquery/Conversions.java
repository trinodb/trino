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
import io.trino.spi.type.Decimals;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.firstNonNull;
import static io.trino.plugin.bigquery.BigQueryType.toTrinoType;

public final class Conversions
{
    private Conversions() {}

    public static BigQueryColumnHandle toColumnHandle(Field field)
    {
        FieldList subFields = field.getSubFields();
        List<BigQueryColumnHandle> subColumns = subFields == null ?
                Collections.emptyList() :
                subFields.stream()
                        .filter(Conversions::isSupportedType)
                        .map(Conversions::toColumnHandle)
                        .collect(Collectors.toList());
        return new BigQueryColumnHandle(
                field.getName(),
                toTrinoType(field).orElseThrow(() -> new IllegalArgumentException("Unsupported type: " + field)),
                field.getType().getStandardType(),
                getMode(field),
                field.getPrecision(),
                field.getScale(),
                subColumns,
                field.getDescription(),
                false);
    }

    public static boolean isSupportedType(Field field)
    {
        LegacySQLTypeName type = field.getType();
        if (type == LegacySQLTypeName.BIGNUMERIC) {
            // Skip BIGNUMERIC without parameters because the precision (77) and scale (38) is too large
            if (field.getPrecision() == null && field.getScale() == null) {
                return false;
            }
            if (field.getPrecision() != null && field.getPrecision() > Decimals.MAX_PRECISION) {
                return false;
            }
        }

        return toTrinoType(field).isPresent();
    }

    private static Field.Mode getMode(Field field)
    {
        return firstNonNull(field.getMode(), Field.Mode.NULLABLE);
    }
}
