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
package io.trino.plugin.paimon;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class FieldNameUtils
{
    private FieldNameUtils() {}

    /**
     * Get field names from RowType in lowercase.
     * Paimon stores column names in lowercase in ORC/Parquet files.
     */
    public static List<String> fieldNames(RowType rowType)
    {
        return List.copyOf(fieldNameIndexes(rowType).keySet());
    }

    /**
     * Get lowercase field names mapped to their positions in the RowType.
     */
    public static Map<String, Integer> fieldNameIndexes(RowType rowType)
    {
        Set<String> fieldNames = new HashSet<>();
        Map<String, Integer> indexes = new LinkedHashMap<>();
        List<DataField> fields = requireNonNull(rowType, "rowType is null").getFields();
        for (int index = 0; index < fields.size(); index++) {
            String fieldName = toLowerCase(requireNonNull(fields.get(index), "rowType contains null field").name());
            if (!fieldNames.add(fieldName)) {
                throw new IllegalStateException(
                        "Paimon row type contains case-insensitive duplicate field name '%s'".formatted(fieldName));
            }
            indexes.put(fieldName, index);
        }
        return Collections.unmodifiableMap(indexes);
    }

    /**
     * Convert a single field name to lowercase.
     * Uses Locale.ENGLISH for consistent behavior across different system locales.
     */
    public static String toLowerCase(String fieldName)
    {
        return requireNonNull(fieldName, "fieldName is null").toLowerCase(Locale.ENGLISH);
    }

    /**
     * Convert a list of field names to lowercase.
     * Uses Locale.ENGLISH for consistent behavior across different system locales.
     */
    public static List<String> toLowerCase(List<String> fieldNames)
    {
        return requireNonNull(fieldNames, "fieldNames is null").stream().map(FieldNameUtils::toLowerCase).collect(Collectors.toList());
    }
}
