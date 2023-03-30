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
package io.trino.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.trino.client.ClientTypeSignatureParameter.ParameterKind;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.client.ClientStandardTypes.ARRAY;
import static io.trino.client.ClientStandardTypes.BIGINT;
import static io.trino.client.ClientStandardTypes.BING_TILE;
import static io.trino.client.ClientStandardTypes.BOOLEAN;
import static io.trino.client.ClientStandardTypes.CHAR;
import static io.trino.client.ClientStandardTypes.DATE;
import static io.trino.client.ClientStandardTypes.DECIMAL;
import static io.trino.client.ClientStandardTypes.DOUBLE;
import static io.trino.client.ClientStandardTypes.GEOMETRY;
import static io.trino.client.ClientStandardTypes.INTEGER;
import static io.trino.client.ClientStandardTypes.INTERVAL_DAY_TO_SECOND;
import static io.trino.client.ClientStandardTypes.INTERVAL_YEAR_TO_MONTH;
import static io.trino.client.ClientStandardTypes.IPADDRESS;
import static io.trino.client.ClientStandardTypes.JSON;
import static io.trino.client.ClientStandardTypes.MAP;
import static io.trino.client.ClientStandardTypes.REAL;
import static io.trino.client.ClientStandardTypes.ROW;
import static io.trino.client.ClientStandardTypes.SMALLINT;
import static io.trino.client.ClientStandardTypes.SPHERICAL_GEOGRAPHY;
import static io.trino.client.ClientStandardTypes.TIME;
import static io.trino.client.ClientStandardTypes.TIMESTAMP;
import static io.trino.client.ClientStandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static io.trino.client.ClientStandardTypes.TIME_WITH_TIME_ZONE;
import static io.trino.client.ClientStandardTypes.TINYINT;
import static io.trino.client.ClientStandardTypes.UUID;
import static io.trino.client.ClientStandardTypes.VARCHAR;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

final class FixJsonDataUtils
{
    private FixJsonDataUtils() {}

    public static Iterable<List<Object>> fixData(List<Column> columns, List<List<Object>> data)
    {
        if (data == null) {
            return null;
        }
        ColumnTypeHandler[] typeHandlers = createTypeHandlers(columns);
        ImmutableList.Builder<List<Object>> rows = ImmutableList.builderWithExpectedSize(data.size());
        for (List<Object> row : data) {
            if (row.size() != typeHandlers.length) {
                throw new IllegalArgumentException("row/column size mismatch");
            }
            ArrayList<Object> newRow = new ArrayList<>(typeHandlers.length);
            int column = 0;
            for (Object value : row) {
                if (value != null) {
                    value = typeHandlers[column].fixValue(value);
                }
                newRow.add(value);
                column++;
            }
            rows.add(unmodifiableList(newRow)); // allow nulls in list
        }
        return rows.build();
    }

    private static ColumnTypeHandler[] createTypeHandlers(List<Column> columns)
    {
        requireNonNull(columns, "columns is null");
        ColumnTypeHandler[] typeHandlers = new ColumnTypeHandler[columns.size()];
        int index = 0;
        for (Column column : columns) {
            typeHandlers[index++] = createTypeHandler(column.getTypeSignature());
        }
        return typeHandlers;
    }

    private interface ColumnTypeHandler
    {
        Object fixValue(Object value);
    }

    private static final class ArrayClientTypeHandler
            implements ColumnTypeHandler
    {
        private final ColumnTypeHandler elementHandler;

        private ArrayClientTypeHandler(ClientTypeSignature signature)
        {
            requireNonNull(signature, "signature is null");
            checkArgument(signature.getRawType().equals(ARRAY), "not an array type signature: %s", signature);
            this.elementHandler = createTypeHandler(signature.getArgumentsAsTypeSignatures().get(0));
        }

        @Override
        public List<Object> fixValue(Object value)
        {
            List<?> listValue = (List<?>) value;
            ArrayList<Object> fixedValues = new ArrayList<>(listValue.size());
            for (Object element : listValue) {
                if (element != null) {
                    element = elementHandler.fixValue(element);
                }
                fixedValues.add(element);
            }
            return fixedValues;
        }
    }

    private static final class MapClientTypeHandler
            implements ColumnTypeHandler
    {
        private final ColumnTypeHandler keyHandler;
        private final ColumnTypeHandler valueHandler;

        private MapClientTypeHandler(ClientTypeSignature signature)
        {
            requireNonNull(signature, "signature is null");
            checkArgument(signature.getRawType().equals(MAP), "not a map type signature: %s", signature);
            this.keyHandler = createTypeHandler(signature.getArgumentsAsTypeSignatures().get(0));
            this.valueHandler = createTypeHandler(signature.getArgumentsAsTypeSignatures().get(1));
        }

        @Override
        public Map<Object, Object> fixValue(Object value)
        {
            Map<?, ?> mapValue = (Map<?, ?>) value;
            Map<Object, Object> fixedMap = Maps.newHashMapWithExpectedSize(mapValue.size());
            for (Map.Entry<?, ?> entry : mapValue.entrySet()) {
                Object fixedKey = entry.getKey();
                if (fixedKey != null) {
                    fixedKey = keyHandler.fixValue(fixedKey);
                }
                Object fixedValue = entry.getValue();
                if (fixedValue != null) {
                    fixedValue = valueHandler.fixValue(fixedValue);
                }
                fixedMap.put(fixedKey, fixedValue);
            }
            return fixedMap;
        }
    }

    private static final class RowClientTypeHandler
            implements ColumnTypeHandler
    {
        private final ColumnTypeHandler[] fieldHandlers;
        private final List<Optional<String>> fieldNames;

        private RowClientTypeHandler(ClientTypeSignature signature)
        {
            requireNonNull(signature, "signature is null");
            checkArgument(signature.getRawType().equals(ROW), "not a row type signature: %s", signature);
            fieldHandlers = new ColumnTypeHandler[signature.getArguments().size()];
            ImmutableList.Builder<Optional<String>> fieldNames = ImmutableList.builderWithExpectedSize(fieldHandlers.length);

            int index = 0;
            for (ClientTypeSignatureParameter parameter : signature.getArguments()) {
                checkArgument(
                        parameter.getKind() == ParameterKind.NAMED_TYPE,
                        "Unexpected parameter [%s] for row type",
                        parameter);
                NamedClientTypeSignature namedTypeSignature = parameter.getNamedTypeSignature();
                fieldHandlers[index] = createTypeHandler(namedTypeSignature.getTypeSignature());
                fieldNames.add(namedTypeSignature.getName());
                index++;
            }
            this.fieldNames = fieldNames.build();
        }

        @Override
        public Row fixValue(Object value)
        {
            List<?> listValue = (List<?>) value;
            checkArgument(listValue.size() == fieldHandlers.length, "Mismatched data values and row type");
            Row.Builder row = Row.builderWithExpectedSize(fieldHandlers.length);
            int field = 0;
            for (Object fieldValue : listValue) {
                if (fieldValue != null) {
                    fieldValue = fieldHandlers[field].fixValue(fieldValue);
                }
                row.addField(fieldNames.get(field), fieldValue);
                field++;
            }
            return row.build();
        }
    }

    /**
     * Force values coming from Jackson to have the expected object type.
     */
    private static ColumnTypeHandler createTypeHandler(ClientTypeSignature signature)
    {
        switch (signature.getRawType()) {
            case ARRAY:
                return new ArrayClientTypeHandler(signature);
            case MAP:
                return new MapClientTypeHandler(signature);
            case ROW:
                return new RowClientTypeHandler(signature);
            case BIGINT:
                return (value) -> {
                    if (value instanceof String) {
                        return Long.parseLong((String) value);
                    }
                    return ((Number) value).longValue();
                };

            case INTEGER:
                return (value) -> {
                    if (value instanceof String) {
                        return Integer.parseInt((String) value);
                    }
                    return ((Number) value).intValue();
                };

            case SMALLINT:
                return (value) -> {
                    if (value instanceof String) {
                        return Short.parseShort((String) value);
                    }
                    return ((Number) value).shortValue();
                };

            case TINYINT:
                return (value) -> {
                    if (value instanceof String) {
                        return Byte.parseByte((String) value);
                    }
                    return ((Number) value).byteValue();
                };

            case DOUBLE:
                return (value) -> {
                    if (value instanceof String) {
                        return Double.parseDouble((String) value);
                    }
                    return ((Number) value).doubleValue();
                };

            case REAL:
                return (value) -> {
                    if (value instanceof String) {
                        return Float.parseFloat((String) value);
                    }
                    return ((Number) value).floatValue();
                };

            case BOOLEAN:
                return (value) -> {
                    if (value instanceof String) {
                        return Boolean.parseBoolean((String) value);
                    }
                    return (Boolean) value;
                };

            case VARCHAR:
            case JSON:
            case TIME:
            case TIME_WITH_TIME_ZONE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIME_ZONE:
            case DATE:
            case INTERVAL_YEAR_TO_MONTH:
            case INTERVAL_DAY_TO_SECOND:
            case IPADDRESS:
            case UUID:
            case DECIMAL:
            case CHAR:
            case GEOMETRY:
            case SPHERICAL_GEOGRAPHY:
                return (value) -> (String) value;
            case BING_TILE:
                // Bing tiles are serialized as strings when used as map keys,
                // they are serialized as json otherwise (value will be a LinkedHashMap).
                return value -> value;
            default:
                // for now we assume that only the explicit types above are passed
                // as a plain text and everything else is base64 encoded binary
                return (value) -> {
                    if (value instanceof String) {
                        return Base64.getDecoder().decode((String) value);
                    }
                    return value;
                };
        }
    }
}
