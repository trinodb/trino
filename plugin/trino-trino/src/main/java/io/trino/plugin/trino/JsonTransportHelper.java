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
package io.trino.plugin.trino;

import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

final class JsonTransportHelper
{
    private final Function<String, String> quote;

    JsonTransportHelper(Function<String, String> quote)
    {
        this.quote = quote;
    }

    String buildJsonTransportExpression(String reference, Type type)
    {
        if (!(type instanceof ArrayType) && !(type instanceof MapType) && !(type instanceof RowType)) {
            return buildStringSurrogateExpression(reference, type);
        }

        if (type instanceof ArrayType arrayType) {
            return "transform(" + reference + ", x -> " + buildJsonTransportExpression("x", arrayType.getElementType()) + ")";
        }
        if (type instanceof MapType mapType) {
            if (usesJsonObjectKeyEncoding(mapType)) {
                return "map_from_entries(transform(map_entries(" + reference + "), entry -> CAST(ROW(" +
                        buildStringSurrogateExpression("entry[1]", mapType.getKeyType()) + ", " +
                        buildJsonTransportExpression("entry[2]", mapType.getValueType()) +
                        ") AS ROW(varchar, " + toSurrogateTypeSql(mapType.getValueType()) + "))))";
            }
            return "transform(map_entries(" + reference + "), entry -> CAST(ROW(" +
                    buildJsonTransportExpression("entry[1]", mapType.getKeyType()) + ", " +
                    buildJsonTransportExpression("entry[2]", mapType.getValueType()) +
                    ") AS ROW(key " + toSurrogateTypeSql(mapType.getKeyType()) + ", value " + toSurrogateTypeSql(mapType.getValueType()) + ")))";
        }
        RowType rowType = (RowType) type;
        List<String> fieldExpressions = new ArrayList<>(rowType.getFields().size());
        for (int index = 0; index < rowType.getFields().size(); index++) {
            RowType.Field field = rowType.getFields().get(index);
            Type fieldType = field.getType();
            String fieldReference = rowFieldReference(reference, field, index);
            if (fieldType instanceof ArrayType || fieldType instanceof MapType || fieldType instanceof RowType) {
                fieldExpressions.add("json_format(CAST(" + buildJsonTransportExpression(fieldReference, fieldType) + " AS JSON))");
            }
            else {
                fieldExpressions.add(buildStringSurrogateExpression(fieldReference, fieldType));
            }
        }
        return "CASE WHEN " + reference + " IS NULL THEN NULL ELSE ARRAY[" + String.join(", ", fieldExpressions) + "] END";
    }

    static boolean usesJsonObjectKeyEncoding(MapType mapType)
    {
        return mapType.getKeyType() instanceof VarcharType;
    }

    private String rowFieldReference(String reference, RowType.Field field, int index)
    {
        if (field.getName().isPresent()) {
            return "(" + reference + ")." + quote.apply(field.getName().orElseThrow());
        }
        return "(" + reference + ")[" + (index + 1) + "]";
    }

    private String buildStringSurrogateExpression(String reference, Type type)
    {
        if (type instanceof TimestampWithTimeZoneType) {
            return TimestampWithTimeZoneTransport.readExpression(reference);
        }
        if (TrinoTypeClassifier.isJsonType(type)) {
            return "json_format(" + reference + ")";
        }
        if (type instanceof VarbinaryType) {
            return "to_hex(" + reference + ")";
        }
        return "CAST(" + reference + " AS VARCHAR)";
    }

    private String toSurrogateTypeSql(Type type)
    {
        if (type instanceof ArrayType arrayType) {
            return "array(" + toSurrogateTypeSql(arrayType.getElementType()) + ")";
        }
        if (type instanceof MapType mapType) {
            if (usesJsonObjectKeyEncoding(mapType)) {
                return "map(varchar, " + toSurrogateTypeSql(mapType.getValueType()) + ")";
            }
            return "array(row(key " + toSurrogateTypeSql(mapType.getKeyType()) + ", value " + toSurrogateTypeSql(mapType.getValueType()) + "))";
        }
        if (type instanceof RowType) {
            return "array(varchar)";
        }
        return "varchar";
    }
}
