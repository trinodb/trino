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
package io.trino.lance.file.v2.metadata;

import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.lance.file.v2.metadata.LogicalType.LogicalTypeKind.STRUCT;

public final class LanceTypeUtil
{
    private LanceTypeUtil() {}

    public static class FieldVisitor<T>
    {
        public T struct(Field field, List<T> fieldResults)
        {
            return null;
        }

        public T list(Field field, T elementResult)
        {
            return null;
        }

        public T primitive(Field primitive)
        {
            return null;
        }
    }

    public static <T> T visit(Field field, FieldVisitor<T> visitor)
    {
        return switch (LogicalType.from(field.logicalType())) {
            case LogicalType.Int8Type _,
                 LogicalType.Int16Type _,
                 LogicalType.Int32Type _,
                 LogicalType.Int64Type _,
                 LogicalType.FloatType _,
                 LogicalType.DoubleType _,
                 LogicalType.StringType _,
                 LogicalType.BinaryType _,
                 LogicalType.DateType _ -> visitor.primitive(field);
            case LogicalType.StructType _ -> {
                List<T> results = new ArrayList<>(field.children().size());
                for (Field child : field.children()) {
                    results.add(visit(child, visitor));
                }
                yield visitor.struct(field, results);
            }
            case LogicalType.ListType _ -> {
                verify(field.children().size() == 1);
                T result = visit(field.children().get(0), visitor);
                yield visitor.list(field, result);
            }
            case LogicalType.FixedSizeListType _ -> throw new UnsupportedOperationException("FIXED LIST TYPES not yet supported");
        };
    }

    public static <T> T visit(List<Field> fields, FieldVisitor<T> visitor)
    {
        Field rootStruct = new Field("root_", -1, -1, STRUCT.name(), Map.of(), false, fields);
        return visit(rootStruct, visitor);
    }

    public static class FieldIdToColumnIndexVisitor
            extends FieldVisitor<Map<Integer, Integer>>
    {
        private int current;

        @Override
        public Map<Integer, Integer> primitive(Field primitive)
        {
            return ImmutableMap.of(primitive.id(), current++);
        }

        @Override
        public Map<Integer, Integer> struct(Field struct, List<Map<Integer, Integer>> fieldResults)
        {
            return fieldResults.stream().flatMap(result -> result.entrySet().stream()).collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        @Override
        public Map<Integer, Integer> list(Field field, Map<Integer, Integer> elementResult)
        {
            return elementResult;
        }
    }
}
