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
package io.trino.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.json.Json;
import io.trino.json.JsonItemBuilder;
import io.trino.json.JsonNestingDepthException;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.block.SqlRow;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.function.TypeVariableConstraint;
import io.trino.spi.type.RowType;
import io.trino.util.JsonUtil.JsonValueWriter;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;

import static io.trino.json.JsonItems.MAX_NESTING_DEPTH;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.TypeTemplates.typeVariable;
import static io.trino.type.JsonType.JSON;
import static io.trino.util.JsonUtil.JsonValueWriter.createJsonValueWriter;
import static io.trino.util.Reflection.methodHandle;

public class RowToJsonCast
        extends SqlScalarFunction
{
    public static final RowToJsonCast ROW_TO_JSON = new RowToJsonCast();

    private static final MethodHandle METHOD_HANDLE = methodHandle(RowToJsonCast.class, "toJsonObject", List.class, List.class, SqlRow.class);

    private RowToJsonCast()
    {
        super(FunctionMetadata.operatorBuilder(CAST)
                .signature(Signature.builder()
                        .typeVariableConstraint(
                                // this is technically a recursive constraint for cast, but TypeRegistry.canCast has explicit handling for row to json cast
                                TypeVariableConstraint.builder("T")
                                        .rowType()
                                        .build())
                        .returnType(JSON)
                        .argumentType(typeVariable("T"))
                        .build())
                .build());
    }

    @Override
    public SpecializedSqlScalarFunction specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        RowType type = (RowType) boundSignature.getArgumentType(0);

        List<RowType.Field> fields = type.getFields();

        List<JsonValueWriter> fieldWriters = new ArrayList<>(fields.size());
        List<String> fieldNames = new ArrayList<>(fields.size());

        for (RowType.Field field : fields) {
            fieldNames.add(field.getName().orElse(""));
            fieldWriters.add(createJsonValueWriter(field.getType()));
        }
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(fieldNames).bindTo(fieldWriters);

        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static Json toJsonObject(List<String> fieldNames, List<JsonValueWriter> fieldWriters, SqlRow sqlRow)
    {
        try {
            return JsonItemBuilder.encodeWithDepthLimit(jsonWriter -> {
                jsonWriter.startObject();
                for (int field = 0; field < sqlRow.getFieldCount(); field++) {
                    jsonWriter.fieldName(fieldNames.get(field));
                    fieldWriters.get(field).writeJsonValue(jsonWriter, sqlRow.getRawFieldBlock(field), sqlRow.getRawIndex());
                }
                jsonWriter.endObject();
            }, MAX_NESTING_DEPTH);
        }
        catch (JsonNestingDepthException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, e.getMessage(), e);
        }
    }
}
