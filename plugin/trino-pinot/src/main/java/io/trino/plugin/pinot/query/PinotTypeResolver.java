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
package io.trino.plugin.pinot.query;

import io.trino.plugin.pinot.PinotColumnHandle;
import io.trino.plugin.pinot.PinotException;
import io.trino.plugin.pinot.client.PinotClient;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.operator.transform.function.LiteralTransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunctionFactory;
import org.apache.pinot.segment.local.segment.index.datasource.EmptyDataSource;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.spi.data.FieldSpec;

import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.pinot.PinotErrorCode.PINOT_INVALID_PQL_GENERATED;
import static io.trino.plugin.pinot.PinotErrorCode.PINOT_UNSUPPORTED_COLUMN_TYPE;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class PinotTypeResolver
{
    private final Map<String, DataSource> datasourceMap;

    public PinotTypeResolver(PinotClient pinotClient, String pinotTableName)
    {
        requireNonNull(pinotClient, "pinotClient is null");
        this.datasourceMap = getDataSourceMap(pinotClient, pinotTableName);
    }

    private static Map<String, DataSource> getDataSourceMap(PinotClient pinotClient, String pinotTableName)
    {
        try {
            return pinotClient.getTableSchema(pinotTableName).getFieldSpecMap().entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey,
                            entry -> new EmptyDataSource(new ColumnMetadataImpl.Builder()
                                    .setFieldSpec(entry.getValue())
                                    .build())));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public TransformResultMetadata resolveExpressionType(ExpressionContext expression, SchemaTableName schemaTableName, Map<String, ColumnHandle> columnHandles)
    {
        switch (expression.getType()) {
            case IDENTIFIER:
                PinotColumnHandle columnHandle = (PinotColumnHandle) columnHandles.get(expression.getIdentifier().toLowerCase(ENGLISH));
                if (columnHandle == null) {
                    throw new ColumnNotFoundException(schemaTableName, expression.getIdentifier());
                }
                return fromTrinoType(columnHandle.getDataType());
            case FUNCTION:
                return TransformFunctionFactory.get(expression, datasourceMap).getResultMetadata();
            case LITERAL:
                FieldSpec.DataType literalDataType = LiteralTransformFunction.inferLiteralDataType(new LiteralTransformFunction(expression.getLiteral()));
                return new TransformResultMetadata(literalDataType, true, false);
            default:
                throw new PinotException(PINOT_INVALID_PQL_GENERATED, Optional.empty(), format("Unsupported expression: '%s'", expression));
        }
    }

    public static TransformResultMetadata fromTrinoType(Type type)
    {
        if (type instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) type;
            Type elementType = arrayType.getElementType();
            return new TransformResultMetadata(fromPrimitiveTrinoType(elementType), false, false);
        }
        else {
            return new TransformResultMetadata(fromPrimitiveTrinoType(type), true, false);
        }
    }

    private static FieldSpec.DataType fromPrimitiveTrinoType(Type type)
    {
        if (type instanceof VarcharType) {
            return FieldSpec.DataType.STRING;
        }
        if (type instanceof BigintType) {
            return FieldSpec.DataType.LONG;
        }
        if (type instanceof IntegerType) {
            return FieldSpec.DataType.INT;
        }
        if (type instanceof DoubleType) {
            return FieldSpec.DataType.DOUBLE;
        }
        if (type instanceof RealType) {
            return FieldSpec.DataType.FLOAT;
        }
        if (type instanceof BooleanType) {
            return FieldSpec.DataType.BOOLEAN;
        }
        if (type instanceof VarbinaryType) {
            return FieldSpec.DataType.BYTES;
        }
        throw new PinotException(PINOT_UNSUPPORTED_COLUMN_TYPE, Optional.empty(), "Unsupported column data type: " + type);
    }
}
