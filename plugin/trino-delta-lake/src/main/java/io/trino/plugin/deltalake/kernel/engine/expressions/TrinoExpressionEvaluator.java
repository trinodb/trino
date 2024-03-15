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
package io.trino.plugin.deltalake.kernel.engine.expressions;

import io.airlift.slice.Slices;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.trino.plugin.deltalake.kernel.KernelSchemaUtils;
import io.trino.plugin.deltalake.kernel.data.DataUtils;
import io.trino.plugin.deltalake.kernel.data.TrinoColumnVectorWrapper;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

public class TrinoExpressionEvaluator
        implements ExpressionEvaluator
{
    private final Expression expression;
    private final TypeManager typeManager;

    public TrinoExpressionEvaluator(TypeManager typeManager, Expression expression)
    {
        this.typeManager = typeManager;
        this.expression = expression;
    }

    @Override
    public ColumnVector eval(ColumnarBatch input)
    {
        if (expression instanceof Literal) {
            return evalLiteralExpression(input, (Literal) expression);
        }
        throw new UnsupportedOperationException(
                "unsupported expression encountered: " + expression);
    }

    @Override
    public void close()
            throws Exception
    {
        // nothing to close
    }

    private ColumnVector evalLiteralExpression(ColumnarBatch input, Literal literal)
    {
        DataType dataType = literal.getDataType();
        int size = input.getSize();

        if (literal.getValue() == null) {
            Block block = DataUtils.createBlockBuilder(typeManager, dataType, 1)
                    .appendNull()
                    .build();
            return new TrinoColumnVectorWrapper(dataType, block);
        }

        Object trinoValue;
        switch (dataType) {
            case BooleanType booleanType -> trinoValue = literal.getValue();
            case IntegerType integerType -> trinoValue = ((Integer) literal.getValue()).longValue();
            case LongType longType -> trinoValue = literal.getValue();
            case FloatType floatType -> trinoValue = ((Integer) Float.floatToIntBits((Float) literal.getValue())).longValue();
            case DoubleType doubleType -> trinoValue = Double.doubleToLongBits((Double) literal.getValue());
            case StringType stringType -> trinoValue = Slices.utf8Slice((String) literal.getValue());
            case BinaryType binaryType -> trinoValue = Slices.wrappedBuffer((byte[]) literal.getValue());
            default -> throw new IllegalStateException("Unexpected value: " + dataType);
        }

        Type type = KernelSchemaUtils.toTrinoType(
                new SchemaTableName("test", "test"),
                typeManager,
                dataType);
        Block block = RunLengthEncodedBlock.create(type, trinoValue, size);
        return new TrinoColumnVectorWrapper(dataType, block);
    }
}
