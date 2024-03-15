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
package io.trino.plugin.deltalake.kernel.engine;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.engine.ExpressionHandler;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.expressions.PredicateEvaluator;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;
import io.trino.plugin.deltalake.kernel.data.TrinoColumnVectorWrapper;
import io.trino.plugin.deltalake.kernel.engine.expressions.TrinoExpressionEvaluator;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.TypeManager;

public class TrinoExpressionHandler
        implements ExpressionHandler
{
    private final TypeManager typeManager;

    public TrinoExpressionHandler(TypeManager typeManager)
    {
        this.typeManager = typeManager;
    }

    @Override
    public ExpressionEvaluator getEvaluator(StructType structType, Expression expression, DataType dataType)
    {
        return new TrinoExpressionEvaluator(typeManager, expression);
    }

    @Override
    public PredicateEvaluator getPredicateEvaluator(StructType structType, Predicate predicate)
    {
        return null;
    }

    @Override
    public ColumnVector createSelectionVector(boolean[] booleans, int i, int i1)
    {
        BlockBuilder blockBuilder = BooleanType.BOOLEAN.createBlockBuilder(null, booleans.length);
        for (boolean aBoolean : booleans) {
            BooleanType.BOOLEAN.writeBoolean(blockBuilder, aBoolean);
        }
        Block block = blockBuilder.build();
        return new TrinoColumnVectorWrapper(io.delta.kernel.types.BooleanType.BOOLEAN, block);
    }
}
