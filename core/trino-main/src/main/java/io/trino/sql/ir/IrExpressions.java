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
package io.trino.sql.ir;

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.RowType;

import java.util.Arrays;
import java.util.Optional;

import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;

public class IrExpressions
{
    private IrExpressions() {}

    public static Expression ifExpression(Expression condition, Expression trueCase)
    {
        return new SearchedCaseExpression(ImmutableList.of(new WhenClause(condition, trueCase)), Optional.empty());
    }

    public static Expression ifExpression(Expression condition, Expression trueCase, Expression falseCase)
    {
        return new SearchedCaseExpression(ImmutableList.of(new WhenClause(condition, trueCase)), Optional.of(falseCase));
    }

    public static Constant row(Constant... values)
    {
        RowType type = RowType.anonymous(Arrays.stream(values).map(Constant::getType).toList());
        return new Constant(type, buildRowValue(type, fields -> {
            for (int i = 0; i < values.length; ++i) {
                writeNativeValue(values[i].getType(), fields.get(i), values[i].getValue());
            }
        }));
    }
}
