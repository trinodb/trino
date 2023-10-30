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
package io.trino.sql.gen.columnar;

import io.trino.metadata.ResolvedFunction;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.sql.relational.ConstantExpression;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.gen.columnar.AndExpressionEvaluator.createAndExpressionEvaluator;
import static io.trino.sql.relational.Expressions.call;
import static io.trino.sql.relational.SpecialForm.Form.AND;
import static io.trino.sql.relational.SpecialForm.Form.BETWEEN;

public final class BetweenExpressionEvaluator
        implements ExpressionEvaluator
{
    public static Optional<Supplier<ExpressionEvaluator>> createBetweenEvaluator(ColumnarFilterCompiler compiler, SpecialForm specialForm)
    {
        checkArgument(specialForm.getForm() == BETWEEN, "specialForm should be BETWEEN");
        checkArgument(specialForm.getArguments().size() == 3, "BETWEEN should have 3 arguments %s", specialForm.getArguments());
        checkArgument(specialForm.getFunctionDependencies().size() == 1, "BETWEEN should have 1 functional dependency %s", specialForm.getFunctionDependencies());

        ResolvedFunction lessThanOrEqual = specialForm.getOperatorDependency(LESS_THAN_OR_EQUAL);
        // Between requires evaluate once semantic for the value being tested
        // Until we can pre-project it into a temporary variable, we apply columnar evaluation only on InputReference
        RowExpression valueExpression = specialForm.getArguments().get(0);
        if (!(valueExpression instanceof InputReferenceExpression)) {
            return Optional.empty();
        }

        // When the min and max arguments of a BETWEEN expression are both constants, evaluating them inline is cheaper than AND-ing subexpressions
        if (specialForm.getArguments().get(1) instanceof ConstantExpression && specialForm.getArguments().get(2) instanceof ConstantExpression) {
            Optional<Supplier<ColumnarFilter>> compiledFilter = compiler.generateFilter(specialForm);
            return compiledFilter.map(filterSupplier -> () -> new BetweenExpressionEvaluator(filterSupplier.get()));
        }
        return createAndExpressionEvaluator(
                compiler,
                new SpecialForm(
                        AND,
                        BOOLEAN,
                        call(lessThanOrEqual, specialForm.getArguments().get(1), valueExpression),
                        call(lessThanOrEqual, valueExpression, specialForm.getArguments().get(2))));
    }

    private final ColumnFilterProcessor processor;

    private BetweenExpressionEvaluator(ColumnarFilter filter)
    {
        checkArgument(filter.getInputChannels().size() == 1, "filter should have 1 input channel");
        this.processor = new ColumnFilterProcessor(new DictionaryAwareColumnarFilter(filter));
    }

    @Override
    public SelectedPositions evaluate(SelectedPositions activePositions, Page page, Consumer<Long> recordFilterTimeSince)
    {
        return processor.processFilter(activePositions, page, recordFilterTimeSince);
    }
}
