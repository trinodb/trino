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

import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.sql.relational.CallExpression;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.trino.sql.relational.DeterminismEvaluator.isDeterministic;

public final class CallExpressionEvaluator
        implements ExpressionEvaluator
{
    public static Optional<Supplier<ExpressionEvaluator>> createCallExpressionEvaluator(ColumnarFilterCompiler compiler, CallExpression callExpression)
    {
        Optional<Supplier<ColumnarFilter>> compiledFilter = compiler.generateFilter(callExpression);
        return compiledFilter.map(filterSupplier -> () -> new CallExpressionEvaluator(filterSupplier.get(), callExpression));
    }

    private final ColumnFilterProcessor processor;

    private CallExpressionEvaluator(ColumnarFilter filter, CallExpression callExpression)
    {
        this.processor = new ColumnFilterProcessor(
                filter.getInputChannels().size() == 1 && isDeterministic(callExpression) ? new DictionaryAwareColumnarFilter(filter) : filter);
    }

    @Override
    public SelectedPositions evaluate(SelectedPositions activePositions, Page page, Consumer<Long> recordFilterTimeSince)
    {
        return processor.processFilter(activePositions, page, recordFilterTimeSince);
    }
}
