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
import io.trino.sql.relational.SpecialForm;

import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.relational.SpecialForm.Form.IN;

public final class InExpressionEvaluator
        implements ExpressionEvaluator
{
    public static Optional<Supplier<ExpressionEvaluator>> createInExpressionEvaluator(ColumnarFilterCompiler compiler, SpecialForm specialForm)
    {
        checkArgument(specialForm.getForm() == IN, "specialForm %s should be IN", specialForm);
        Optional<Supplier<ColumnarFilter>> compiledFilter = compiler.generateFilter(specialForm);
        return compiledFilter.map(filterSupplier -> () -> new InExpressionEvaluator(filterSupplier.get()));
    }

    private final ColumnFilterProcessor processor;

    private InExpressionEvaluator(ColumnarFilter filter)
    {
        checkArgument(filter.getInputChannels().size() == 1);
        this.processor = new ColumnFilterProcessor(new DictionaryAwareColumnarFilter(filter));
    }

    @Override
    public SelectedPositions evaluate(SelectedPositions activePositions, Page page)
    {
        return processor.processFilter(activePositions, page);
    }
}
