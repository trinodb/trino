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
import io.trino.spi.type.Type;
import io.trino.sql.relational.SpecialForm;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.relational.SpecialForm.Form.IS_NULL;
import static io.trino.type.UnknownType.UNKNOWN;

public final class IsNullExpressionEvaluator
        implements ExpressionEvaluator
{
    public static Optional<Supplier<ExpressionEvaluator>> createIsNullExpressionEvaluator(ColumnarFilterCompiler compiler, SpecialForm specialForm)
    {
        checkArgument(specialForm.getForm() == IS_NULL, "specialForm %s should be IS_NULL", specialForm);
        Optional<Supplier<ColumnarFilter>> compiledFilter = compiler.generateFilter(specialForm);
        return compiledFilter.map(filterSupplier -> () -> new IsNullExpressionEvaluator(filterSupplier.get(), specialForm));
    }

    private final ColumnFilterProcessor processor;
    private final Type argumentType;

    private IsNullExpressionEvaluator(ColumnarFilter filter, SpecialForm specialForm)
    {
        this.argumentType = specialForm.getArguments().get(0).getType();
        this.processor = new ColumnFilterProcessor(new DictionaryAwareColumnarFilter(filter));
    }

    @Override
    public SelectedPositions evaluate(SelectedPositions activePositions, Page page, Consumer<Long> recordFilterTimeSince)
    {
        if (argumentType.equals(UNKNOWN)) {
            return activePositions;
        }
        return processor.processFilter(activePositions, page, recordFilterTimeSince);
    }
}
