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
package io.trino.operator.table.json.execution;

import com.google.common.collect.ImmutableList;
import io.trino.json.JsonPathInvocationContext;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.operator.project.PageProjection;
import io.trino.operator.table.json.JsonTableColumn;
import io.trino.operator.table.json.JsonTableOrdinalityColumn;
import io.trino.operator.table.json.JsonTablePlanCross;
import io.trino.operator.table.json.JsonTablePlanLeaf;
import io.trino.operator.table.json.JsonTablePlanNode;
import io.trino.operator.table.json.JsonTablePlanSingle;
import io.trino.operator.table.json.JsonTablePlanUnion;
import io.trino.operator.table.json.JsonTableQueryColumn;
import io.trino.operator.table.json.JsonTableValueColumn;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.type.TypeManager;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.planner.Symbol;
import io.trino.type.FunctionType;

import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FUNCTION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;

public final class ExecutionPlanner
{
    private ExecutionPlanner() {}

    public static JsonTableProcessingFragment getExecutionPlan(
            JsonTablePlanNode plan,
            Object[] newRow,
            boolean errorOnError,
            ConnectorSession session,
            Metadata metadata,
            TypeManager typeManager,
            FunctionManager functionManager,
            PageFunctionCompiler pageFunctionCompiler)
    {
        switch (plan) {
            case JsonTablePlanLeaf planLeaf -> {
                return new FragmentLeaf(
                        planLeaf.path(),
                        planLeaf.columns().stream()
                                .map(column -> getColumn(column, session, functionManager, pageFunctionCompiler))
                                .collect(toImmutableList()),
                        errorOnError,
                        newRow,
                        session,
                        metadata,
                        typeManager,
                        functionManager);
            }
            case JsonTablePlanSingle planSingle -> {
                return new FragmentSingle(
                        planSingle.path(),
                        planSingle.columns().stream()
                                .map(column -> getColumn(column, session, functionManager, pageFunctionCompiler))
                                .collect(toImmutableList()),
                        errorOnError,
                        planSingle.outer(),
                        getExecutionPlan(planSingle.child(), newRow, errorOnError, session, metadata, typeManager, functionManager, pageFunctionCompiler),
                        newRow,
                        session,
                        metadata,
                        typeManager,
                        functionManager);
            }
            case JsonTablePlanCross planCross -> {
                return new FragmentCross(planCross.siblings().stream()
                        .map(sibling -> getExecutionPlan(sibling, newRow, errorOnError, session, metadata, typeManager, functionManager, pageFunctionCompiler))
                        .collect(toImmutableList()));
            }
            case JsonTablePlanUnion planUnion -> {
                return new FragmentUnion(
                        planUnion.siblings().stream()
                                .map(sibling -> getExecutionPlan(sibling, newRow, errorOnError, session, metadata, typeManager, functionManager, pageFunctionCompiler))
                                .collect(toImmutableList()),
                        newRow);
            }
        }
    }

    private static Column getColumn(JsonTableColumn column, ConnectorSession session, FunctionManager functionManager, PageFunctionCompiler pageFunctionCompiler)
    {
        return switch (column) {
            case JsonTableValueColumn valueColumn -> {
                ScalarFunctionImplementation implementation = functionManager.getScalarFunctionImplementation(
                        valueColumn.function(),
                        new InvocationConvention(
                                ImmutableList.of(BOXED_NULLABLE, BOXED_NULLABLE, BOXED_NULLABLE, BOXED_NULLABLE, NEVER_NULL, FUNCTION, NEVER_NULL, FUNCTION),
                                NULLABLE_RETURN,
                                true,
                                true));
                JsonPathInvocationContext context;
                checkArgument(implementation.getInstanceFactory().isPresent(), "instance factory is missing");
                try {
                    context = (JsonPathInvocationContext) implementation.getInstanceFactory().get().invoke();
                }
                catch (Throwable throwable) {
                    throwIfUnchecked(throwable);
                    throw new RuntimeException(throwable);
                }
                Map<Symbol, Integer> defaultInputLayout = valueColumn.defaultInputLayout().isEmpty()
                        ? Map.of()
                        : IntStream.range(0, valueColumn.defaultInputLayout().size())
                                .boxed()
                                .collect(toImmutableMap(valueColumn.defaultInputLayout()::get, i -> i));
                PageProjection emptyDefaultProjection = valueColumn.emptyDefault() == null ? null : pageFunctionCompiler.compileProjection(valueColumn.emptyDefault(), defaultInputLayout, Optional.empty()).get();
                PageProjection errorDefaultProjection = valueColumn.errorDefault() == null ? null : pageFunctionCompiler.compileProjection(valueColumn.errorDefault(), defaultInputLayout, Optional.empty()).get();
                yield new ValueColumn(
                        valueColumn.outputIndex(),
                        implementation.getMethodHandle()
                                .bindTo(context)
                                .bindTo(session),
                        session,
                        valueColumn.path(),
                        valueColumn.emptyBehavior(),
                        emptyDefaultProjection,
                        valueColumn.emptyDefault() == null ? ((FunctionType) valueColumn.function().signature().getArgumentType(5)).getReturnType() : valueColumn.emptyDefault().type(),
                        valueColumn.errorBehavior(),
                        errorDefaultProjection,
                        valueColumn.errorDefault() == null ? ((FunctionType) valueColumn.function().signature().getArgumentType(7)).getReturnType() : valueColumn.errorDefault().type());
            }
            case JsonTableQueryColumn queryColumn -> {
                ScalarFunctionImplementation implementation = functionManager.getScalarFunctionImplementation(
                        queryColumn.function(),
                        new InvocationConvention(
                                ImmutableList.of(BOXED_NULLABLE, BOXED_NULLABLE, BOXED_NULLABLE, NEVER_NULL, NEVER_NULL, NEVER_NULL),
                                NULLABLE_RETURN,
                                true,
                                true));
                JsonPathInvocationContext context;
                checkArgument(implementation.getInstanceFactory().isPresent(), "instance factory is missing");
                try {
                    context = (JsonPathInvocationContext) implementation.getInstanceFactory().get().invoke();
                }
                catch (Throwable throwable) {
                    throwIfUnchecked(throwable);
                    throw new RuntimeException(throwable);
                }
                yield new QueryColumn(
                        queryColumn.outputIndex(),
                        implementation.getMethodHandle()
                                .bindTo(context)
                                .bindTo(session),
                        queryColumn.path(),
                        queryColumn.wrapperBehavior(),
                        queryColumn.emptyBehavior(),
                        queryColumn.errorBehavior());
            }
            case JsonTableOrdinalityColumn(int outputIndex) -> new OrdinalityColumn(outputIndex);
        };
    }
}
