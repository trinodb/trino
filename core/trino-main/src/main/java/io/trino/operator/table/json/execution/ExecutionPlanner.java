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
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;

public class ExecutionPlanner
{
    private ExecutionPlanner()
    {
    }

    public static JsonTableProcessingFragment getExecutionPlan(
            JsonTablePlanNode plan,
            Object[] newRow,
            boolean errorOnError,
            Type[] outputTypes,
            ConnectorSession session,
            Metadata metadata,
            TypeManager typeManager,
            FunctionManager functionManager)
    {
        switch (plan) {
            case JsonTablePlanLeaf planLeaf -> {
                return new FragmentLeaf(
                        planLeaf.path(),
                        planLeaf.columns().stream()
                                .map(column -> getColumn(column, outputTypes, session, functionManager))
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
                                .map(column -> getColumn(column, outputTypes, session, functionManager))
                                .collect(toImmutableList()),
                        errorOnError,
                        planSingle.outer(),
                        getExecutionPlan(planSingle.child(), newRow, errorOnError, outputTypes, session, metadata, typeManager, functionManager),
                        newRow,
                        session,
                        metadata,
                        typeManager,
                        functionManager);
            }
            case JsonTablePlanCross planCross -> {
                return new FragmentCross(planCross.siblings().stream()
                        .map(sibling -> getExecutionPlan(sibling, newRow, errorOnError, outputTypes, session, metadata, typeManager, functionManager))
                        .collect(toImmutableList()));
            }
            case JsonTablePlanUnion planUnion -> {
                return new FragmentUnion(
                        planUnion.siblings().stream()
                                .map(sibling -> getExecutionPlan(sibling, newRow, errorOnError, outputTypes, session, metadata, typeManager, functionManager))
                                .collect(toImmutableList()),
                        newRow);
            }
        }
    }

    private static Column getColumn(JsonTableColumn column, Type[] outputTypes, ConnectorSession session, FunctionManager functionManager)
    {
        return switch (column) {
            case JsonTableValueColumn valueColumn -> {
                ScalarFunctionImplementation implementation = functionManager.getScalarFunctionImplementation(
                        valueColumn.function(),
                        new InvocationConvention(
                                ImmutableList.of(BOXED_NULLABLE, BOXED_NULLABLE, BOXED_NULLABLE, NEVER_NULL, BOXED_NULLABLE, NEVER_NULL, BOXED_NULLABLE),
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
                yield new ValueColumn(
                        valueColumn.outputIndex(),
                        implementation.getMethodHandle()
                                .bindTo(context)
                                .bindTo(session),
                        valueColumn.path(),
                        valueColumn.emptyBehavior(),
                        valueColumn.emptyDefaultInput(),
                        valueColumn.errorBehavior(),
                        valueColumn.errorDefaultInput(),
                        outputTypes[valueColumn.outputIndex()]);
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
