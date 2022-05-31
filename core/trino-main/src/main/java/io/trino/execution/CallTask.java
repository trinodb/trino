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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.ProcedureRegistry;
import io.trino.metadata.QualifiedObjectName;
import io.trino.security.AccessControl;
import io.trino.security.InjectedConnectorAccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.eventlistener.RoutineInfo;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.procedure.Procedure.Argument;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.ParameterRewriter;
import io.trino.sql.tree.Call;
import io.trino.sql.tree.CallArgument;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.transaction.TransactionManager;

import javax.inject.Inject;

import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Predicate;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.PROCEDURE_CALL_FAILED;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.sql.ParameterUtils.parameterExtractor;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.planner.ExpressionInterpreter.evaluateConstantExpression;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class CallTask
        implements DataDefinitionTask<Call>
{
    private final TransactionManager transactionManager;
    private final PlannerContext plannerContext;
    private final AccessControl accessControl;
    private final ProcedureRegistry procedureRegistry;

    @Inject
    public CallTask(TransactionManager transactionManager, PlannerContext plannerContext, AccessControl accessControl, ProcedureRegistry procedureRegistry)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.procedureRegistry = requireNonNull(procedureRegistry, "procedureRegistry is null");
    }

    @Override
    public String getName()
    {
        return "CALL";
    }

    @Override
    public ListenableFuture<Void> execute(
            Call call,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        if (!transactionManager.isAutoCommit(stateMachine.getSession().getRequiredTransactionId())) {
            throw new TrinoException(NOT_SUPPORTED, "Procedures cannot be called within a transaction (use autocommit mode)");
        }

        Session session = stateMachine.getSession();
        QualifiedObjectName procedureName = createQualifiedObjectName(session, call, call.getName());
        CatalogName catalogName = plannerContext.getMetadata().getCatalogHandle(stateMachine.getSession(), procedureName.getCatalogName())
                .orElseThrow(() -> semanticException(CATALOG_NOT_FOUND, call, "Catalog '%s' does not exist", procedureName.getCatalogName()));
        Procedure procedure = procedureRegistry.resolve(catalogName, procedureName.asSchemaTableName());

        // map declared argument names to positions
        Map<String, Integer> positions = new HashMap<>();
        for (int i = 0; i < procedure.getArguments().size(); i++) {
            positions.put(procedure.getArguments().get(i).getName(), i);
        }

        // per specification, do not allow mixing argument types
        Predicate<CallArgument> hasName = argument -> argument.getName().isPresent();
        boolean anyNamed = call.getArguments().stream().anyMatch(hasName);
        boolean allNamed = call.getArguments().stream().allMatch(hasName);
        if (anyNamed && !allNamed) {
            throw semanticException(INVALID_ARGUMENTS, call, "Named and positional arguments cannot be mixed");
        }

        // get the argument names in call order
        Map<String, CallArgument> names = new LinkedHashMap<>();
        for (int i = 0; i < call.getArguments().size(); i++) {
            CallArgument argument = call.getArguments().get(i);
            if (argument.getName().isPresent()) {
                String name = argument.getName().get().getCanonicalValue();
                if (names.put(name, argument) != null) {
                    throw semanticException(INVALID_ARGUMENTS, argument, "Duplicate procedure argument: %s", name);
                }
                if (!positions.containsKey(name)) {
                    throw semanticException(INVALID_ARGUMENTS, argument, "Unknown argument name: %s", name);
                }
            }
            else if (i < procedure.getArguments().size()) {
                names.put(procedure.getArguments().get(i).getName(), argument);
            }
            else {
                throw semanticException(INVALID_ARGUMENTS, call, "Too many arguments for procedure");
            }
        }

        procedure.getArguments().stream()
                .filter(Argument::isRequired)
                .filter(argument -> !names.containsKey(argument.getName()))
                .map(Argument::getName)
                .findFirst()
                .ifPresent(argument -> {
                    throw semanticException(INVALID_ARGUMENTS, call, "Required procedure argument '%s' is missing", argument);
                });

        // get argument values
        Object[] values = new Object[procedure.getArguments().size()];
        Map<NodeRef<Parameter>, Expression> parameterLookup = parameterExtractor(call, parameters);
        for (Entry<String, CallArgument> entry : names.entrySet()) {
            CallArgument callArgument = entry.getValue();
            int index = positions.get(entry.getKey());
            Argument argument = procedure.getArguments().get(index);

            Expression expression = ExpressionTreeRewriter.rewriteWith(new ParameterRewriter(parameterLookup), callArgument.getValue());

            Type type = argument.getType();
            Object value = evaluateConstantExpression(expression, type, plannerContext, session, accessControl, parameterLookup);

            values[index] = toTypeObjectValue(session, type, value);
        }

        // fill values with optional arguments defaults
        for (int i = 0; i < procedure.getArguments().size(); i++) {
            Argument argument = procedure.getArguments().get(i);

            if (!names.containsKey(argument.getName())) {
                verify(argument.isOptional());
                values[i] = toTypeObjectValue(session, argument.getType(), argument.getDefaultValue());
            }
        }

        // validate arguments
        MethodType methodType = procedure.getMethodHandle().type();
        for (int i = 0; i < procedure.getArguments().size(); i++) {
            if ((values[i] == null) && methodType.parameterType(i).isPrimitive()) {
                String name = procedure.getArguments().get(i).getName();
                throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Procedure argument cannot be null: " + name);
            }
        }

        // insert session argument
        List<Object> arguments = new ArrayList<>();
        Iterator<Object> valuesIterator = asList(values).iterator();
        for (Class<?> type : methodType.parameterList()) {
            if (ConnectorSession.class.equals(type)) {
                arguments.add(session.toConnectorSession(catalogName));
            }
            else if (ConnectorAccessControl.class.equals(type)) {
                arguments.add(new InjectedConnectorAccessControl(accessControl, session.toSecurityContext(), catalogName.getCatalogName()));
            }
            else {
                arguments.add(valuesIterator.next());
            }
        }

        accessControl.checkCanExecuteProcedure(session.toSecurityContext(), procedureName);
        stateMachine.setRoutines(ImmutableList.of(new RoutineInfo(procedureName.getObjectName(), session.getUser())));

        try {
            procedure.getMethodHandle().invokeWithArguments(arguments);
        }
        catch (Throwable t) {
            if (t instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throwIfInstanceOf(t, TrinoException.class);
            throw new TrinoException(PROCEDURE_CALL_FAILED, t);
        }

        return immediateVoidFuture();
    }

    private static Object toTypeObjectValue(Session session, Type type, Object value)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
        writeNativeValue(type, blockBuilder, value);
        return type.getObjectValue(session.toConnectorSession(), blockBuilder, 0);
    }
}
