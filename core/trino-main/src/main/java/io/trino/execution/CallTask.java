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
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.trino.Session;
import io.trino.connector.CatalogHandle;
import io.trino.execution.scheduler.NodeScheduler;
import io.trino.execution.scheduler.NodeSelector;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.ProcedureRegistry;
import io.trino.metadata.QualifiedObjectName;
import io.trino.node.InternalNode;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.eventlistener.RoutineInfo;
import io.trino.spi.predicate.NullableValue;
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

import java.lang.invoke.MethodType;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.execution.ParameterExtractor.bindParameters;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.metadata.MetadataUtil.getRequiredCatalogHandle;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static io.trino.spi.StandardErrorCode.PROCEDURE_CALL_FAILED;
import static io.trino.sql.analyzer.ConstantEvaluator.evaluateConstant;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

public class CallTask
        implements DataDefinitionTask<Call>
{
    private final TransactionManager transactionManager;
    private final PlannerContext plannerContext;
    private final AccessControl accessControl;
    private final ProcedureRegistry procedureRegistry;
    private final CatalogManager catalogManager;
    private final Provider<NodeScheduler> nodeScheduler;
    private final Provider<RemoteCallProcedureTask> remoteCallProcedureTask;

    @Inject
    public CallTask(
            TransactionManager transactionManager,
            PlannerContext plannerContext,
            AccessControl accessControl,
            ProcedureRegistry procedureRegistry,
            CatalogManager catalogManager,
            Provider<NodeScheduler> nodeScheduler,
            Provider<RemoteCallProcedureTask> remoteCallProcedureTask)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.procedureRegistry = requireNonNull(procedureRegistry, "procedureRegistry is null");
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
        this.remoteCallProcedureTask = requireNonNull(remoteCallProcedureTask, "remoteCallProcedureTask is null");
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
        if (!transactionManager.getTransactionInfo(stateMachine.getSession().getRequiredTransactionId()).isAutoCommitContext()) {
            throw new TrinoException(NOT_SUPPORTED, "Procedures cannot be called within a transaction (use autocommit mode)");
        }

        Session session = stateMachine.getSession();
        QualifiedObjectName procedureName = createQualifiedObjectName(session, call, call.getName());
        CatalogHandle catalogHandle = getRequiredCatalogHandle(plannerContext.getMetadata(), stateMachine.getSession(), call, procedureName.catalogName());
        Procedure procedure = procedureRegistry.resolve(catalogHandle, procedureName.asSchemaTableName());

        // map declared argument names to positions
        Map<String, Integer> positions = new HashMap<>();
        for (int i = 0; i < procedure.getArguments().size(); i++) {
            positions.put(procedure.getArguments().get(i).getName(), i);
        }

        // per specification, do not allow mixing argument types
        Predicate<CallArgument> hasName = argument -> argument.getName().isPresent();
        boolean anyNamed = call.getArguments().stream().anyMatch(hasName);
        boolean allNamed = call.getArguments().stream().allMatch(hasName);
        if (!allNamed && procedure.requiresNamedArguments()) {
            throw semanticException(INVALID_ARGUMENTS, call, "Only named arguments are allowed for this procedure");
        }
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
        Map<NodeRef<Parameter>, Expression> parameterLookup = bindParameters(call, parameters);
        for (Entry<String, CallArgument> entry : names.entrySet()) {
            CallArgument callArgument = entry.getValue();
            int index = positions.get(entry.getKey());
            Argument argument = procedure.getArguments().get(index);

            Expression expression = ExpressionTreeRewriter.rewriteWith(new ParameterRewriter(parameterLookup), callArgument.getValue());

            Type type = argument.getType();
            Object value = evaluateConstant(expression, type, parameterLookup, plannerContext, session, accessControl);

            values[index] = value;
        }

        // fill values with optional arguments defaults
        for (int i = 0; i < procedure.getArguments().size(); i++) {
            Argument argument = procedure.getArguments().get(i);

            if (!names.containsKey(argument.getName())) {
                verify(argument.isOptional());
                values[i] = argument.getDefaultValue();
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

        accessControl.checkCanExecuteProcedure(session.toSecurityContext(), procedureName);
        stateMachine.setRoutines(ImmutableList.of(new RoutineInfo(procedureName.objectName(), session.getUser())));

        if (procedure.executesOnWorker()) {
            return callOnWorker(procedure, values, session, catalogHandle, procedureName, stateMachine);
        }

        Optional<Map<String, Long>> metrics = ProcedureInvoker.invoke(procedure, values, session, catalogHandle, accessControl, procedureName);
        metrics.ifPresent(stateMachine::setCallResult);

        return immediateVoidFuture();
    }

    private ListenableFuture<Void> callOnWorker(
            Procedure procedure,
            Object[] values,
            Session session,
            CatalogHandle catalogHandle,
            QualifiedObjectName procedureName,
            QueryStateMachine stateMachine)
    {
        NodeSelector nodeSelector = nodeScheduler.get().createNodeSelector(session);
        int clusterSize = nodeSelector.allNodes().size();
        if (clusterSize == 0) {
            throw new TrinoException(NO_NODES_AVAILABLE, "No nodes available to run procedure " + procedureName);
        }
        List<InternalNode> nodes = nodeSelector.selectRandomNodes(clusterSize, ImmutableSet.of()).stream()
                .filter(node -> !node.isCoordinator())
                .collect(toImmutableList());
        if (nodes.isEmpty()) {
            throw new TrinoException(NO_NODES_AVAILABLE, "No worker nodes available to run procedure " + procedureName);
        }

        CatalogProperties catalogProperties = catalogManager.getCatalogProperties(catalogHandle)
                .orElseThrow(() -> new TrinoException(PROCEDURE_CALL_FAILED, "Catalog properties not found for " + catalogHandle));

        ImmutableList.Builder<NullableValue> argumentValues = ImmutableList.builderWithExpectedSize(values.length);
        for (int i = 0; i < values.length; i++) {
            argumentValues.add(new NullableValue(procedure.getArguments().get(i).getType(), values[i]));
        }

        CallProcedureRequest request = new CallProcedureRequest(
                catalogHandle,
                catalogProperties,
                procedureName.asSchemaTableName(),
                argumentValues.build(),
                session.toSessionRepresentation());

        SettableFuture<Void> result = SettableFuture.create();
        callOnNode(nodes.iterator(), request, procedureName, stateMachine, result);
        return result;
    }

    /**
     * Attempts the call on successive candidate nodes, retrying only when a node reports that it
     * has not yet loaded the target catalog (catalogs propagate to workers asynchronously, so this
     * is a transient, expected condition rather than a procedure failure). Any other failure fails
     * fast, since procedures are not assumed to be idempotent.
     */
    private void callOnNode(
            Iterator<InternalNode> candidateNodes,
            CallProcedureRequest request,
            QualifiedObjectName procedureName,
            QueryStateMachine stateMachine,
            SettableFuture<Void> result)
    {
        if (!candidateNodes.hasNext()) {
            result.setException(new TrinoException(PROCEDURE_CALL_FAILED, "No node has loaded catalog for procedure " + procedureName));
            return;
        }
        InternalNode node = candidateNodes.next();

        ListenableFuture<JsonResponse<CallProcedureResponse>> future = remoteCallProcedureTask.get().call(node, request);
        Futures.addCallback(future, new FutureCallback<>()
        {
            @Override
            public void onSuccess(JsonResponse<CallProcedureResponse> response)
            {
                try {
                    if (!response.hasValue()) {
                        result.setException(new TrinoException(PROCEDURE_CALL_FAILED, "Failed to call procedure " + procedureName + " on node " + node + ": HTTP " + response.getStatusCode()));
                        return;
                    }
                    CallProcedureResponse callProcedureResponse = response.getValue();
                    if (callProcedureResponse.catalogNotLoaded()) {
                        callOnNode(candidateNodes, request, procedureName, stateMachine, result);
                        return;
                    }
                    if (callProcedureResponse.failure().isPresent()) {
                        result.setException(callProcedureResponse.failure().get().toException());
                        return;
                    }
                    callProcedureResponse.metrics().ifPresent(stateMachine::setCallResult);
                    result.set(null);
                }
                catch (Throwable t) {
                    result.setException(t);
                }
            }

            @Override
            public void onFailure(Throwable t)
            {
                result.setException(new TrinoException(PROCEDURE_CALL_FAILED, "Failed to call procedure " + procedureName + " on node " + node, t));
            }
        }, directExecutor());
    }
}
