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

import io.trino.Session;
import io.trino.connector.CatalogHandle;
import io.trino.metadata.QualifiedObjectName;
import io.trino.security.AccessControl;
import io.trino.security.InjectedConnectorAccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.trino.spi.StandardErrorCode.PROCEDURE_CALL_FAILED;
import static io.trino.spi.type.TypeUtils.writeNativeValue;

/**
 * Shared invocation logic used by {@link CallTask} (local, coordinator-side invocation) and the
 * worker-side {@code CallProcedureResource} (remote invocation for procedures that opt into
 * {@link Procedure#executesOnWorker()}).
 */
public final class ProcedureInvoker
{
    private ProcedureInvoker() {}

    /**
     * @param values procedure argument values in the procedure's declared argument order, in each
     *         argument type's <b>native</b> stack representation (not object-value representation)
     */
    public static Optional<Map<String, Long>> invoke(
            Procedure procedure,
            Object[] values,
            Session session,
            CatalogHandle catalogHandle,
            AccessControl accessControl,
            QualifiedObjectName procedureName)
    {
        List<Object> arguments = bindArguments(procedure, values, session, catalogHandle, accessControl, procedureName);

        try {
            Object result = procedure.getMethodHandle().invokeWithArguments(arguments);
            if (procedure.getMethodHandle().type().returnType() == Map.class && result != null) {
                @SuppressWarnings("unchecked")
                Map<String, Long> metrics = (Map<String, Long>) result;
                if (!metrics.isEmpty()) {
                    return Optional.of(metrics);
                }
            }
            return Optional.empty();
        }
        catch (Throwable t) {
            if (t instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throwIfInstanceOf(t, TrinoException.class);
            throw new TrinoException(PROCEDURE_CALL_FAILED, t);
        }
    }

    private static List<Object> bindArguments(
            Procedure procedure,
            Object[] values,
            Session session,
            CatalogHandle catalogHandle,
            AccessControl accessControl,
            QualifiedObjectName procedureName)
    {
        MethodType methodType = procedure.getMethodHandle().type();
        List<Object> arguments = new ArrayList<>();
        int argumentIndex = 0;
        for (Class<?> type : methodType.parameterList()) {
            if (ConnectorSession.class.equals(type)) {
                arguments.add(session.toConnectorSession(catalogHandle));
            }
            else if (ConnectorAccessControl.class.equals(type)) {
                arguments.add(new InjectedConnectorAccessControl(accessControl, session.toSecurityContext(), procedureName.catalogName()));
            }
            else {
                Type argumentType = procedure.getArguments().get(argumentIndex).getType();
                arguments.add(toTypeObjectValue(argumentType, values[argumentIndex]));
                argumentIndex++;
            }
        }
        return arguments;
    }

    private static Object toTypeObjectValue(Type type, Object value)
    {
        Block block = writeNativeValue(type, value);
        return type.getObjectValue(block, 0);
    }
}
