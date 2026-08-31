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
package io.trino.server;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.execution.CallProcedureRequest;
import io.trino.execution.CallProcedureResponse;
import io.trino.execution.ProcedureInvoker;
import io.trino.metadata.ProcedureRegistry;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.SessionPropertyManager;
import io.trino.security.AccessControl;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.procedure.Procedure;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.server.security.ResourceSecurity.AccessType.INTERNAL_ONLY;
import static io.trino.util.Failures.toFailure;
import static java.util.Objects.requireNonNull;

@Path("/v1/callProcedure")
@ResourceSecurity(INTERNAL_ONLY)
public class CallProcedureResource
{
    private final ProcedureRegistry procedureRegistry;
    private final ConnectorServicesProvider connectorServicesProvider;
    private final AccessControl accessControl;
    private final SessionPropertyManager sessionPropertyManager;

    @Inject
    public CallProcedureResource(
            ProcedureRegistry procedureRegistry,
            ConnectorServicesProvider connectorServicesProvider,
            AccessControl accessControl,
            SessionPropertyManager sessionPropertyManager)
    {
        this.procedureRegistry = requireNonNull(procedureRegistry, "procedureRegistry is null");
        this.connectorServicesProvider = requireNonNull(connectorServicesProvider, "connectorServicesProvider is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public CallProcedureResponse callProcedure(CallProcedureRequest request)
    {
        Procedure procedure;
        try {
            connectorServicesProvider.ensureCatalogsLoaded(ImmutableList.of(request.catalogProperties()));
            procedure = procedureRegistry.resolve(request.catalogHandle(), request.procedureName());
        }
        catch (IllegalArgumentException e) {
            // the target catalog failed to load, or was not loaded and ensureCatalogsLoaded
            // could not load it (e.g. a transient error); the coordinator can retry on a different node
            return CallProcedureResponse.catalogNotLoadedResponse();
        }

        try {
            Session session = request.session().toSession(sessionPropertyManager);
            QualifiedObjectName procedureName = new QualifiedObjectName(
                    request.catalogHandle().getCatalogName().toString(),
                    request.procedureName().getSchemaName(),
                    request.procedureName().getTableName());

            Object[] values = toNativeValues(request.argumentValues());
            Optional<Map<String, Long>> metrics = ProcedureInvoker.invoke(procedure, values, session, request.catalogHandle(), accessControl, procedureName);
            return CallProcedureResponse.success(metrics);
        }
        catch (RuntimeException e) {
            return CallProcedureResponse.failure(toFailure(e));
        }
    }

    private static Object[] toNativeValues(List<NullableValue> argumentValues)
    {
        Object[] values = new Object[argumentValues.size()];
        for (int i = 0; i < argumentValues.size(); i++) {
            values[i] = argumentValues.get(i).getValue();
        }
        return values;
    }
}
