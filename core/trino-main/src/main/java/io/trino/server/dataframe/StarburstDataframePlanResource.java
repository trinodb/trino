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
package io.trino.server.dataframe;

import com.google.inject.Inject;
import com.starburstdata.dataframe.DataframeException;
import com.starburstdata.dataframe.analyzer.Analyzer;
import com.starburstdata.dataframe.analyzer.AnalyzerFactory;
import com.starburstdata.dataframe.analyzer.TrinoMetadata;
import com.starburstdata.dataframe.plan.LogicalPlan;
import com.starburstdata.dataframe.plan.TrinoPlan;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.trino.Session;
import io.trino.dispatcher.DispatchManager;
import io.trino.metadata.LanguageFunctionManager;
import io.trino.server.HttpRequestSessionContextFactory;
import io.trino.server.SessionContext;
import io.trino.server.SessionSupplier;
import io.trino.server.security.InternalPrincipal;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MultivaluedMap;

import java.util.Optional;

import static com.starburstdata.dataframe.DataframeException.ErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.server.ServletSecurityUtils.authenticatedIdentity;
import static io.trino.server.security.ResourceSecurity.AccessType.AUTHENTICATED_USER;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;

@Path("/v1/dataframe/plan")
public class StarburstDataframePlanResource
{
    private final TestingTrinoMetadataFactory testingTrinoMetadataFactory;
    private final AnalyzerFactory analyzerFactory;
    private final HttpRequestSessionContextFactory sessionContextFactory;
    private final DispatchManager dispatchManager;
    private final TransactionManager transactionManager;
    private final SessionSupplier sessionSupplier;
    private final Tracer tracer;
    private final LanguageFunctionManager languageFunctionManager;

    @Inject
    public StarburstDataframePlanResource(TestingTrinoMetadataFactory testingTrinoMetadataFactory,
             AnalyzerFactory analyzerFactory,
             HttpRequestSessionContextFactory sessionContextFactory,
             DispatchManager dispatchManager,
             TransactionManager transactionManager,
             SessionSupplier sessionSupplier,
             Tracer tracer,
             LanguageFunctionManager languageFunctionManager)
    {
        this.testingTrinoMetadataFactory = requireNonNull(testingTrinoMetadataFactory, "testingTrinoMetadataFactory is null");
        this.analyzerFactory = requireNonNull(analyzerFactory, "analyzerFactory is null");
        this.sessionContextFactory = requireNonNull(sessionContextFactory, "sessionContextFactory is null");
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.sessionSupplier = requireNonNull(sessionSupplier, "sessionSupplier is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.languageFunctionManager = requireNonNull(languageFunctionManager, "languageFunctionManager is null");
    }

    @POST
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    @ResourceSecurity(AUTHENTICATED_USER)
    public TrinoPlan createTrinoPlan(
            LogicalPlan plan,
            @Context HttpServletRequest servletRequest,
            @Context HttpHeaders httpHeaders)
    {
        Optional<String> remoteAddress = Optional.ofNullable(servletRequest.getRemoteAddr());
        Optional<Identity> identity = authenticatedIdentity(servletRequest);
        if (identity.flatMap(Identity::getPrincipal).map(InternalPrincipal.class::isInstance).orElse(false)) {
            throw new DataframeException("Internal communication cannot be used to start a query", GENERIC_INTERNAL_ERROR);
        }
        MultivaluedMap<String, String> headers = httpHeaders.getRequestHeaders();
        Session session = initializeSession(headers, remoteAddress, identity);
        languageFunctionManager.registerQuery(session);
        TrinoMetadata trinoMetadata = testingTrinoMetadataFactory.create(session);
        Analyzer analyzer = analyzerFactory.create(trinoMetadata);
        return analyzer.resolve(plan);
    }

    private Session initializeSession(MultivaluedMap<String, String> headers, Optional<String> remoteAddress, Optional<Identity> identity)
    {
        SessionContext sessionContext = sessionContextFactory.createSessionContext(headers, remoteAddress, identity);
        QueryId queryId = dispatchManager.createQueryId();
        TransactionId transactionId = transactionManager.beginTransaction(true);
        Span span = tracer.spanBuilder("starburst-dataframe-api-session").startSpan();
        Session session = sessionSupplier.createSession(queryId, span, sessionContext);
        return Session.builder(session).setTransactionId(transactionId).build();
    }
}
