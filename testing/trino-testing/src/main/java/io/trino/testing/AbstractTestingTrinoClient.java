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
package io.trino.testing;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.client.ClientSelectedRole;
import io.trino.client.ClientSession;
import io.trino.client.Column;
import io.trino.client.QueryStatusInfo;
import io.trino.client.StatementClient;
import io.trino.metadata.MetadataUtil;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.QueryId;
import io.trino.spi.security.SelectedRole;
import io.trino.spi.session.ResourceEstimates;
import io.trino.spi.type.Type;
import okhttp3.OkHttpClient;
import org.intellij.lang.annotations.Language;

import java.io.Closeable;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.client.StatementClientFactory.newStatementClient;
import static io.trino.spi.security.SelectedRole.Type.ROLE;
import static io.trino.spi.session.ResourceEstimates.CPU_TIME;
import static io.trino.spi.session.ResourceEstimates.EXECUTION_TIME;
import static io.trino.spi.session.ResourceEstimates.PEAK_MEMORY;
import static io.trino.transaction.TransactionBuilder.transaction;
import static java.util.Objects.requireNonNull;

public abstract class AbstractTestingTrinoClient<T>
        implements Closeable
{
    private final TestingTrinoServer trinoServer;
    private final Session defaultSession;
    private final OkHttpClient httpClient;

    protected AbstractTestingTrinoClient(TestingTrinoServer trinoServer, Session defaultSession)
    {
        this(trinoServer, defaultSession, new OkHttpClient());
    }

    protected AbstractTestingTrinoClient(TestingTrinoServer trinoServer, Session defaultSession, OkHttpClient httpClient)
    {
        this.trinoServer = requireNonNull(trinoServer, "trinoServer is null");
        this.defaultSession = requireNonNull(defaultSession, "defaultSession is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    @Override
    public void close()
    {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
    }

    protected abstract ResultsSession<T> getResultSession(Session session);

    public ResultWithQueryId<T> execute(@Language("SQL") String sql)
            throws QueryFailedException
    {
        return execute(defaultSession, sql);
    }

    public ResultWithQueryId<T> execute(Session session, @Language("SQL") String sql)
            throws QueryFailedException
    {
        ResultsSession<T> resultsSession = getResultSession(session);

        ClientSession clientSession = toClientSession(session, trinoServer.getBaseUrl(), new Duration(2, TimeUnit.MINUTES));

        try (StatementClient client = newStatementClient(httpClient, clientSession, sql, Optional.of(session.getClientCapabilities()))) {
            while (client.isRunning()) {
                resultsSession.addResults(client.currentStatusInfo(), client.currentData());
                client.advance();
            }

            checkState(client.isFinished());
            QueryStatusInfo results = client.finalStatusInfo();
            QueryId queryId = new QueryId(results.getId());

            if (results.getError() == null) {
                if (results.getUpdateType() != null) {
                    resultsSession.setUpdateType(results.getUpdateType());
                }
                if (results.getUpdateCount() != null) {
                    resultsSession.setUpdateCount(results.getUpdateCount());
                }

                resultsSession.setWarnings(results.getWarnings());
                resultsSession.setStatementStats(results.getStats());

                T result = resultsSession.build(client.getSetSessionProperties(), client.getResetSessionProperties());
                return new ResultWithQueryId<>(queryId, result);
            }

            if (results.getError().getFailureInfo() != null) {
                RuntimeException remoteException = results.getError().getFailureInfo().toException();
                throw new QueryFailedException(queryId, Optional.ofNullable(remoteException.getMessage()).orElseGet(remoteException::toString), remoteException);
            }
            throw new QueryFailedException(queryId, "Query failed: " + results.getError().getMessage());

            // dump query info to console for debugging (NOTE: not pretty printed)
            // JsonCodec<QueryInfo> queryInfoJsonCodec = createCodecFactory().prettyPrint().jsonCodec(QueryInfo.class);
            // log.info("\n" + queryInfoJsonCodec.toJson(queryInfo));
        }
    }

    private static ClientSession toClientSession(Session session, URI server, Duration clientRequestTimeout)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties.putAll(session.getSystemProperties());
        for (Entry<String, Map<String, String>> catalogAndConnectorProperties : session.getCatalogProperties().entrySet()) {
            for (Entry<String, String> connectorProperties : catalogAndConnectorProperties.getValue().entrySet()) {
                String catalogName = catalogAndConnectorProperties.getKey();
                properties.put(catalogName + "." + connectorProperties.getKey(), connectorProperties.getValue());
            }
        }

        ImmutableMap.Builder<String, String> resourceEstimates = ImmutableMap.builder();
        ResourceEstimates estimates = session.getResourceEstimates();
        estimates.getExecutionTime().ifPresent(e -> resourceEstimates.put(EXECUTION_TIME, e.toString()));
        estimates.getCpuTime().ifPresent(e -> resourceEstimates.put(CPU_TIME, e.toString()));
        estimates.getPeakMemoryBytes().ifPresent(e -> resourceEstimates.put(PEAK_MEMORY, e.toString()));

        return ClientSession.builder()
                .server(server)
                .principal(Optional.of(session.getIdentity().getUser()))
                .source(session.getSource().orElse(null))
                .traceToken(session.getTraceToken())
                .clientTags(session.getClientTags())
                .clientInfo(session.getClientInfo().orElse(null))
                .catalog(session.getCatalog().orElse(null))
                .schema(session.getSchema().orElse(null))
                .path(session.getPath().toString())
                .timeZone(session.getTimeZoneKey().getZoneId())
                .locale(session.getLocale())
                .resourceEstimates(resourceEstimates.buildOrThrow())
                .properties(properties.buildOrThrow())
                .preparedStatements(session.getPreparedStatements())
                .roles(getRoles(session))
                .credentials(session.getIdentity().getExtraCredentials())
                .transactionId(session.getTransactionId().map(Object::toString).orElse(null))
                .clientRequestTimeout(clientRequestTimeout)
                .compressionDisabled(true)
                .build();
    }

    private static Map<String, ClientSelectedRole> getRoles(Session session)
    {
        ImmutableMap.Builder<String, ClientSelectedRole> builder = ImmutableMap.builder();
        session.getIdentity().getEnabledRoles().forEach(role -> builder.put("system", toClientSelectedRole(new SelectedRole(ROLE, Optional.of(role)))));
        session.getIdentity().getCatalogRoles().forEach((key, value) -> builder.put(key, toClientSelectedRole(value)));
        return builder.buildOrThrow();
    }

    private static ClientSelectedRole toClientSelectedRole(io.trino.spi.security.SelectedRole value)
    {
        return new ClientSelectedRole(ClientSelectedRole.Type.valueOf(value.getType().toString()), value.getRole());
    }

    public List<QualifiedObjectName> listTables(Session session, String catalog, String schema)
    {
        return inTransaction(session, transactionSession ->
                trinoServer.getMetadata().listTables(transactionSession, new QualifiedTablePrefix(catalog, schema)));
    }

    public boolean tableExists(Session session, String table)
    {
        return inTransaction(session, transactionSession ->
                MetadataUtil.tableExists(trinoServer.getMetadata(), transactionSession, table));
    }

    private <V> V inTransaction(Session session, Function<Session, V> callback)
    {
        return transaction(trinoServer.getTransactionManager(), trinoServer.getAccessControl())
                .readOnly()
                .singleStatement()
                .execute(session, callback);
    }

    public Session getDefaultSession()
    {
        return defaultSession;
    }

    public TestingTrinoServer getServer()
    {
        return trinoServer;
    }

    protected List<Type> getTypes(List<Column> columns)
    {
        return columns.stream()
                .map(Column::getType)
                .map(trinoServer.getTypeManager()::fromSqlType)
                .collect(toImmutableList());
    }

    protected List<String> getNames(List<Column> columns)
    {
        return columns.stream()
                .map(Column::getName)
                .collect(toImmutableList());
    }
}
