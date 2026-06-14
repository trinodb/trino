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
package io.trino.plugin.exasol;

import com.exasol.jdbc.EXAConnection;
import com.exasol.jdbc.EXADriver;
import com.google.inject.Inject;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.spi.connector.ConnectorSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;

public class ParallelConnectionFactory
{
    private static final Logger log = LoggerFactory.getLogger(ParallelConnectionFactory.class);

    private final CredentialProvider credentialProvider;
    private final ExasolTlsCertificateFingerprintProvider tlsCertificateFingerprintProvider;
    private final OpenTelemetry openTelemetry;

    @Inject
    public ParallelConnectionFactory(CredentialProvider credentialProvider, ExasolTlsCertificateFingerprintProvider tlsCertificateFingerprintProvider, OpenTelemetry openTelemetry)
    {
        this.credentialProvider = requireNonNull(credentialProvider);
        this.tlsCertificateFingerprintProvider = requireNonNull(tlsCertificateFingerprintProvider);
        this.openTelemetry = requireNonNull(openTelemetry);
    }

    public List<Connection> createConnections(ConnectorSession session, Connection connection)
    {
        try {
            EXAConnection exaConnection = connection.unwrap(EXAConnection.class);
            int maxWorkers = ExasolSessionProperties.getParallelImportWorkerCount(session);
            exaConnection.RequestParallelConnections(maxWorkers);
            String[] hosts = exaConnection.GetAvailableWorkerHosts();
            long token = exaConnection.GetWorkerToken();
            long sessionId = exaConnection.getSessionID();
            return createConnections(session, hosts, token, sessionId);
        }
        catch (SQLException e) {
            throw new RuntimeException("Error creating subconnections: " + e.getMessage(), e);
        }
    }

    private List<Connection> createConnections(ConnectorSession session, String[] hosts, long token, long sessionId)
    {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<CompletableFuture<Connection>> connectionFutures = IntStream.range(0, hosts.length)
                    .mapToObj(workerId -> new WorkerConnectionInfo(session, workerId, hosts[workerId], token, sessionId))
                    .map(connectionInfo -> CompletableFuture.supplyAsync(() -> createSubConnection(connectionInfo), executor))
                    .toList();

            List<Connection> connections = new ArrayList<>(hosts.length);
            try {
                for (CompletableFuture<Connection> connectionFuture : connectionFutures) {
                    connections.add(connectionFuture.get());
                }
                return List.copyOf(connections);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                closeConnections(connectionFutures);
                throw new RuntimeException("Interrupted while creating subconnections", e);
            }
            catch (ExecutionException e) {
                closeConnections(connectionFutures);
                throw new RuntimeException("Error creating subconnections", e.getCause());
            }
        }
    }

    private Connection createSubConnection(WorkerConnectionInfo connectionInfo)
    {
        log.info("Creating subconnection #{} to host {}...", connectionInfo.workerId(), connectionInfo.host());
        try {
            try (DriverConnectionFactory connectionFactory = buildConnectionFactory(connectionInfo)) {
                return connectionFactory.openConnection(connectionInfo.session());
            }
        }
        catch (SQLException e) {
            log.error("Failed to create subconnection #{} to {}: {}", connectionInfo.workerId(), connectionInfo.host(), e.getMessage(), e);
            throw new RuntimeException("Failed connecting to %s: %s".formatted(connectionInfo.host(), e.getMessage()), e);
        }
    }

    private DriverConnectionFactory buildConnectionFactory(WorkerConnectionInfo connectionInfo)
    {
        return DriverConnectionFactory.builder(
                        new EXADriver(),
                        connectionInfo.getJdbcUrl(),
                        credentialProvider)
                .setConnectionProperties(buildConnectionProperties(connectionInfo))
                .setOpenTelemetry(openTelemetry)
                .build();
    }

    private Properties buildConnectionProperties(WorkerConnectionInfo connectionInfo)
    {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("workertoken", String.valueOf(connectionInfo.token()));
        connectionProperties.setProperty("sessionid", String.valueOf(connectionInfo.sessionId()));
        connectionProperties.setProperty("autocommit", "0");
        connectionProperties.setProperty("fingerprint", tlsCertificateFingerprintProvider.getCertificateFingerprint());
        connectionProperties.setProperty("validateservercertificate", "1");

        return connectionProperties;
    }

    private void closeConnections(List<CompletableFuture<Connection>> connectionFutures)
    {
        for (CompletableFuture<Connection> connectionFuture : connectionFutures) {
            connectionFuture.cancel(true);
            if (!connectionFuture.isDone() || connectionFuture.isCompletedExceptionally() || connectionFuture.isCancelled()) {
                continue;
            }

            try {
                connectionFuture.get().close();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            catch (ExecutionException | SQLException e) {
                log.debug("Failed closing subconnection during cleanup", e);
            }
        }
    }

    private record WorkerConnectionInfo(ConnectorSession session, int workerId, String host, long token, long sessionId)
    {
        private String getJdbcUrl()
        {
            return "jdbc:exa-worker:" + host;
        }
    }
}
