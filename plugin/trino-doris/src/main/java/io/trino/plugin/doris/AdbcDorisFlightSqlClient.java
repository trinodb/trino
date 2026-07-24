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
package io.trino.plugin.doris;

import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static com.google.common.base.Throwables.getCausalChain;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class AdbcDorisFlightSqlClient
        implements DorisFlightSqlClient
{
    private static final String APPLICATION_NAME_PREFIX = "/* ApplicationName=Trino Doris Flight SQL Query */ ";

    private final DorisQueryBuilder queryBuilder;
    private final DorisFlightSqlPortResolver portResolver;
    private final FlightSqlStreamOpenerFactory streamOpenerFactory;
    private final Supplier<List<String>> feHostsSupplier;
    private volatile List<String> cachedFeHosts;
    private volatile String preferredFeHost;

    @Inject
    public AdbcDorisFlightSqlClient(DorisConfig config, DorisQueryBuilder queryBuilder, DorisFlightSqlPortResolver portResolver)
    {
        this(queryBuilder,
                portResolver,
                new AdbcFlightSqlStreamOpenerFactory(config),
                () -> DorisFeEndpoints.getHosts(config));
    }

    AdbcDorisFlightSqlClient(
            DorisQueryBuilder queryBuilder,
            DorisFlightSqlPortResolver portResolver,
            FlightSqlStreamOpenerFactory streamOpenerFactory,
            Supplier<List<String>> feHostsSupplier)
    {
        this.queryBuilder = requireNonNull(queryBuilder, "queryBuilder is null");
        this.portResolver = requireNonNull(portResolver, "portResolver is null");
        this.streamOpenerFactory = requireNonNull(streamOpenerFactory, "streamOpenerFactory is null");
        this.feHostsSupplier = requireNonNull(feHostsSupplier, "feHostsSupplier is null");
    }

    @Override
    public DorisFlightSqlResult openStream(DorisTableHandle tableHandle, DorisSplit split, List<DorisColumnHandle> columns)
    {
        return openStream(null, tableHandle, split, columns);
    }

    @Override
    public DorisFlightSqlResult openStream(ConnectorSession session, DorisTableHandle tableHandle, DorisSplit split, List<DorisColumnHandle> columns)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(split, "split is null");
        requireNonNull(columns, "columns is null");

        int flightSqlPort = portResolver.resolveFlightSqlPort(session);
        String sql = APPLICATION_NAME_PREFIX + queryBuilder.buildSelectSql(
                tableHandle,
                columns.stream()
                        .map(DorisColumnHandle::columnName)
                        .toList(),
                split.tabletIds());

        List<String> failures = new ArrayList<>();
        for (String feHost : prioritizedFeHosts()) {
            try {
                DorisFlightSqlResult result = openStream(feHost, flightSqlPort, sql);
                preferredFeHost = feHost;
                return result;
            }
            catch (RuntimeException e) {
                failures.add("%s:%s -> %s".formatted(feHost, flightSqlPort, exceptionMessage(e)));
            }
        }

        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to open Doris Flight SQL stream: " + failures);
    }

    private static String exceptionMessage(Throwable throwable)
    {
        String message = throwable.getMessage();
        Throwable rootCause = getCausalChain(throwable).getLast();
        String rootCauseMessage = rootCause.getMessage();
        if (rootCauseMessage == null || rootCauseMessage.isBlank() || rootCauseMessage.equals(message) || (message != null && message.contains(rootCauseMessage))) {
            return message;
        }
        if (message == null || message.isBlank()) {
            return rootCauseMessage;
        }
        return "%s: %s".formatted(message, rootCauseMessage);
    }

    private List<String> prioritizedFeHosts()
    {
        List<String> hosts = cachedFeHosts;
        if (hosts == null) {
            hosts = feHostsSupplier.get();
            cachedFeHosts = hosts;
        }

        String preferred = preferredFeHost;
        if (preferred == null || !hosts.contains(preferred)) {
            return hosts;
        }

        List<String> prioritized = new ArrayList<>(hosts.size());
        prioritized.add(preferred);
        for (String host : hosts) {
            if (!host.equals(preferred)) {
                prioritized.add(host);
            }
        }
        return List.copyOf(prioritized);
    }

    private DorisFlightSqlResult openStream(String feHost, int flightSqlPort, String sql)
    {
        FlightSqlStreamOpener opener = streamOpenerFactory.create(feHost, flightSqlPort);

        try {
            return new ManagedDorisFlightSqlResult(opener.openStream(sql), opener::close);
        }
        catch (RuntimeException e) {
            closeQuietly(opener);
            throw e;
        }
    }

    private static void closeQuietly(AutoCloseable closeable)
    {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        }
        catch (Exception ignored) {
        }
    }

    private static Exception closeResource(Exception failure, AutoCloseable closeable)
    {
        try {
            closeable.close();
        }
        catch (Exception e) {
            if (failure == null) {
                return e;
            }
            failure.addSuppressed(e);
        }
        return failure;
    }

    interface FlightSqlStreamOpenerFactory
    {
        FlightSqlStreamOpener create(String feHost, int flightSqlPort);
    }

    interface FlightSqlStreamOpener
            extends AutoCloseable
    {
        DorisFlightSqlResult openStream(String sql);

        long getMemoryUsage();

        @Override
        void close();
    }

    private static final class ManagedDorisFlightSqlResult
            implements DorisFlightSqlResult
    {
        private final DorisFlightSqlResult delegate;
        private final Runnable afterClose;
        private final AtomicBoolean closed = new AtomicBoolean();

        private ManagedDorisFlightSqlResult(DorisFlightSqlResult delegate, Runnable afterClose)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.afterClose = requireNonNull(afterClose, "afterClose is null");
        }

        @Override
        public boolean loadNextBatch()
        {
            return delegate.loadNextBatch();
        }

        @Override
        public VectorSchemaRoot getVectorSchemaRoot()
        {
            return delegate.getVectorSchemaRoot();
        }

        @Override
        public long getMemoryUsage()
        {
            try {
                return delegate.getMemoryUsage();
            }
            catch (RuntimeException ignored) {
                return 0;
            }
        }

        @Override
        public void close()
        {
            if (!closed.compareAndSet(false, true)) {
                return;
            }

            RuntimeException failure = null;
            try {
                delegate.close();
            }
            catch (RuntimeException e) {
                failure = e;
            }

            try {
                afterClose.run();
            }
            catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                }
                else {
                    failure.addSuppressed(e);
                }
            }

            if (failure != null) {
                throw failure;
            }
        }
    }

    private static final class AdbcFlightSqlStreamOpenerFactory
            implements FlightSqlStreamOpenerFactory
    {
        private final DorisConfig config;

        private AdbcFlightSqlStreamOpenerFactory(DorisConfig config)
        {
            this.config = requireNonNull(config, "config is null");
        }

        @Override
        public FlightSqlStreamOpener create(String feHost, int flightSqlPort)
        {
            return new AdbcFlightSqlStreamOpener(config, feHost, flightSqlPort);
        }
    }

    private static final class AdbcFlightSqlStreamOpener
            implements FlightSqlStreamOpener
    {
        private final BufferAllocator allocator;
        private final AdbcDatabase database;
        private final AdbcConnection connection;
        private final AtomicBoolean closed = new AtomicBoolean();

        private AdbcFlightSqlStreamOpener(DorisConfig config, String feHost, int flightSqlPort)
        {
            requireNonNull(config, "config is null");

            BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            AdbcDatabase database = null;
            AdbcConnection connection = null;

            try {
                FlightSqlDriver driver = new FlightSqlDriver(allocator);
                Map<String, Object> parameters = new HashMap<>();
                AdbcDriver.PARAM_URI.set(parameters, Location.forGrpcInsecure(feHost, flightSqlPort).getUri().toString());
                config.getUsername().ifPresent(value -> AdbcDriver.PARAM_USERNAME.set(parameters, value));
                config.getPassword().ifPresent(value -> AdbcDriver.PARAM_PASSWORD.set(parameters, value));

                database = driver.open(parameters);
                connection = database.connect();

                this.allocator = allocator;
                this.database = database;
                this.connection = connection;
            }
            catch (AdbcException | RuntimeException e) {
                closeQuietly(connection);
                closeQuietly(database);
                closeQuietly(allocator);
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to initialize Doris Flight SQL executor: " + exceptionMessage(e), e);
            }
        }

        @Override
        public DorisFlightSqlResult openStream(String sql)
        {
            if (closed.get()) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Doris Flight SQL executor is closed");
            }

            AdbcStatement statement = null;
            AdbcStatement.QueryResult queryResult = null;

            try {
                statement = connection.createStatement();
                statement.setSqlQuery(sql);
                queryResult = statement.executeQuery();
                ArrowReader reader = queryResult.getReader();
                return new AdbcStreamResult(this, statement, queryResult, reader);
            }
            catch (AdbcException | RuntimeException e) {
                closeQuietly(queryResult);
                closeQuietly(statement);
                close();
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to execute Doris Flight SQL query: " + exceptionMessage(e), e);
            }
        }

        @Override
        public long getMemoryUsage()
        {
            if (closed.get()) {
                return 0;
            }

            try {
                return allocator.getAllocatedMemory();
            }
            catch (RuntimeException ignored) {
                return 0;
            }
        }

        @Override
        public void close()
        {
            if (!closed.compareAndSet(false, true)) {
                return;
            }

            Exception failure = null;
            failure = closeResource(failure, connection);
            failure = closeResource(failure, database);
            failure = closeResource(failure, allocator);
            if (failure != null) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to close Doris Flight SQL executor", failure);
            }
        }
    }

    private static final class AdbcStreamResult
            implements DorisFlightSqlResult
    {
        private final FlightSqlStreamOpener opener;
        private final AdbcStatement statement;
        private final AdbcStatement.QueryResult queryResult;
        private final ArrowReader reader;

        private AdbcStreamResult(
                FlightSqlStreamOpener opener,
                AdbcStatement statement,
                AdbcStatement.QueryResult queryResult,
                ArrowReader reader)
        {
            this.opener = requireNonNull(opener, "opener is null");
            this.statement = requireNonNull(statement, "statement is null");
            this.queryResult = requireNonNull(queryResult, "queryResult is null");
            this.reader = requireNonNull(reader, "reader is null");
        }

        @Override
        public boolean loadNextBatch()
        {
            try {
                return reader.loadNextBatch();
            }
            catch (Exception e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to load Doris Flight SQL batch", e);
            }
        }

        @Override
        public VectorSchemaRoot getVectorSchemaRoot()
        {
            try {
                return reader.getVectorSchemaRoot();
            }
            catch (Exception e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to access Doris Flight SQL batch schema root", e);
            }
        }

        @Override
        public long getMemoryUsage()
        {
            return opener.getMemoryUsage();
        }

        @Override
        public void close()
        {
            Exception failure = null;
            failure = close(failure, reader);
            failure = close(failure, queryResult);
            failure = close(failure, statement);

            if (failure != null) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to close Doris Flight SQL resources", failure);
            }
        }

        private static Exception close(Exception failure, AutoCloseable closeable)
        {
            try {
                closeable.close();
            }
            catch (Exception e) {
                if (failure == null) {
                    return e;
                }
                failure.addSuppressed(e);
            }
            return failure;
        }
    }
}
