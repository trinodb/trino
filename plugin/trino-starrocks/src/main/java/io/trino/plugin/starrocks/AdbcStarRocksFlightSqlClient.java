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
package io.trino.plugin.starrocks;

import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.core.PartitionDescriptor;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlConnectionProperties;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class AdbcStarRocksFlightSqlClient
        implements StarRocksFlightSqlClient
{
    private static final String APPLICATION_NAME_PREFIX = "/* ApplicationName=Trino StarRocks Flight SQL Query */ ";

    private final String flightSqlHost;
    private final int flightSqlPort;
    private final boolean flightSqlTlsEnabled;
    private final Optional<File> flightSqlTlsRootCertificate;
    private final boolean flightSqlTlsSkipVerify;
    private final Optional<String> flightSqlTlsOverrideHostname;
    private final Optional<String> username;
    private final Optional<String> password;
    private final long flightSqlMaxAllocationBytes;
    private final StarRocksQueryBuilder queryBuilder;

    @Inject
    public AdbcStarRocksFlightSqlClient(StarRocksConfig config, StarRocksQueryBuilder queryBuilder)
    {
        requireNonNull(config, "config is null");
        this.flightSqlHost = config.getFlightSqlHost()
                .orElseGet(() -> inferFlightSqlHost(config.getJdbcUrl()
                        .orElseThrow(() -> new IllegalArgumentException("starrocks.jdbc-url must be set"))));
        this.flightSqlPort = config.getFlightSqlPort();
        this.flightSqlTlsEnabled = config.isFlightSqlTlsEnabled();
        this.flightSqlTlsRootCertificate = config.getFlightSqlTlsRootCertificate();
        this.flightSqlTlsSkipVerify = config.isFlightSqlTlsSkipVerify();
        this.flightSqlTlsOverrideHostname = config.getFlightSqlTlsOverrideHostname();
        this.username = config.getUsername();
        this.password = config.getPassword();
        this.flightSqlMaxAllocationBytes = config.getFlightSqlMaxAllocation().toBytes();
        this.queryBuilder = requireNonNull(queryBuilder, "queryBuilder is null");
    }

    @Override
    public List<StarRocksSplit> getSplits(
            ConnectorSession session,
            StarRocksTableHandle tableHandle,
            List<StarRocksColumnHandle> columns)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(columns, "columns is null");

        if (requiresSingleSplit(tableHandle)) {
            return List.of(new StarRocksSplit());
        }

        String sql = APPLICATION_NAME_PREFIX + queryBuilder.buildSelectSql(tableHandle, columns);
        try (AdbcResources resources = openResources();
                AdbcStatement statement = resources.connection().createStatement()) {
            statement.setSqlQuery(sql);
            List<StarRocksSplit> splits = new ArrayList<>();
            Iterable<PartitionDescriptor> partitionDescriptors;
            try {
                partitionDescriptors = statement.executePartitioned().getPartitionDescriptors();
            }
            catch (AdbcException e) {
                if (!shouldFallbackToSingleSplit(e)) {
                    throw e;
                }
                return List.of(new StarRocksSplit());
            }
            catch (UnsupportedOperationException e) {
                return List.of(new StarRocksSplit());
            }
            for (PartitionDescriptor partitionDescriptor : partitionDescriptors) {
                ByteBuffer descriptor = partitionDescriptor.getDescriptor();
                byte[] bytes = new byte[descriptor.remaining()];
                descriptor.get(bytes);
                splits.add(new StarRocksSplit(Optional.of(bytes)));
            }
            if (splits.isEmpty()) {
                return List.of(new StarRocksSplit());
            }
            return List.copyOf(splits);
        }
        catch (AdbcException | RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to plan StarRocks Flight SQL splits", e);
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to close StarRocks Flight SQL split planning resources", e);
        }
    }

    @Override
    public StarRocksFlightSqlResult openStream(
            ConnectorSession session,
            StarRocksTableHandle tableHandle,
            StarRocksSplit split,
            List<StarRocksColumnHandle> columns)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(split, "split is null");
        requireNonNull(columns, "columns is null");

        if (split.partitionDescriptor().isPresent()) {
            return openPartition(split.partitionDescriptor().orElseThrow());
        }
        return openQuery(APPLICATION_NAME_PREFIX + queryBuilder.buildSelectSql(tableHandle, columns));
    }

    private StarRocksFlightSqlResult openQuery(String sql)
    {
        AdbcResources resources = null;
        AdbcStatement statement = null;
        AdbcStatement.QueryResult queryResult = null;
        ArrowReader reader = null;

        try {
            resources = openResources();
            statement = resources.connection().createStatement();
            statement.setSqlQuery(sql);
            queryResult = statement.executeQuery();
            reader = queryResult.getReader();
            return new AdbcResult(resources, statement, queryResult, reader);
        }
        catch (AdbcException | RuntimeException e) {
            closeQuietly(reader);
            closeQuietly(queryResult);
            closeQuietly(statement);
            closeQuietly(resources);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to execute StarRocks Flight SQL query", e);
        }
    }

    private StarRocksFlightSqlResult openPartition(byte[] partitionDescriptor)
    {
        AdbcResources resources = null;
        ArrowReader reader = null;

        try {
            resources = openResources();
            reader = resources.connection().readPartition(ByteBuffer.wrap(partitionDescriptor));
            return new AdbcResult(resources, null, null, reader);
        }
        catch (AdbcException | RuntimeException e) {
            closeQuietly(reader);
            closeQuietly(resources);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to read StarRocks Flight SQL partition", e);
        }
    }

    private AdbcResources openResources()
            throws AdbcException
    {
        BufferAllocator allocator = new RootAllocator(flightSqlMaxAllocationBytes);
        AdbcDatabase database = null;
        AdbcConnection connection = null;
        InputStream rootCertificate = null;
        try {
            FlightSqlDriver driver = new FlightSqlDriver(allocator);
            Map<String, Object> parameters = new HashMap<>();
            AdbcDriver.PARAM_URI.set(parameters, location().getUri().toString());
            username.ifPresent(value -> AdbcDriver.PARAM_USERNAME.set(parameters, value));
            password.ifPresent(value -> AdbcDriver.PARAM_PASSWORD.set(parameters, value));
            FlightSqlConnectionProperties.TLS_SKIP_VERIFY.set(parameters, flightSqlTlsSkipVerify);
            flightSqlTlsOverrideHostname.ifPresent(value -> FlightSqlConnectionProperties.TLS_OVERRIDE_HOSTNAME.set(parameters, value));
            if (flightSqlTlsRootCertificate.isPresent()) {
                rootCertificate = new FileInputStream(flightSqlTlsRootCertificate.orElseThrow());
                FlightSqlConnectionProperties.TLS_ROOT_CERTS.set(parameters, rootCertificate);
            }

            database = driver.open(parameters);
            connection = database.connect();
            return new AdbcResources(allocator, database, connection);
        }
        catch (AdbcException | RuntimeException e) {
            closeQuietly(connection);
            closeQuietly(database);
            closeQuietly(allocator);
            throw e;
        }
        catch (Exception e) {
            closeQuietly(connection);
            closeQuietly(database);
            closeQuietly(allocator);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to open StarRocks Flight SQL connection", e);
        }
        finally {
            closeQuietly(rootCertificate);
        }
    }

    private Location location()
    {
        if (flightSqlTlsEnabled) {
            return Location.forGrpcTls(flightSqlHost, flightSqlPort);
        }
        return Location.forGrpcInsecure(flightSqlHost, flightSqlPort);
    }

    private static String inferFlightSqlHost(String jdbcUrl)
    {
        try {
            URI uri = URI.create(jdbcUrl.substring("jdbc:".length()));
            if (uri.getHost() == null || uri.getHost().isBlank()) {
                throw new IllegalArgumentException("Unable to infer host from configured StarRocks JDBC URL");
            }
            return uri.getHost();
        }
        catch (RuntimeException e) {
            throw new IllegalArgumentException("Unable to infer StarRocks Flight SQL host from configured JDBC URL", e);
        }
    }

    static boolean shouldFallbackToSingleSplit(AdbcException exception)
    {
        return exception.getStatus() == AdbcStatusCode.NOT_IMPLEMENTED;
    }

    private static boolean requiresSingleSplit(StarRocksTableHandle tableHandle)
    {
        return tableHandle.aggregation().isPresent() ||
                tableHandle.limit().isPresent() ||
                !tableHandle.sortOrder().isEmpty();
    }

    private static void closeQuietly(AutoCloseable closeable)
    {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        }
        catch (Exception _) {
            // Cleanup failures are ignored so the original query failure is preserved.
        }
    }

    private static final class AdbcResult
            implements StarRocksFlightSqlResult
    {
        private final AdbcResources resources;
        private final AdbcStatement statement;
        private final AdbcStatement.QueryResult queryResult;
        private final ArrowReader reader;
        private final AtomicBoolean closed = new AtomicBoolean();

        private AdbcResult(
                AdbcResources resources,
                AdbcStatement statement,
                AdbcStatement.QueryResult queryResult,
                ArrowReader reader)
        {
            this.resources = requireNonNull(resources, "resources is null");
            this.statement = statement;
            this.queryResult = queryResult;
            this.reader = requireNonNull(reader, "reader is null");
        }

        @Override
        public boolean loadNextBatch()
        {
            if (closed.get()) {
                return false;
            }

            try {
                return reader.loadNextBatch();
            }
            catch (Exception e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to load StarRocks Flight SQL batch", e);
            }
        }

        @Override
        public VectorSchemaRoot getVectorSchemaRoot()
        {
            try {
                return reader.getVectorSchemaRoot();
            }
            catch (Exception e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to access StarRocks Flight SQL batch schema root", e);
            }
        }

        @Override
        public long getMemoryUsage()
        {
            if (closed.get()) {
                return 0;
            }
            try {
                return resources.allocator().getAllocatedMemory();
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
            failure = closeResource(failure, reader);
            failure = closeResource(failure, queryResult);
            failure = closeResource(failure, statement);
            failure = closeResource(failure, resources);

            if (failure != null) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to close StarRocks Flight SQL result", failure);
            }
        }

        private static Exception closeResource(Exception failure, AutoCloseable closeable)
        {
            if (closeable == null) {
                return failure;
            }
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

    private record AdbcResources(BufferAllocator allocator, AdbcDatabase database, AdbcConnection connection)
            implements AutoCloseable
    {
        private AdbcResources
        {
            requireNonNull(allocator, "allocator is null");
            requireNonNull(database, "database is null");
            requireNonNull(connection, "connection is null");
        }

        @Override
        public void close()
        {
            try {
                closeQuietly(connection);
                closeQuietly(database);
            }
            finally {
                closeQuietly(allocator);
            }
        }
    }
}
