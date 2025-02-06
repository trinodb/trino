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
package io.trino.plugin.redshift;

import com.amazon.redshift.jdbc.RedshiftPreparedStatement;
import com.amazon.redshift.util.RedshiftException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.redshift.RedshiftErrorCode.REDSHIFT_FILESYSTEM_ERROR;
import static io.trino.plugin.redshift.RedshiftErrorCode.REDSHIFT_S3_CROSS_REGION_UNSUPPORTED;
import static java.util.Objects.requireNonNull;

public class RedshiftUnloadSplitSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(RedshiftUnloadSplitSource.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    private final JdbcClient jdbcClient;
    private final QueryBuilder queryBuilder;
    private final RemoteQueryModifier queryModifier;
    private final Optional<String> unloadAuthorization;
    private final String unloadOutputPath;
    private final TrinoFileSystem fileSystem;
    private final CompletableFuture<Void> resultSetFuture;

    private boolean finished;

    public RedshiftUnloadSplitSource(
            ExecutorService executor,
            ConnectorSession session,
            JdbcClient jdbcClient,
            JdbcTableHandle jdbcTableHandle,
            List<JdbcColumnHandle> columns,
            QueryBuilder queryBuilder,
            RemoteQueryModifier queryModifier,
            String unloadLocation,
            Optional<String> unloadAuthorization,
            TrinoFileSystem fileSystem)
    {
        requireNonNull(executor, "executor is null");
        requireNonNull(session, "session is null");
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        requireNonNull(jdbcTableHandle, "jdbcTableHandle is null");
        requireNonNull(columns, "columns is null");
        this.queryBuilder = requireNonNull(queryBuilder, "queryBuilder is null");
        this.queryModifier = requireNonNull(queryModifier, "queryModifier is null");
        this.unloadAuthorization = requireNonNull(unloadAuthorization, "unloadAuthorization is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");

        String queryFragmentId = session.getQueryId() + "/" + UUID.randomUUID();
        this.unloadOutputPath = unloadLocation + "/" + queryFragmentId + "/";

        resultSetFuture = CompletableFuture.runAsync(() -> {
            try (Connection connection = jdbcClient.getConnection(session)) {
                String redshiftSelectSql = buildRedshiftSelectSql(session, connection, jdbcTableHandle, columns);
                try (PreparedStatement statement = buildRedshiftUnloadSql(session, connection, columns, redshiftSelectSql, unloadOutputPath)) {
                    // Exclusively set readOnly to false to avoid query failing with "ERROR: transaction is read-only".
                    connection.setReadOnly(false);
                    log.debug("Executing: %s", statement);
                    long start = System.nanoTime();
                    statement.execute(); // Return value of `statement.execute()` is not useful to determine whether UNLOAD command produced any result as it always return false.
                    log.info("Redshift UNLOAD command for %s query took %s", queryFragmentId, nanosSince(start));
                }
            }
            catch (SQLException e) {
                if (e instanceof RedshiftException && e.getMessage() != null && e.getMessage().contains("The S3 bucket addressed by the query is in a different region from this cluster")) {
                    throw new TrinoException(REDSHIFT_S3_CROSS_REGION_UNSUPPORTED, "Redshift cluster and S3 bucket in different regions is not supported", e);
                }
                throw new RuntimeException(e);
            }
        }, executor);
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        return resultSetFuture
                .thenApply(_ -> {
                    ConnectorSplitBatch connectorSplitBatch = new ConnectorSplitBatch(readUnloadedFilePaths().stream()
                            .map(fileInfo -> (ConnectorSplit) new RedshiftUnloadSplit(fileInfo.path, fileInfo.size))
                            .collect(toImmutableList()), true);
                    finished = true;
                    return connectorSplitBatch;
                });
    }

    @Override
    public void close()
    {
        resultSetFuture.cancel(true);
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    private String buildRedshiftSelectSql(ConnectorSession session, Connection connection, JdbcTableHandle table, List<JdbcColumnHandle> columns)
            throws SQLException
    {
        PreparedQuery preparedQuery = jdbcClient.prepareQuery(session, table, Optional.empty(), columns, ImmutableMap.of());
        String selectQuerySql;
        try (PreparedStatement openTelemetryPreparedStatement = queryBuilder.prepareStatement(jdbcClient, session, connection, preparedQuery, Optional.of(columns.size()))) {
            RedshiftPreparedStatement redshiftPreparedStatement = openTelemetryPreparedStatement.unwrap(RedshiftPreparedStatement.class);
            selectQuerySql = redshiftPreparedStatement.toString();
        }
        return queryModifier.apply(session, selectQuerySql);
    }

    private PreparedStatement buildRedshiftUnloadSql(ConnectorSession session, Connection connection, List<JdbcColumnHandle> columns, String redshiftSelectSql, String unloadOutputPath)
            throws SQLException
    {
        String unloadSql = "UNLOAD ('%s') TO '%s' IAM_ROLE %s FORMAT PARQUET MAXFILESIZE 64MB MANIFEST VERBOSE".formatted(
                escapeUnloadIllegalCharacters(redshiftSelectSql),
                unloadOutputPath,
                unloadAuthorization.map("'%s'"::formatted).orElse("DEFAULT"));
        return queryBuilder.prepareStatement(jdbcClient, session, connection, new PreparedQuery(unloadSql, List.of()), Optional.of(columns.size()));
    }

    private List<FileInfo> readUnloadedFilePaths()
    {
        Location manifestLocation = Location.of(unloadOutputPath + "manifest");
        TrinoInputFile inputFile = fileSystem.newInputFile(manifestLocation);
        JsonNode outputFileEntries;
        try (TrinoInputStream inputStream = inputFile.newStream()) {
            byte[] manifestContent = inputStream.readAllBytes();
            outputFileEntries = OBJECT_MAPPER.readTree(manifestContent).path("entries");
        }
        // manifest is not generated if unload query doesn't produce any results.
        // Rely on the catching `FileNotFoundException` as opposed to calling `TrinoInputFile#exists` for determining absence of manifest file as `TrinoInputFile#exists` adds additional call to S3.
        catch (FileNotFoundException e) {
            return ImmutableList.of();
        }
        catch (IOException e) {
            throw new TrinoException(REDSHIFT_FILESYSTEM_ERROR, e);
        }
        ImmutableList.Builder<FileInfo> unloadedFilePaths = ImmutableList.builder();
        outputFileEntries.elements()
                .forEachRemaining(fileInfo -> unloadedFilePaths.add(new FileInfo(fileInfo.get("url").asText(), fileInfo.get("meta").get("content_length").longValue())));
        return unloadedFilePaths.build();
    }

    private static String escapeUnloadIllegalCharacters(String value)
    {
        return value
                .replace("'", "''") // escape single quotes with single quotes
                .replace("\\b", "\\\\b"); // escape backspace with backslash
    }

    private record FileInfo(String path, long size) {}
}
