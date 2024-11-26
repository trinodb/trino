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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.redshift.RedshiftErrorCode.REDSHIFT_FILESYSTEM_ERROR;
import static java.util.Objects.requireNonNull;

public class RedshiftUnloadSplitSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(RedshiftUnloadSplitSource.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    private final CompletableFuture<Boolean> resultSetFuture;
    private final String unloadOutputLocation;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final ConnectorSession session;

    private boolean finished;

    public RedshiftUnloadSplitSource(ExecutorService executor, Connection connection, PreparedStatement statement, String unloadOutputLocation, TrinoFileSystemFactory fileSystemFactory, ConnectorSession session)
    {
        requireNonNull(executor, "executor is null");
        requireNonNull(statement, "statement is null");
        resultSetFuture = CompletableFuture.supplyAsync(() -> {
            log.debug("Executing: %s", statement);
            try {
                // Exclusively set readOnly to false to avoid query failing with "ERROR: transaction is read-only".
                connection.setReadOnly(false);
                return statement.execute();
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, executor);
        this.unloadOutputLocation = requireNonNull(unloadOutputLocation, "unloadOutputLocation is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.session = requireNonNull(session, "session is null");
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        return resultSetFuture
                .thenApply(_ -> {
                    ConnectorSplitBatch connectorSplitBatch = new ConnectorSplitBatch(getUnloadedFilePaths().stream()
                            .map(fileInfo -> (ConnectorSplit) new RedshiftUnloadSplit(fileInfo.path, fileInfo.size))
                            .collect(toImmutableList()), true);
                    finished = true;
                    return connectorSplitBatch;
                })
                .exceptionally(e -> {
                    throw new RuntimeException(e);
                });
    }

    private List<FileInfo> getUnloadedFilePaths()
    {
        ImmutableList.Builder<FileInfo> unloadedFilePaths = ImmutableList.builder();
        Location location = Location.of(unloadOutputLocation + "manifest");
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        TrinoInputFile inputFile = fileSystem.newInputFile(location);
        JsonNode outputFileEntries;
        try {
            // If query doesn't produce any results then manifest is not generated.
            if (!inputFile.exists()) {
                return ImmutableList.of();
            }
        }
        catch (IOException e) {
            throw new TrinoException(REDSHIFT_FILESYSTEM_ERROR, e);
        }
        try (TrinoInputStream inputStream = inputFile.newStream()) {
            byte[] manifestContent = inputStream.readAllBytes();
            outputFileEntries = OBJECT_MAPPER.readTree(manifestContent).path("entries");
        }
        catch (IOException e) {
            throw new TrinoException(REDSHIFT_FILESYSTEM_ERROR, e);
        }
        outputFileEntries.elements()
                .forEachRemaining(fileInfo -> unloadedFilePaths.add(new FileInfo(fileInfo.get("url").asText(), fileInfo.get("meta").get("content_length").longValue())));
        return unloadedFilePaths.build();
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

    private record FileInfo(String path, long size) {}
}
