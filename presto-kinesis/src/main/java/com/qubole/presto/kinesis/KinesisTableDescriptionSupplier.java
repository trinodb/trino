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
package com.qubole.presto.kinesis;

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.qubole.presto.kinesis.s3config.S3TableConfigClient;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.firstNonNull;
import static java.util.Objects.requireNonNull;

/**
 * This class get() method reads the table description file stored in Kinesis directory
 * and then creates user defined field for Presto Table.
 */
public class KinesisTableDescriptionSupplier
        implements Supplier<Map<SchemaTableName, KinesisStreamDescription>>, ConnectorShutdown
{
    private static final Logger log = Logger.get(KinesisTableDescriptionSupplier.class);

    private final KinesisConnectorConfig kinesisConnectorConfig;
    private final JsonCodec<KinesisStreamDescription> streamDescriptionCodec;
    private final S3TableConfigClient s3TableConfigClient;

    @Inject
    KinesisTableDescriptionSupplier(KinesisConnectorConfig kinesisConnectorConfig,
            JsonCodec<KinesisStreamDescription> streamDescriptionCodec,
            S3TableConfigClient anS3Client)
    {
        this.kinesisConnectorConfig = requireNonNull(kinesisConnectorConfig, "kinesisConnectorConfig is null");
        this.streamDescriptionCodec = requireNonNull(streamDescriptionCodec, "streamDescriptionCodec is null");
        this.s3TableConfigClient = requireNonNull(anS3Client, "S3 table config client is null");
    }

    @Override
    public Map<SchemaTableName, KinesisStreamDescription> get()
    {
        if (this.s3TableConfigClient.isUsingS3()) {
            return this.s3TableConfigClient.getTablesFromS3();
        }
        else {
            return getTablesFromDirectory();
        }
    }

    public Map<SchemaTableName, KinesisStreamDescription> getTablesFromDirectory()
    {
        ImmutableMap.Builder<SchemaTableName, KinesisStreamDescription> builder = ImmutableMap.builder();
        try {
            for (Path file : listFiles(Paths.get(kinesisConnectorConfig.getTableDescriptionDir()))) {
                if (Files.isRegularFile(file) && file.getFileName().toString().endsWith("json")) {
                    KinesisStreamDescription table = streamDescriptionCodec.fromJson(Files.readAllBytes(file));
                    String schemaName = firstNonNull(table.getSchemaName(), kinesisConnectorConfig.getDefaultSchema());
                    log.debug("Kinesis table %s %s %s", schemaName, table.getTableName(), table);
                    builder.put(new SchemaTableName(schemaName, table.getTableName()), table);
                }
            }

            Map<SchemaTableName, KinesisStreamDescription> tableDefinitions = builder.build();
            log.debug("Loaded table definitions: %s", tableDefinitions.keySet());

            return tableDefinitions;
        }
        catch (IOException e) {
            log.warn(e, "Error: ");
            throw Throwables.propagate(e);
        }
    }

    /**
     * Shutdown any periodic update jobs.
     */
    @Override
    public void shutdown()
    {
        this.s3TableConfigClient.shutdown();
        return;
    }

    private static List<Path> listFiles(Path dir)
    {
        if ((dir != null) && Files.isDirectory(dir)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
                ImmutableList.Builder<Path> builder = ImmutableList.builder();
                for (Path file : stream) {
                    builder.add(file);
                }

                return builder.build();
            }
            catch (IOException | DirectoryIteratorException x) {
                log.warn(x, "Warning.");
                throw Throwables.propagate(x);
            }
        }
        return ImmutableList.of();
    }
}
