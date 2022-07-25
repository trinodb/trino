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
package io.trino.plugin.kinesis;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.plugin.kinesis.s3config.S3TableConfigClient;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;

import javax.annotation.PreDestroy;

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
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

/**
 * This class get() method reads the table description file stored in Kinesis directory
 * and then creates user defined field for Trino Table.
 */
public class KinesisTableDescriptionSupplier
        implements Supplier<Map<SchemaTableName, KinesisStreamDescription>>
{
    private static final Logger log = Logger.get(KinesisTableDescriptionSupplier.class);

    private final KinesisConfig kinesisConfig;
    private final JsonCodec<KinesisStreamDescription> streamDescriptionCodec;
    private final S3TableConfigClient s3TableConfigClient;

    @Inject
    public KinesisTableDescriptionSupplier(
            KinesisConfig kinesisConfig,
            JsonCodec<KinesisStreamDescription> streamDescriptionCodec,
            S3TableConfigClient s3TableConfigClient)
    {
        this.kinesisConfig = requireNonNull(kinesisConfig, "kinesisConfig is null");
        this.streamDescriptionCodec = requireNonNull(streamDescriptionCodec, "streamDescriptionCodec is null");
        this.s3TableConfigClient = requireNonNull(s3TableConfigClient, "s3TableConfigClient is null");
    }

    @Override
    public Map<SchemaTableName, KinesisStreamDescription> get()
    {
        if (s3TableConfigClient.isUsingS3()) {
            return s3TableConfigClient.getTablesFromS3();
        }

        return getTablesFromPath();
    }

    public Map<SchemaTableName, KinesisStreamDescription> getTablesFromPath()
    {
        ImmutableMap.Builder<SchemaTableName, KinesisStreamDescription> builder = ImmutableMap.builder();
        try {
            for (Path file : listFiles(Paths.get(kinesisConfig.getTableDescriptionLocation()))) {
                if (Files.isRegularFile(file) && file.getFileName().toString().endsWith("json")) {
                    KinesisStreamDescription table = streamDescriptionCodec.fromJson(Files.readAllBytes(file));
                    String schemaName = firstNonNull(table.getSchemaName(), kinesisConfig.getDefaultSchema());
                    log.debug("Kinesis table %s %s %s", schemaName, table.getTableName(), table);
                    builder.put(new SchemaTableName(schemaName, table.getTableName()), table);
                }
            }

            Map<SchemaTableName, KinesisStreamDescription> tableDefinitions = builder.buildOrThrow();
            log.debug("Loaded table definitions: %s", tableDefinitions.keySet());

            return tableDefinitions;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Shutdown any periodic update jobs.
     */
    @PreDestroy
    public void shutdown()
    {
        this.s3TableConfigClient.run();
    }

    private static List<Path> listFiles(Path path)
    {
        if (path == null || !Files.isDirectory(path)) {
            throw new TrinoException(KinesisErrorCode.KINESIS_METADATA_EXCEPTION, "Table description location does not exist or is not a directory");
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
            ImmutableList.Builder<Path> builder = ImmutableList.builder();
            for (Path file : stream) {
                builder.add(file);
            }

            return builder.build();
        }
        catch (IOException | DirectoryIteratorException e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
