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
package io.trino.plugin.iceberg.catalog.hadoop;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputStream;
import io.trino.plugin.iceberg.catalog.AbstractIcebergTableOperations;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_COMMIT_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;

@NotThreadSafe
public class HadoopIcebergTableOperations
        extends AbstractIcebergTableOperations
{
    private final FileIO fileIO;
    private final TrinoHadoopCatalog catalog;

    private static final Joiner SLASH = Joiner.on("/");
    public static final String VERSION_HINT_FILENAME = "version-hint.text";
    private static final Pattern VERSION_PATTERN = Pattern.compile("v([^\\.]*)\\..*");

    protected HadoopIcebergTableOperations(
            FileIO fileIo,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location,
            TrinoHadoopCatalog catalog)
    {
        super(fileIo, session, database, table, owner, location);
        this.fileIO = fileIo;
        this.catalog = catalog;
    }

    @Override
    protected String getRefreshedLocation(boolean invalidateCaches)
    {
        int version = findVersion();
        Optional<Location> metadataFileLocation;

        try {
            metadataFileLocation = getMetadataFile(version);
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, String.format("Could not determine refreshed table metadata location. Error trying to recover metadataFile location for table %s", getSchemaTableName()), e);
        }
        if (metadataFileLocation.isEmpty()) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, String.format("Could not establish refreshed table metadata location for table: %s", getSchemaTableName()));
        }
        else {
            return SLASH.join(metadataFileLocation.get().path(), metadataFileLocation.get().fileName());
        }
    }

    @Override
    protected void commitNewTable(TableMetadata metadata)
    {
        verify(version.isEmpty(), "commitNewTable called on a table which already exists");
        String newMetadataLocation = writeNewMetadata(metadata, 0);

        Map<String, String> properties = Map.of(
                TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE,
                METADATA_LOCATION_PROP, newMetadataLocation);
        String tableComment = metadata.properties().get(TABLE_COMMENT);
        if (tableComment != null) {
            properties.put(TABLE_COMMENT, tableComment);
        }

        commit(null, metadata);
    }

    @Override
    protected void commitToExistingTable(TableMetadata base, TableMetadata metadata)
    {
        Table table = getTable();
        checkState(currentMetadataLocation != null, "No current metadata location for existing table");
        String metadataLocation = table.properties().get(METADATA_LOCATION_PROP);
        if (!currentMetadataLocation.equals(metadataLocation)) {
            throw new CommitFailedException("Metadata location [%s] is not same as table metadata location [%s] for %s",
                    currentMetadataLocation, metadataLocation, getSchemaTableName());
        }
        Map<String, String> properties = table.properties();
        properties.put(METADATA_LOCATION_PROP, metadataLocation);
        properties.put(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation);
        commit(base, metadata);
    }

    public void commit(TableMetadata base, TableMetadata metadata)
    {
        Pair<OptionalInt, TableMetadata> current = Pair.of(version, currentMetadata);
        if (base != current.second()) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Cannot commit changes based on stale table metadata");
        }

        checkArgument(
                base == null || base.location().equals(metadata.location()),
                "Hadoop path-based tables cannot be relocated");
        checkArgument(
                !metadata.properties().containsKey(TableProperties.WRITE_METADATA_LOCATION),
                "Hadoop path-based tables cannot relocate metadata");
        String codecName =
                metadata.property(
                        TableProperties.METADATA_COMPRESSION, TableProperties.METADATA_COMPRESSION_DEFAULT);
        TableMetadataParser.Codec codec = TableMetadataParser.Codec.fromName(codecName);
        String fileExtension = TableMetadataParser.getFileExtension(codec);
        Location tempMetadataFile = metadataLocation(UUID.randomUUID().toString() + fileExtension);
        TableMetadataParser.write(metadata, fileIo.newOutputFile(tempMetadataFile.toString()));
        int nextVersion = current.first().isPresent() ? current.first().getAsInt() + 1 : 0;
        Location finalMetadataFile = metadataFileLocation(nextVersion, codec);

        try {
            if (catalog.getTrinoFileSystem().listFiles(finalMetadataFile).hasNext()) {
                throw new TrinoException(ICEBERG_COMMIT_ERROR, String.format("Version %d already exists: %s", nextVersion, finalMetadataFile.toString()));
            }
            catalog.getTrinoFileSystem().renameFile(tempMetadataFile, finalMetadataFile);
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, String.format("Failed to commit changes using rename: %s", finalMetadataFile.toString()), e);
        }

        writeVersionHint(nextVersion);
        deleteRemovedMetadataFiles(base, metadata);
        this.shouldRefresh = true;
    }

    @Override
    protected void commitMaterializedViewRefresh(TableMetadata base, TableMetadata metadata)
    {
        throw new TrinoException(NOT_SUPPORTED, "commitMaterializedViewRefresh is not supported by " + catalog.getCatalogName());
    }

    private Table getTable()
    {
        return catalog.loadTable(session, getSchemaTableName());
    }

    private synchronized void updateVersionAndMetadata(int newVersion, String metadataFile)
    {
        // update if the current version is out of date
        if (version.isEmpty() || version.orElseThrow() != newVersion) {
            this.version = OptionalInt.of(newVersion);
            this.currentMetadata =
                    checkUUID(currentMetadata, TableMetadataParser.read(fileIo, metadataFile));
        }
    }

    private static TableMetadata checkUUID(TableMetadata currentMetadata, TableMetadata newMetadata)
    {
        String newUUID = newMetadata.uuid();
        if (currentMetadata != null && currentMetadata.uuid() != null && newUUID != null) {
            checkState(
                    newUUID.equals(currentMetadata.uuid()),
                    "Table UUID does not match: current=%s != refreshed=%s",
                    currentMetadata.uuid(),
                    newUUID);
        }
        return newMetadata;
    }

    @Override
    public TableMetadata current()
    {
        if (shouldRefresh) {
            return this.refresh();
        }
        return currentMetadata;
    }

    @Override
    public TableMetadata refresh()
    {
        int ver = version.orElseGet(this::findVersion);
        try {
            Optional<Location> metadataFile = getMetadataFile(ver);
            if (version.isEmpty() && metadataFile.isEmpty() && ver == 0) {
                // no v0 metadata means the table doesn't exist yet
                return null;
            }
            else if (metadataFile.isEmpty()) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, String.format("Metadata file for version %d is missing", ver));
            }

            Optional<Location> nextMetadataFile = getMetadataFile(ver + 1);
            while (nextMetadataFile.isPresent()) {
                ver++;
                metadataFile = nextMetadataFile;
                nextMetadataFile = getMetadataFile(ver + 1);
            }

            updateVersionAndMetadata(ver, metadataFile.get().toString());
            this.shouldRefresh = false;
            return currentMetadata;
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, String.format("Failed to refresh the table"), e);
        }
    }

    Location versionHintFile()
    {
        return metadataLocation(VERSION_HINT_FILENAME);
    }

    private void writeVersionHint(int versionToWrite)
    {
        Location versionHintFile = versionHintFile();

        try {
            Location tempVersionHintFile = metadataLocation(UUID.randomUUID().toString() + "-version-hint.temp");
            writeVersionToLocation(tempVersionHintFile, versionToWrite);
            catalog.getTrinoFileSystem().deleteFile(versionHintFile);
            catalog.getTrinoFileSystem().renameFile(tempVersionHintFile, versionHintFile);
        }
        catch (IOException ignored) {
        }
    }

    private void writeVersionToLocation(Location location, int versionToWrite)
            throws IOException
    {
        try (OutputStream out = catalog.getTrinoFileSystem().newOutputFile(location).create()) {
            out.write(String.valueOf(versionToWrite).getBytes(StandardCharsets.UTF_8));
        }
    }

    private void deleteRemovedMetadataFiles(TableMetadata base, TableMetadata metadata)
    {
        if (base == null) {
            return;
        }

        boolean deleteAfterCommit =
                metadata.propertyAsBoolean(
                        TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED,
                        TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT);

        if (deleteAfterCommit) {
            Set<TableMetadata.MetadataLogEntry> removedPreviousMetadataFiles = Sets.newHashSet(base.previousFiles());
            metadata.previousFiles().forEach(removedPreviousMetadataFiles::remove);
            Tasks.foreach(removedPreviousMetadataFiles)
                    .executeWith(ThreadPools.getWorkerPool())
                    .noRetry()
                    .suppressFailureWhenFinished()
                    .run(previousMetadataFile -> {
                        try {
                            catalog.getTrinoFileSystem().deleteFile(Location.of(previousMetadataFile.file()));
                        }
                        catch (IOException ignored) {
                        }
                    });
        }
    }

    int findVersion()
    {
        Location versionHintFile = versionHintFile();
        TrinoFileSystem tfs = catalog.getTrinoFileSystem();

        try (InputStreamReader fsr = new InputStreamReader(fileIO.newInputFile(versionHintFile.toString()).newStream(), StandardCharsets.UTF_8);
                BufferedReader in = new BufferedReader(fsr)) {
            return Integer.parseInt(in.readLine().replace("\n", ""));
        }
        catch (Exception e) {
            try {
                Optional<Boolean> directoryExists = tfs.directoryExists(metadataRoot());
                if (directoryExists.isEmpty()) {
                    return 0;
                }
                if (!directoryExists.get()) {
                    return 0;
                }

                // List the metadata directory to find the version files, and try to recover the max
                // available version
                FileIterator files = tfs.listFiles(metadataRoot());
                int maxVersion = 0;

                while (files.hasNext()) {
                    int currentVersion = version(files.next().location().fileName());
                    if (currentVersion > maxVersion && getMetadataFile(currentVersion).isEmpty()) {
                        maxVersion = currentVersion;
                    }
                }

                return maxVersion;
            }
            catch (IOException io) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, String.format("Failed to retrieve version number at location %s", location.toString()), e);
            }
        }
    }

    Optional<Location> getMetadataFile(int metadataVersion)
            throws IOException
    {
        TrinoFileSystem tfs = catalog.getTrinoFileSystem();
        Location metadataFileLocation = metadataFileLocation(metadataVersion, TableMetadataParser.Codec.NONE);
        TrinoInputStream inputStream;
        try {
            inputStream = tfs.newInputFile(metadataFileLocation).newStream();
            inputStream.close();
        }
        catch (Exception e) {
            return Optional.empty();
        }
        return Optional.of(metadataFileLocation);
    }

    private Location metadataFileLocation(int metadataVersion, TableMetadataParser.Codec codec)
    {
        return metadataLocation("v" + metadataVersion + TableMetadataParser.getFileExtension(codec));
    }

    private Location metadataLocation(String filename)
    {
        return metadataRoot().appendPath(filename);
    }

    private Location metadataRoot()
    {
        return Location.of(location.orElse("")).appendPath("metadata");
    }

    private int version(String fileName)
    {
        Matcher matcher = VERSION_PATTERN.matcher(fileName);
        if (!matcher.matches()) {
            return -1;
        }
        String versionNumber = matcher.group(1);
        try {
            return Integer.parseInt(versionNumber);
        }
        catch (NumberFormatException ne) {
            return -1;
        }
    }
}
