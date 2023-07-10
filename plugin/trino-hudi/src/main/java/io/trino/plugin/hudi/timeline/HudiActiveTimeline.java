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
package io.trino.plugin.hudi.timeline;

import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInputStream;
import io.trino.plugin.hudi.model.HudiInstant;
import io.trino.plugin.hudi.table.HudiTableMetaClient;
import io.trino.spi.TrinoException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.hudi.HudiErrorCode.HUDI_BAD_DATA;

public class HudiActiveTimeline
        extends HudiDefaultTimeline
{
    private static final Set<String> VALID_EXTENSIONS_IN_ACTIVE_TIMELINE = ImmutableSet.of(
            COMMIT_EXTENSION, INFLIGHT_COMMIT_EXTENSION, REQUESTED_COMMIT_EXTENSION,
            DELTA_COMMIT_EXTENSION, INFLIGHT_DELTA_COMMIT_EXTENSION, REQUESTED_DELTA_COMMIT_EXTENSION,
            SAVEPOINT_EXTENSION, INFLIGHT_SAVEPOINT_EXTENSION,
            CLEAN_EXTENSION, REQUESTED_CLEAN_EXTENSION, INFLIGHT_CLEAN_EXTENSION,
            INFLIGHT_COMPACTION_EXTENSION, REQUESTED_COMPACTION_EXTENSION,
            REQUESTED_RESTORE_EXTENSION, INFLIGHT_RESTORE_EXTENSION, RESTORE_EXTENSION,
            ROLLBACK_EXTENSION, REQUESTED_ROLLBACK_EXTENSION, INFLIGHT_ROLLBACK_EXTENSION,
            REQUESTED_REPLACE_COMMIT_EXTENSION, INFLIGHT_REPLACE_COMMIT_EXTENSION, REPLACE_COMMIT_EXTENSION,
            REQUESTED_INDEX_COMMIT_EXTENSION, INFLIGHT_INDEX_COMMIT_EXTENSION, INDEX_COMMIT_EXTENSION,
            REQUESTED_SAVE_SCHEMA_ACTION_EXTENSION, INFLIGHT_SAVE_SCHEMA_ACTION_EXTENSION, SAVE_SCHEMA_ACTION_EXTENSION);

    private HudiTableMetaClient metaClient;

    public HudiActiveTimeline(HudiTableMetaClient metaClient)
    {
        // Filter all the filter in the metapath and include only the extensions passed and
        // convert them into HoodieInstant
        try {
            this.setInstants(metaClient.scanHoodieInstantsFromFileSystem(VALID_EXTENSIONS_IN_ACTIVE_TIMELINE, true));
        }
        catch (IOException e) {
            throw new TrinoException(HUDI_BAD_DATA, "Failed to scan metadata", e);
        }
        this.metaClient = metaClient;
        this.details = this::getInstantDetails;
    }

    @Deprecated
    public HudiActiveTimeline()
    {
    }

    @Override
    public Optional<byte[]> getInstantDetails(HudiInstant instant)
    {
        Location detailLocation = getInstantFileNamePath(instant.getFileName());
        return readDataFromPath(detailLocation);
    }

    //-----------------------------------------------------------------
    //      BEGIN - COMPACTION RELATED META-DATA MANAGEMENT.
    //-----------------------------------------------------------------

    public Optional<byte[]> readCompactionPlanAsBytes(HudiInstant instant)
    {
        // Reading from auxiliary location first. In future release, we will cleanup compaction management
        // to only write to timeline and skip auxiliary and this code will be able to handle it.
        return readDataFromPath(Location.of(metaClient.getMetaAuxiliaryPath()).appendPath(instant.getFileName()));
    }

    private Location getInstantFileNamePath(String fileName)
    {
        return Location.of(fileName.contains(SCHEMA_COMMIT_ACTION) ? metaClient.getSchemaFolderName() : metaClient.getMetaPath().path()).appendPath(fileName);
    }

    private Optional<byte[]> readDataFromPath(Location detailPath)
    {
        try (TrinoInputStream inputStream = metaClient.getFileSystem().newInputFile(detailPath).newStream()) {
            return Optional.of(readAsByteArray(inputStream));
        }
        catch (IOException e) {
            throw new TrinoException(HUDI_BAD_DATA, "Could not read commit details from " + detailPath, e);
        }
    }

    private static byte[] readAsByteArray(InputStream input)
            throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(128);
        copy(input, bos);
        return bos.toByteArray();
    }

    private static void copy(InputStream inputStream, OutputStream outputStream)
            throws IOException
    {
        byte[] buffer = new byte[1024];
        int len;
        while ((len = inputStream.read(buffer)) != -1) {
            outputStream.write(buffer, 0, len);
        }
    }
}
