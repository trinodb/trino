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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.PrestoWarning;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.TableConnectorWarningContext;

import java.util.List;
import java.util.Optional;

import static io.prestosql.plugin.hive.HiveSessionProperties.getPreferredFileFormatSuggestion;
import static io.prestosql.plugin.hive.HiveSessionProperties.getTooManyPartitionsLimit;
import static io.prestosql.plugin.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static io.prestosql.plugin.hive.HiveWarningCode.PREFERRED_FILE_FORMAT_SUGGESTION;
import static io.prestosql.plugin.hive.HiveWarningCode.TOO_MANY_PARTITIONS;
import static java.lang.String.format;

public class HiveWarningManager
{
    public List<PrestoWarning> generateHiveMetadataWarnings(
            ConnectorSession session,
            TableConnectorWarningContext tableConnectorWarningContext,
            HiveMetadata hiveMetadata)
    {
        ImmutableList.Builder<PrestoWarning> hiveMetadataWarnings = new ImmutableList.Builder<>();

        String preferredFileFormat = getPreferredFileFormatSuggestion(session);
        Optional<String> storageFormatWarningsMessages =
                generateStorageFormatWarningsMessages(hiveMetadata, session, tableConnectorWarningContext.getConnectorTableHandle(), preferredFileFormat);
        if (storageFormatWarningsMessages.isPresent()) {
            hiveMetadataWarnings.add(new PrestoWarning(PREFERRED_FILE_FORMAT_SUGGESTION, format("Try using %s for hive table %s", preferredFileFormat, storageFormatWarningsMessages.get())));
        }

        int partitionsLimit = getTooManyPartitionsLimit(session);
        Optional<String> partitionsWarningsMessages = generatePartionsWarningsMessages(tableConnectorWarningContext.getConnectorTableHandle(), partitionsLimit);
        if (partitionsWarningsMessages.isPresent()) {
            hiveMetadataWarnings.add(new PrestoWarning(TOO_MANY_PARTITIONS, format("Using more than %s partitions for table %s", partitionsLimit, partitionsWarningsMessages.get())));
        }
        return hiveMetadataWarnings.build();
    }

    private Optional<String> generateStorageFormatWarningsMessages(
            HiveMetadata hiveMetadata,
            ConnectorSession connectorSession,
            ConnectorTableHandle connectorTableHandle,
            String preferredFileFormat)
    {
        HiveStorageFormat storageFormat =
                (HiveStorageFormat) hiveMetadata
                .getTableMetadata(connectorSession, connectorTableHandle)
                .getProperties()
                .get(STORAGE_FORMAT_PROPERTY);

        if (!storageFormat.name().equals(preferredFileFormat)) {
            String tableName = ((HiveTableHandle) connectorTableHandle).getTableName();
            return Optional.of(tableName);
        }

        return Optional.empty();
    }

    private Optional<String> generatePartionsWarningsMessages(
            ConnectorTableHandle connectorTableHandle,
            int partitionsLimit)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) connectorTableHandle;
        if (hiveTableHandle.getPartitions().isPresent()) {
            List<HivePartition> partitions = hiveTableHandle.getPartitions().get();
            if (partitions.size() > partitionsLimit) {
                return Optional.of(hiveTableHandle.getTableName());
            }
        }

        return Optional.empty();
    }
}
