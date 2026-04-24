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
package io.trino.plugin.deltalake.statistics;

import io.trino.plugin.deltalake.metastore.VendedCredentialsHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;

import java.util.Optional;

public interface ExtendedStatisticsAccess
{
    /**
     * Reads the extended statistics for a table from the given stats file.
     *
     * @return the extended statistics, or {@link Optional#empty()} if the file does not exist
     */
    Optional<ExtendedStatistics> readExtendedStatistics(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            String tableLocation,
            String extendedStatsFile,
            VendedCredentialsHandle credentialsHandle);

    /**
     * Writes the given extended statistics to a new file under the table's stats directory,
     * then deletes the previous stats file if one existed.
     * <p>
     * The caller must persist the returned filename in the
     * <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#commit-provenance-information">commitInfo</a>
     * entry of the transaction log; otherwise the written statistics will not be discoverable on subsequent reads.
     *
     * @return the name of the newly written stats file
     */
    String writeExtendedStatistics(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            String tableLocation,
            Optional<String> previousExtendedStatsFile,
            VendedCredentialsHandle credentialsHandle,
            ExtendedStatistics statistics);

    /**
     * Deletes the extended statistics file for a table. No-op if the file does not exist.
     */
    void deleteExtendedStatistics(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            String tableLocation,
            String extendedStatsFile,
            VendedCredentialsHandle credentialsHandle);
}
