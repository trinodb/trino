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
    Optional<ExtendedStatistics> readExtendedStatistics(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            String tableLocation,
            VendedCredentialsHandle credentialsHandle);

    void updateExtendedStatistics(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            String tableLocation,
            VendedCredentialsHandle credentialsHandle,
            ExtendedStatistics statistics);

    void deleteExtendedStatistics(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            String tableLocation,
            VendedCredentialsHandle credentialsHandle);
}
