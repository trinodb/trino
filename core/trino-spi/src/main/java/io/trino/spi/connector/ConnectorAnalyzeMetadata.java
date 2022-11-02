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
package io.trino.spi.connector;

import io.trino.spi.statistics.TableStatisticsMetadata;

import static java.util.Objects.requireNonNull;

public class ConnectorAnalyzeMetadata
{
    private final ConnectorTableHandle tableHandle;
    private final TableStatisticsMetadata statisticsMetadata;

    public ConnectorAnalyzeMetadata(ConnectorTableHandle tableHandle, TableStatisticsMetadata statisticsMetadata)
    {
        this.statisticsMetadata = requireNonNull(statisticsMetadata, "statisticsMetadata is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
    }

    public ConnectorTableHandle getTableHandle()
    {
        return tableHandle;
    }

    public TableStatisticsMetadata getStatisticsMetadata()
    {
        return statisticsMetadata;
    }
}
