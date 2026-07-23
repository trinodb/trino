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
package io.trino.plugin.dynamodb;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class DynamoDbSplitManager
        implements ConnectorSplitManager
{
    private final DynamoDbService service;

    @Inject
    public DynamoDbSplitManager(DynamoDbService service)
    {
        this.service = requireNonNull(service, "service is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        DynamoDbTableHandle tableHandle = (DynamoDbTableHandle) connectorTableHandle;
        String tableName = tableHandle.getTableName();
        int totalSegments = service.getScanSegments();

        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        for (int segment = 0; segment < totalSegments; segment++) {
            splits.add(new DynamoDbSplit(tableName, segment, totalSegments));
        }
        List<ConnectorSplit> splitList = splits.build();
        return new FixedSplitSource(splitList);
    }
}
