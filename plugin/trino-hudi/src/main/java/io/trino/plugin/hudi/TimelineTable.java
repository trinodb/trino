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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hudi.HudiUtil.buildTableMetaClient;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class TimelineTable
        implements SystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final List<Type> types;
    private final Configuration configuration;
    private final String location;

    public TimelineTable(Configuration configuration, SchemaTableName tableName, Table hudiTable)
    {
        this.tableMetadata = new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"),
                ImmutableList.<ColumnMetadata>builder()
                        .add(new ColumnMetadata("timestamp", VARCHAR))
                        .add(new ColumnMetadata("action", VARCHAR))
                        .add(new ColumnMetadata("state", VARCHAR))
                        .build());
        this.types = tableMetadata.getColumns().stream().map(ColumnMetadata::getType).collect(toImmutableList());
        this.configuration = requireNonNull(configuration, "configuration is null");
        this.location = requireNonNull(hudiTable.getStorage().getLocation(), "location is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        HoodieTableMetaClient metaClient = buildTableMetaClient(configuration, location);
        Iterable<List<Object>> records = () -> metaClient.getCommitsTimeline().getInstants().map(this::getRecord).iterator();
        return new InMemoryRecordSet(types, records).cursor();
    }

    private List<Object> getRecord(HoodieInstant hudiInstant)
    {
        List<Object> columns = new ArrayList<>();
        columns.add(hudiInstant.getTimestamp());
        columns.add(hudiInstant.getAction());
        columns.add(hudiInstant.getState().toString());
        checkArgument(columns.size() == types.size(), "Expected %s types in row, but got %s values", types.size(), columns.size());
        return columns;
    }
}
