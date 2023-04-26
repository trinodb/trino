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

package io.trino.plugin.influxdb;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;
import org.influxdb.dto.Query;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.influxdb.QueryUtils.buildQueryCommand;
import static java.util.Objects.requireNonNull;

public class InfluxRecordSet
        implements RecordSet
{
    private final InfluxRecord sourceData;
    private final List<InfluxColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    public InfluxRecordSet(InfluxTableHandle tableHandle, List<InfluxColumnHandle> columnHandles, InfluxClient client)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(columnHandles, "columnHandles is null");

        this.columnHandles = ImmutableList.copyOf(columnHandles);
        this.columnTypes = columnHandles.stream()
                .map(InfluxColumnHandle::getType)
                .collect(toImmutableList());
        this.sourceData = querySourceData(client, tableHandle, columnHandles);
    }

    private InfluxRecord querySourceData(InfluxClient client, InfluxTableHandle tableHandle, List<InfluxColumnHandle> columnHandles)
    {
        Query query = buildQueryCommand(tableHandle, columnHandles);
        return client.query(query);
    }

    @VisibleForTesting
    public InfluxRecord getSourceData()
    {
        return sourceData;
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new InfluxRecordCursor(columnHandles, sourceData);
    }
}
