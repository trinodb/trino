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
package io.trino.plugin.kudu;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.kudu.KuduColumnHandle.ROW_ID_POSITION;

public class KuduRecordSet
        implements RecordSet
{
    private final KuduClientSession clientSession;
    private final KuduSplit kuduSplit;
    private final List<? extends ColumnHandle> columns;

    private KuduTable kuduTable;

    public KuduRecordSet(KuduClientSession clientSession, KuduSplit kuduSplit, List<? extends ColumnHandle> columns)
    {
        this.clientSession = clientSession;
        this.kuduSplit = kuduSplit;
        this.columns = columns;
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columns.stream()
                .map(column -> ((KuduColumnHandle) column).getType())
                .collect(toImmutableList());
    }

    @Override
    public RecordCursor cursor()
    {
        KuduScanner scanner = clientSession.createScanner(kuduSplit);
        Schema projectedSchema = scanner.getProjectionSchema();
        ImmutableMap.Builder<Integer, Integer> builder = ImmutableMap.builder();
        for (int i = 0; i < columns.size(); i++) {
            KuduColumnHandle handle = (KuduColumnHandle) columns.get(i);
            if (handle.isVirtualRowId()) {
                builder.put(i, ROW_ID_POSITION);
            }
            else {
                builder.put(i, projectedSchema.getColumnIndex(handle.getName()));
            }
        }

        return new KuduRecordCursor(scanner, getTable(), getColumnTypes(), builder.build());
    }

    KuduTable getTable()
    {
        if (kuduTable == null) {
            kuduTable = clientSession.openTable(kuduSplit.getSchemaTableName());
        }
        return kuduTable;
    }

    KuduClientSession getClientSession()
    {
        return clientSession;
    }
}
