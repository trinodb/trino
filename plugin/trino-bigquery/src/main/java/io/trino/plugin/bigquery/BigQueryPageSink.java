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
package io.trino.plugin.bigquery;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.type.Type;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.google.cloud.bigquery.storage.v1.WriteStream.Type.COMMITTED;
import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.bigquery.BigQueryErrorCode.BIGQUERY_BAD_WRITE;
import static io.trino.plugin.bigquery.BigQueryTypeUtils.readNativeValue;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class BigQueryPageSink
        implements ConnectorPageSink
{
    private final BigQueryWriteClient client;
    private final WriteStream writeStream;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final ConnectorPageSinkId pageSinkId;
    private final Optional<String> pageSinkIdColumnName;

    public BigQueryPageSink(
            BigQueryWriteClient client,
            RemoteTableName remoteTableName,
            List<String> columnNames,
            List<Type> columnTypes,
            ConnectorPageSinkId pageSinkId,
            Optional<String> temporaryTableName,
            Optional<String> pageSinkIdColumnName)
    {
        this.client = requireNonNull(client, "client is null");
        requireNonNull(remoteTableName, "remoteTableName is null");
        this.columnNames = ImmutableList.copyOf(requireNonNull(columnNames, "columnNames is null"));
        this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
        checkArgument(columnNames.size() == columnTypes.size(), "columnNames and columnTypes must have the same size");
        this.pageSinkId = requireNonNull(pageSinkId, "pageSinkId is null");
        requireNonNull(temporaryTableName, "temporaryTableName is null");
        this.pageSinkIdColumnName = requireNonNull(pageSinkIdColumnName, "pageSinkIdColumnName is null");
        checkArgument(temporaryTableName.isPresent() == pageSinkIdColumnName.isPresent(),
                "temporaryTableName.isPresent is not equal to pageSinkIdColumn.isPresent");
        TableName tableName = temporaryTableName
                .map(table -> TableName.of(remoteTableName.getProjectId(), remoteTableName.getDatasetName(), table))
                .orElseGet(remoteTableName::toTableName);
        // TODO: Consider using PENDING mode
        WriteStream stream = WriteStream.newBuilder().setType(COMMITTED).build();
        CreateWriteStreamRequest createWriteStreamRequest = CreateWriteStreamRequest.newBuilder()
                .setParent(tableName.toString())
                .setWriteStream(stream)
                .build();
        this.writeStream = client.createWriteStream(createWriteStreamRequest);
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        JSONArray batch = new JSONArray();
        for (int position = 0; position < page.getPositionCount(); position++) {
            JSONObject row = new JSONObject();
            pageSinkIdColumnName.ifPresent(column -> row.put(column, pageSinkId.getId()));
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                row.put(columnNames.get(channel), readNativeValue(columnTypes.get(channel), page.getBlock(channel), position));
            }
            batch.put(row);
        }

        insertWithCommitted(batch);
        return NOT_BLOCKED;
    }

    private void insertWithCommitted(JSONArray batch)
    {
        try (JsonStreamWriter writer = JsonStreamWriter.newBuilder(writeStream.getName(), writeStream.getTableSchema(), client).build()) {
            ApiFuture<AppendRowsResponse> future = writer.append(batch);
            AppendRowsResponse response = future.get(); // Throw error
            if (response.hasError()) {
                throw new TrinoException(BIGQUERY_BAD_WRITE, format("Response has error: %s", response.getError().getMessage()));
            }
        }
        catch (Exception e) {
            throw new TrinoException(BIGQUERY_BAD_WRITE, "Failed to insert rows", e);
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        Slice value = Slices.allocate(Long.BYTES);
        value.setLong(0, pageSinkId.getId());
        return completedFuture(ImmutableList.of(value));
    }

    @Override
    public void abort() {}
}
