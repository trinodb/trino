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
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.Exceptions.AppendSerializationError;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.type.Type;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.google.cloud.bigquery.storage.v1.WriteStream.Type.PENDING;
import static com.google.common.base.MoreObjects.firstNonNull;
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
    private final JsonStreamWriter streamWriter;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final ConnectorPageSinkId pageSinkId;
    private final Optional<String> pageSinkIdColumnName;
    private final TableName tableName;

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
        tableName = temporaryTableName
                .map(table -> TableName.of(remoteTableName.projectId(), remoteTableName.datasetName(), table))
                .orElseGet(remoteTableName::toTableName);

        CreateWriteStreamRequest createWriteStreamRequest = CreateWriteStreamRequest.newBuilder()
                .setParent(tableName.toString())
                .setWriteStream(WriteStream.newBuilder().setType(PENDING).build())
                .build();
        writeStream = client.createWriteStream(createWriteStreamRequest);
        try {
            streamWriter = JsonStreamWriter.newBuilder(writeStream.getName(), writeStream.getTableSchema(), client).build();
        }
        catch (DescriptorValidationException | IOException | InterruptedException e) {
            throw new TrinoException(BIGQUERY_BAD_WRITE, "Failed to create stream writer: " + firstNonNull(e.getMessage(), e), e);
        }
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

        append(batch);
        return NOT_BLOCKED;
    }

    private void append(JSONArray batch)
    {
        try {
            ApiFuture<AppendRowsResponse> future = streamWriter.append(batch);
            AppendRowsResponse response = future.get(); // Throw error
            if (response.hasError()) {
                throw new TrinoException(BIGQUERY_BAD_WRITE, format("Response has error: %s", response.getError().getMessage()));
            }
        }
        catch (IOException | DescriptorValidationException | ExecutionException | InterruptedException | AppendSerializationError e) {
            throw new TrinoException(BIGQUERY_BAD_WRITE, "Write failed: " + firstNonNull(e.getMessage(), e), e);
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        streamWriter.close();
        client.finalizeWriteStream(streamWriter.getStreamName());

        BatchCommitWriteStreamsRequest commitRequest = BatchCommitWriteStreamsRequest.newBuilder()
                .setParent(tableName.toString())
                .addWriteStreams(writeStream.getName())
                .build();
        BatchCommitWriteStreamsResponse response = client.batchCommitWriteStreams(commitRequest);
        if (response.getStreamErrorsCount() > 0) {
            throw new TrinoException(BIGQUERY_BAD_WRITE, "Committing write failed: " + response.getStreamErrorsList());
        }

        client.close();
        Slice value = Slices.allocate(Long.BYTES);
        value.setLong(0, pageSinkId.getId());
        return completedFuture(ImmutableList.of(value));
    }

    @Override
    public void abort()
    {
        client.close();
    }
}
