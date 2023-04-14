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
package io.trino.plugin.spanner;

import com.google.cloud.NoCredentials;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;

import java.time.LocalDate;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.trino.plugin.jdbc.JdbcWriteSessionProperties.getWriteBatchSize;
import static java.time.format.DateTimeFormatter.ISO_DATE;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class SpannerSink
        implements ConnectorPageSink
{
    private final SpannerOptions options;
    private final int maxBatchSize;
    private final DatabaseClient client;
    private final List<Type> columnTypes;
    private final List<String> columnNames;
    private final String project = "spanner-project";
    private final String instance = "spanner-instance";
    private final String database = "spanner-database";
    private final String table;
    private final ConnectorPageSinkId pageSinkId;
    private final SpannerSessionProperties.Mode writeMode;
    private List<Mutation> mutations = new LinkedList<>();

    public SpannerSink(ConnectorSession session, JdbcOutputTableHandle handle,
            ConnectorPageSinkId pageSinkId)
    {
        this.options = SpannerOptions
                .newBuilder()
                .setEmulatorHost("0.0.0.0:9010")
                .setCredentials(NoCredentials.getInstance())
                .setProjectId(project)
                .build();
        this.pageSinkId = pageSinkId;
        this.maxBatchSize = getWriteBatchSize(session);

        this.client = options.getService().getDatabaseClient(DatabaseId.of(project, instance, database));
        columnTypes = handle.getColumnTypes();
        columnNames = handle.getColumnNames();
        table = handle.getTableName();
        writeMode = session.getProperty(SpannerSessionProperties.WRITE_MODE, SpannerSessionProperties.Mode.class);
    }

    public Mutation.WriteBuilder createWriteBuilder()
    {
        if (writeMode.equals(SpannerSessionProperties.Mode.UPSERT)) {
            return Mutation.newInsertOrUpdateBuilder(table);
        }
        else {
            return Mutation.newInsertBuilder(table);
        }
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        for (int position = 0; position < page.getPositionCount(); position++) {
            Mutation.WriteBuilder writeBuilder = createWriteBuilder();
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                Block block = page.getBlock(channel);
                Type type = columnTypes.get(channel);
                String columnName = columnNames.get(channel);
                if (!block.isNull(position)) {
                    Class<?> javaType = type.getJavaType();
                    if (javaType == boolean.class) {
                        writeBuilder.set(columnName).to(type.getBoolean(block, position));
                    }
                    else if (javaType == long.class) {
                        if (type.getDisplayName().equalsIgnoreCase("DATE")) {
                            String date = LocalDate.ofEpochDay(type.getLong(block, position)).format(ISO_DATE);
                            writeBuilder.set(columnName).to(date);
                        }
                        else {
                            writeBuilder.set(columnName).to(type.getLong(block, position));
                        }
                    }
                    else if (javaType == double.class) {
                        writeBuilder.set(columnName).to(type.getDouble(block, position));
                    }
                    else if (javaType == Slice.class) {
                        writeBuilder.set(columnName).to(type.getSlice(block, position).toStringUtf8());
                    }
                    else {
                        System.out.println("TYPE CLASS " + javaType);
                        System.out.println("TYPE Display NAME " + type.getDisplayName());
                        System.out.println("TYPE Base NAME " + type.getBaseName());
                        System.out.println("TYPE ID " + type.getTypeId());
                        System.out.println("TYPE Signature " + type.getTypeSignature());
                        throw new RuntimeException("Unknown type");
                    }
                }
                mutations.add(writeBuilder.build());
                if (mutations.size() >= maxBatchSize) {
                    write();
                }
            }
        }
        return NOT_BLOCKED;
    }

    private void write()
    {
        if (!mutations.isEmpty()) {
            try {
                Timestamp write = client.write(mutations);
                System.out.println("Batch write completed " + write + " " + mutations.size() + " records flushed");
            }
            catch (Exception e) {
                System.out.println(e);
                if (e instanceof SpannerException spannerEx) {
                    if (spannerEx.getErrorCode().equals(ErrorCode.ALREADY_EXISTS)) {
                        throw new TrinoException(SpannerClient.SpannerErrorCode.SPANNER_ERROR_CODE, String.format("%s Try changing %s to %s and retry this query ", spannerEx.getMessage(), SpannerSessionProperties.WRITE_MODE,
                                SpannerSessionProperties.Mode.UPSERT));
                    }
                }
            }
            mutations = new LinkedList<>();
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        write();
        return completedFuture(ImmutableList.of(Slices.wrappedLongArray(pageSinkId.getId())));
    }

    @Override
    public void abort()
    {
        mutations = null;
    }
}
