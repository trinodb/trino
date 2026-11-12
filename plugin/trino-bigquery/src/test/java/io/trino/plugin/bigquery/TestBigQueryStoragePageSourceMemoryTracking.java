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

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.AvroRows;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadGrpc;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SourcePage;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.trino.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestBigQueryStoragePageSourceMemoryTracking
{
    private static final BigQueryTypeManager TYPE_MANAGER = new BigQueryTypeManager(TESTING_TYPE_MANAGER);
    private static final List<BigQueryColumnHandle> COLUMNS = ImmutableList.of(
            new BigQueryColumnHandle("value", ImmutableList.of(), BIGINT, StandardSQLTypeName.INT64, true, Mode.NULLABLE, ImmutableList.of(), null, false));
    private static final Schema ARROW_SCHEMA = new Schema(ImmutableList.of(Field.nullable("value", new ArrowType.Int(64, true))));
    private static final org.apache.avro.Schema AVRO_SCHEMA = SchemaBuilder.record("__root__").fields()
            .optionalLong("value")
            .endRecord();

    // Fetch the responses synchronously, so that every page is available when requested
    private final ExecutorService executor = newDirectExecutorService();
    private final BigQueryArrowBufferAllocator arrowBufferAllocator = new BigQueryArrowBufferAllocator(new BigQueryArrowConfig(), new BigQueryArrowAllocatorStats());

    @AfterAll
    void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    void testArrowMemoryUsage()
            throws Exception
    {
        try (TestingBigQueryReadServer server = new TestingBigQueryReadServer(ImmutableList.of(arrowResponse(0, 100), arrowResponse(100, 50)))) {
            List<Long> reportedBytes = new ArrayList<>();
            assertMemoryUsage(createArrowPageSource(server.client(), reportedBytes::add), reportedBytes);
        }
    }

    @Test
    void testAvroMemoryUsage()
            throws Exception
    {
        try (TestingBigQueryReadServer server = new TestingBigQueryReadServer(ImmutableList.of(avroResponse(0, 100), avroResponse(100, 50)))) {
            List<Long> reportedBytes = new ArrayList<>();
            assertMemoryUsage(createAvroPageSource(server.client(), reportedBytes::add), reportedBytes);
        }
    }

    @Test
    void testArrowMemoryLimitExceededOnCreation()
            throws Exception
    {
        try (TestingBigQueryReadServer server = new TestingBigQueryReadServer(ImmutableList.of())) {
            TrinoException memoryLimitExceeded = new TrinoException(EXCEEDED_LOCAL_MEMORY_LIMIT, "Exceeded memory limit");
            assertThatThrownBy(() -> createArrowPageSource(server.client(), _ -> {
                throw memoryLimitExceeded;
            })).isSameAs(memoryLimitExceeded);
            assertThat(server.readRowsRequests()).isZero();
        }
    }

    @Test
    void testAvroMemoryLimitExceededOnCreation()
            throws Exception
    {
        try (TestingBigQueryReadServer server = new TestingBigQueryReadServer(ImmutableList.of())) {
            TrinoException memoryLimitExceeded = new TrinoException(EXCEEDED_LOCAL_MEMORY_LIMIT, "Exceeded memory limit");
            assertThatThrownBy(() -> createAvroPageSource(server.client(), _ -> {
                throw memoryLimitExceeded;
            })).isSameAs(memoryLimitExceeded);
            assertThat(server.readRowsRequests()).isZero();
        }
    }

    private static void assertMemoryUsage(ConnectorPageSource pageSource, List<Long> reportedBytes)
            throws IOException
    {
        long emptyPageBuilderBytes = new PageBuilder(ImmutableList.of(BIGINT)).getRetainedSizeInBytes();
        try (pageSource) {
            assertThat(reportedBytes).containsExactly(emptyPageBuilderBytes);

            // the built page is handed over, so only the reset page builder remains reported
            assertPage(pageSource.getNextSourcePage(), 0, 100);
            assertThat(reportedBytes).containsExactly(emptyPageBuilderBytes, emptyPageBuilderBytes);
            assertPage(pageSource.getNextSourcePage(), 100, 50);
            assertThat(reportedBytes).containsExactly(emptyPageBuilderBytes, emptyPageBuilderBytes, emptyPageBuilderBytes);

            assertThat(pageSource.getNextSourcePage()).isNull();
            assertThat(pageSource.isFinished()).isTrue();
            assertThat(reportedBytes).hasSize(3);
        }
        // the page builder is still retained after close, so its reservation is released by the context's owner
        assertThat(reportedBytes).hasSize(3);
    }

    private static void assertPage(SourcePage page, long firstValue, int positionCount)
    {
        assertThat(page.getPositionCount()).isEqualTo(positionCount);
        for (int position = 0; position < positionCount; position++) {
            assertThat(BIGINT.getLong(page.getBlock(0), position)).isEqualTo(firstValue + position);
        }
    }

    private ConnectorPageSource createArrowPageSource(BigQueryReadClient client, MemoryContext memoryContext)
    {
        BigQuerySplit split = BigQuerySplit.forStream("trace", "stream", ARROW_SCHEMA.toJson(), COLUMNS, OptionalInt.empty());
        return new BigQueryStorageArrowPageSource(TYPE_MANAGER, client, executor, arrowBufferAllocator, 0, split, COLUMNS, memoryContext);
    }

    private ConnectorPageSource createAvroPageSource(BigQueryReadClient client, MemoryContext memoryContext)
    {
        BigQuerySplit split = BigQuerySplit.forStream("trace", "stream", AVRO_SCHEMA.toString(), COLUMNS, OptionalInt.empty());
        return new BigQueryStorageAvroPageSource(client, executor, TYPE_MANAGER, 0, split, COLUMNS, memoryContext);
    }

    private static ReadRowsResponse arrowResponse(long firstValue, int rowCount)
            throws IOException
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(ARROW_SCHEMA, allocator)) {
            BigIntVector vector = (BigIntVector) root.getVector(0);
            vector.allocateNew(rowCount);
            for (int row = 0; row < rowCount; row++) {
                vector.set(row, firstValue + row);
            }
            root.setRowCount(rowCount);
            try (ArrowRecordBatch batch = new VectorUnloader(root).getRecordBatch()) {
                MessageSerializer.serialize(new WriteChannel(Channels.newChannel(output)), batch);
            }
        }
        return ReadRowsResponse.newBuilder()
                .setArrowRecordBatch(com.google.cloud.bigquery.storage.v1.ArrowRecordBatch.newBuilder()
                        .setSerializedRecordBatch(ByteString.copyFrom(output.toByteArray())))
                .setRowCount(rowCount)
                .build();
    }

    private static ReadRowsResponse avroResponse(long firstValue, int rowCount)
            throws IOException
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(AVRO_SCHEMA);
        for (int row = 0; row < rowCount; row++) {
            GenericRecord record = new GenericData.Record(AVRO_SCHEMA);
            record.put("value", firstValue + row);
            writer.write(record, encoder);
        }
        encoder.flush();
        return ReadRowsResponse.newBuilder()
                .setAvroRows(AvroRows.newBuilder().setSerializedBinaryRows(ByteString.copyFrom(output.toByteArray())))
                .setRowCount(rowCount)
                .build();
    }

    private static class TestingBigQueryReadServer
            implements AutoCloseable
    {
        private final AtomicInteger readRowsRequests = new AtomicInteger();
        private final Server server;
        private final ManagedChannel channel;
        private final BigQueryReadClient client;

        public TestingBigQueryReadServer(List<ReadRowsResponse> responses)
                throws IOException
        {
            String serverName = InProcessServerBuilder.generateName();
            server = InProcessServerBuilder.forName(serverName)
                    .directExecutor()
                    .addService(new BigQueryReadGrpc.BigQueryReadImplBase()
                    {
                        @Override
                        public void readRows(ReadRowsRequest request, StreamObserver<ReadRowsResponse> responseObserver)
                        {
                            readRowsRequests.incrementAndGet();
                            responses.forEach(responseObserver::onNext);
                            responseObserver.onCompleted();
                        }
                    })
                    .build()
                    .start();
            channel = InProcessChannelBuilder.forName(serverName)
                    .directExecutor()
                    .build();
            client = BigQueryReadClient.create(BigQueryReadSettings.newBuilder()
                    .setCredentialsProvider(NoCredentialsProvider.create())
                    .setTransportChannelProvider(FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel)))
                    .build());
        }

        public BigQueryReadClient client()
        {
            return client;
        }

        public int readRowsRequests()
        {
            return readRowsRequests.get();
        }

        @Override
        public void close()
        {
            client.close();
            channel.shutdownNow();
            server.shutdownNow();
        }
    }
}
