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
package io.trino.plugin.kinesis;

import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.plugin.kinesis.util.MockKinesisClient;
import io.trino.plugin.kinesis.util.TestUtils;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TransactionBuilder.transaction;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Test record access and querying along with all associated setup.
 * <p>
 * This is a lighter weight integration test that exercises more parts of
 * the plug in without requiring an actual Kinesis connection.  It uses the mock
 * kinesis client so no AWS activity will occur.
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestRecordAccess
{
    private static final Logger log = Logger.get(TestRecordAccess.class);

    private static final Session SESSION = testSessionBuilder()
            .setCatalog("kinesis")
            .setSchema("default")
            .build();

    private String dummyStreamName;
    private String jsonStreamName;
    private String jsonGzipCompressStreamName;
    private String jsonAutomaticCompressStreamName;
    private QueryRunner queryRunner;
    private MockKinesisClient mockClient;

    @BeforeAll
    public void start()
    {
        dummyStreamName = "test123";
        jsonStreamName = "sampleTable";
        jsonGzipCompressStreamName = "sampleGzipCompressTable";
        jsonAutomaticCompressStreamName = "sampleAutomaticCompressTable";
        queryRunner = new StandaloneQueryRunner(SESSION);
        mockClient = TestUtils.installKinesisPlugin(queryRunner);
    }

    @AfterAll
    public void stop()
    {
        queryRunner.close();
        queryRunner = null;
        mockClient.close();
    }

    private void createDummyMessages(String streamName, int count)
    {
        PutRecordsRequest.Builder putRecordsRequest = PutRecordsRequest.builder();
        putRecordsRequest.streamName(streamName);
        List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            PutRecordsRequestEntry putRecordsRequestEntry = PutRecordsRequestEntry.builder()
                    .data(SdkBytes.fromByteArray(UUID.randomUUID().toString().getBytes(UTF_8)))
                    .partitionKey(Long.toString(i))
                    .build();
            putRecordsRequestEntryList.add(putRecordsRequestEntry);
        }

        putRecordsRequest.records(putRecordsRequestEntryList);
        mockClient.putRecords(putRecordsRequest.build());
    }

    private void createJsonMessages(String streamName, int count, int idStart, boolean compress)
    {
        String jsonFormat = "{\"id\" : %d, \"name\" : \"%s\"}";
        PutRecordsRequest.Builder putRecordsRequest = PutRecordsRequest.builder()
                .streamName(streamName);
        List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            PutRecordsRequestEntry.Builder putRecordsRequestEntry = PutRecordsRequestEntry.builder();
            long id = idStart + i;
            String name = UUID.randomUUID().toString();
            String jsonVal = format(jsonFormat, id, name);

            // ? with StandardCharsets.UTF_8
            if (compress) {
                putRecordsRequestEntry.data(SdkBytes.fromByteArray(compressMessage(jsonVal.getBytes(UTF_8))));
            }
            else {
                putRecordsRequestEntry.data(SdkBytes.fromByteArray(jsonVal.getBytes(UTF_8)));
            }
            putRecordsRequestEntry.partitionKey(Long.toString(id));
            putRecordsRequestEntryList.add(putRecordsRequestEntry.build());
        }

        putRecordsRequest.records(putRecordsRequestEntryList);
        mockClient.putRecords(putRecordsRequest.build());
    }

    private static byte[] compressMessage(byte[] data)
    {
        try (ByteArrayOutputStream byteOS = new ByteArrayOutputStream()) {
            GZIPOutputStream gzipOS = new GZIPOutputStream(byteOS);
            gzipOS.write(data);
            gzipOS.close(); //Explict close is needed to end the stream
            return byteOS.toByteArray();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testStreamExists()
    {
        QualifiedObjectName name = new QualifiedObjectName("kinesis", "default", dummyStreamName);

        Metadata metadata = queryRunner.getPlannerContext().getMetadata();
        transaction(queryRunner.getTransactionManager(), metadata, new AllowAllAccessControl())
                .singleStatement()
                .execute(SESSION, session -> {
                    Optional<TableHandle> handle = metadata.getTableHandle(session, name);
                    assertThat(handle.isPresent()).isTrue();
                });
        log.info("Completed first test (access table handle)");
    }

    @Test
    public void testStreamHasData()
    {
        MaterializedResult result = queryRunner.execute("Select count(1) from " + dummyStreamName);
        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(0)
                .build();

        assertThat(result.getRowCount()).isEqualTo(expected.getRowCount());

        int count = 500;
        createDummyMessages(dummyStreamName, count);

        result = queryRunner.execute("SELECT count(1) from " + dummyStreamName);

        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(count)
                .build();

        assertThat(result.getRowCount()).isEqualTo(expected.getRowCount());
        log.info("Completed second test (select counts)");
    }

    @Test
    public void testJsonStream()
    {
        testJsonStream(4, 0, jsonStreamName);
        testJsonStream(0, 4, jsonGzipCompressStreamName);
        testJsonStream(2, 2, jsonAutomaticCompressStreamName);
    }

    private void testJsonStream(int uncompressedMessages, int compressedMessages, String streamName)
    {
        // Simple case: add a few specific items, query object and internal fields:
        if (uncompressedMessages > 0) {
            createJsonMessages(streamName, uncompressedMessages, 100, false);
        }
        if (compressedMessages > 0) {
            createJsonMessages(streamName, compressedMessages, 100 + uncompressedMessages, true);
        }
        MaterializedResult result = queryRunner.execute("Select id, name, _shard_id, _message_length, _message from " + streamName + " where _message_length >= 1");
        assertThat(result.getRowCount()).isEqualTo(uncompressedMessages + compressedMessages);

        List<Type> types = result.getTypes();
        assertThat(types.size()).isEqualTo(5);
        assertThat(types.get(0).toString()).isEqualTo("bigint");
        assertThat(types.get(1).toString()).isEqualTo("varchar");
        log.info("Types : %s", types);

        List<MaterializedRow> rows = result.getMaterializedRows();
        assertThat(rows.size()).isEqualTo(uncompressedMessages + compressedMessages);
        for (MaterializedRow row : rows) {
            assertThat(row.getFieldCount()).isEqualTo(5);
            assertThat((long) row.getFields().get(0) >= 100).isTrue();
            log.info("ROW: %s", row);
        }
    }
}
