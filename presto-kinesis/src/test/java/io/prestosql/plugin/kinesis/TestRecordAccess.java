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
package io.prestosql.plugin.kinesis;

import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.TableHandle;
import io.prestosql.plugin.kinesis.util.MockKinesisClient;
import io.prestosql.plugin.kinesis.util.TestUtils;
import io.prestosql.security.AllowAllAccessControl;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.Type;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import io.prestosql.tests.StandaloneQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.transaction.TransactionBuilder.transaction;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Test record access and querying along with all associated setup.
 * <p>
 * This is a lighter weight integration test that exercises more parts of
 * the plug in without requiring an actual Kinesis connection.  It uses the mock
 * kinesis client so no AWS activity will occur.
 */
@Test(singleThreaded = true)
public class TestRecordAccess
{
    private static final Logger log = Logger.get(TestRecordAccess.class);

    private static final Session SESSION = testSessionBuilder()
            .setCatalog("kinesis")
            .setSchema("default")
            .build();

    private String dummyStreamName;
    private String jsonStreamName;
    private StandaloneQueryRunner queryRunner;
    private MockKinesisClient mockClient;

    @BeforeClass
    public void start()
            throws Exception
    {
        dummyStreamName = "test123";
        jsonStreamName = "sampleTable";
        this.queryRunner = new StandaloneQueryRunner(SESSION);
        mockClient = TestUtils.installKinesisPlugin(queryRunner);
    }

    @AfterClass
    public void stop()
            throws Exception
    {
        queryRunner.close();
    }

    private void createDummyMessages(String streamName, int count)
            throws Exception
    {
        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(streamName);
        List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
            putRecordsRequestEntry.setData(ByteBuffer.wrap(UUID.randomUUID().toString().getBytes()));
            putRecordsRequestEntry.setPartitionKey(Long.toString(i));
            putRecordsRequestEntryList.add(putRecordsRequestEntry);
        }

        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        mockClient.putRecords(putRecordsRequest);
    }

    private void createJsonMessages(String streamName, int count, int idStart)
            throws Exception
    {
        String jsonFormat = "{\"id\" : %d, \"name\" : \"%s\"}";
        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(streamName);
        List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
            long id = idStart + i;
            String name = UUID.randomUUID().toString();
            String jsonVal = String.format(jsonFormat, id, name);

            // ? with StandardCharsets.UTF_8
            putRecordsRequestEntry.setData(ByteBuffer.wrap(jsonVal.getBytes()));
            putRecordsRequestEntry.setPartitionKey(Long.toString(id));
            putRecordsRequestEntryList.add(putRecordsRequestEntry);
        }

        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        mockClient.putRecords(putRecordsRequest);
    }

    @Test
    public void testStreamExists()
            throws Exception
    {
        QualifiedObjectName name = new QualifiedObjectName("kinesis", "default", dummyStreamName);

        transaction(queryRunner.getTransactionManager(), new AllowAllAccessControl())
                .singleStatement()
                .execute(SESSION, session -> {
                    Optional<TableHandle> handle = queryRunner.getServer().getMetadata().getTableHandle(session, name);
                    assertTrue(handle.isPresent());
                });
        log.info("Completed first test (access table handle)");
    }

    @Test
    public void testStreamHasData()
            throws Exception
    {
        MaterializedResult result = queryRunner.execute("Select count(1) from " + dummyStreamName);
        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(0)
                .build();

        assertEquals(result.getRowCount(), expected.getRowCount());

        int count = 500;
        createDummyMessages(dummyStreamName, count);

        result = queryRunner.execute("SELECT count(1) from " + dummyStreamName);

        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(count)
                .build();

        assertEquals(result.getRowCount(), expected.getRowCount());
        log.info("Completed second test (select counts)");
    }

    @Test
    public void testJsonStream()
            throws Exception
    {
        // Simple case: add a few specific items, query object and internal fields:
        createJsonMessages(jsonStreamName, 4, 100);

        MaterializedResult result = queryRunner.execute("Select id, name, _shard_id, _message_length, _message from " + jsonStreamName + " where _message_length >= 1");
        assertEquals(result.getRowCount(), 4);

        List<Type> types = result.getTypes();
        assertEquals(types.size(), 5);
        assertEquals(types.get(0).toString(), "bigint");
        assertEquals(types.get(1).toString(), "varchar");
        log.info("Types : " + types.toString());

        List<MaterializedRow> rows = result.getMaterializedRows();
        assertEquals(rows.size(), 4);
        for (MaterializedRow row : rows) {
            assertEquals(row.getFieldCount(), 5);
            log.info("ROW: " + row.toString());
        }
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        // If desired clear messages or streams depending on the test being conducted!
    }
}
