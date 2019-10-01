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
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.metadata.TableHandle;
import io.prestosql.plugin.kinesis.util.EmbeddedKinesisStream;
import io.prestosql.plugin.kinesis.util.TestUtils;
import io.prestosql.security.AllowAllAccessControl;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.type.BigintType;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.tests.StandaloneQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static io.prestosql.transaction.TransactionBuilder.transaction;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertTrue;

/**
 * Note: this is an integration test that connects to AWS Kinesis.
 * <p>
 * Only run if you have an account setup where you can create streams and put/get records.
 * You may incur AWS charges if you run this test.  You probably want to setup an IAM
 * user for your CI server to use.
 */
@Test(singleThreaded = true)
public class TestMinimalFunctionality
{
    private static final Logger log = Logger.get(TestMinimalFunctionality.class);

    public static final Session SESSION = Session.builder(new SessionPropertyManager())
            .setIdentity(Identity.ofUser("user"))
            .setSource("source")
            .setCatalog("kinesis")
            .setSchema("default")
            .setTimeZoneKey(UTC_KEY)
            .setLocale(ENGLISH)
            .setQueryId(new QueryId("dummy"))
            .build();

    private EmbeddedKinesisStream embeddedKinesisStream;
    private String streamName;
    private StandaloneQueryRunner queryRunner;

    @Parameters({
            "kinesis.awsAccessKey",
            "kinesis.awsSecretKey"
    })
    @BeforeClass
    public void start(String accessKey, String secretKey)
            throws Exception
    {
        embeddedKinesisStream = new EmbeddedKinesisStream(TestUtils.noneToBlank(accessKey), TestUtils.noneToBlank(secretKey));
    }

    @AfterClass
    public void stop()
            throws Exception
    {
        embeddedKinesisStream.close();
    }

    @Parameters({
            "kinesis.awsAccessKey",
            "kinesis.awsSecretKey"
    })
    @BeforeMethod
    public void spinUp(String accessKey, String secretKey)
            throws Exception
    {
        System.setProperty("com.amazonaws.sdk.disableCbor", "true");
        streamName = "test_" + UUID.randomUUID().toString().replaceAll("-", "_");

        embeddedKinesisStream.createStream(2, streamName);
        this.queryRunner = new StandaloneQueryRunner(SESSION);
        Path tempDir = Files.createTempDirectory("tempdir");
        File baseFile = new File("src/test/resources/tableDescriptions/EmptyTable.json");
        File file = new File(tempDir.toAbsolutePath().toString() + "/" + streamName + ".json");

        try (Stream<String> lines = Files.lines(baseFile.toPath())) {
            List<String> replaced = lines
                    .map(line -> line.replaceAll("TABLE_NAME", streamName))
                    .map(line -> line.replaceAll("STREAM_NAME", streamName))
                    .collect(Collectors.toList());
            Files.write(file.toPath(), replaced);
        }
        TestUtils.installKinesisPlugin(queryRunner, tempDir.toAbsolutePath().toString(),
                TestUtils.noneToBlank(accessKey), TestUtils.noneToBlank(secretKey));
    }

    private void createMessages(String streamName, long count)
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
        PutRecordsResult result = embeddedKinesisStream.getKinesisClient().putRecords(putRecordsRequest);
    }

    @Test
    public void testStreamExists()
            throws Exception
    {
        // TODO: Was QualifiedTableName, is this OK:
        QualifiedObjectName name = new QualifiedObjectName("kinesis", "default", streamName);

        transaction(queryRunner.getTransactionManager(), new AllowAllAccessControl())
                .singleStatement()
                .execute(SESSION, session -> {
                    Optional<TableHandle> handle = queryRunner.getServer().getMetadata().getTableHandle(session, name);
                    assertTrue(handle.isPresent());
                });
    }

    @Test
    public void testStreamHasData()
            throws Exception
    {
        MaterializedResult result = queryRunner.execute("SELECT COUNT(1) FROM " + streamName);

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(0L)
                .build();

        assertEquals(result, expected);

        long count = 500L;
        createMessages(streamName, count);

        result = queryRunner.execute("SELECT COUNT(1) FROM " + streamName);

        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(count)
                .build();
        Thread.sleep(5000);
        assertEquals(result, expected);
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        embeddedKinesisStream.delteStream(streamName);
        queryRunner.close();
    }
}
