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

import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.SessionPropertyManager;
import io.trino.metadata.TableHandle;
import io.trino.plugin.kinesis.util.EmbeddedKinesisStream;
import io.trino.plugin.kinesis.util.TestUtils;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.StandaloneQueryRunner;
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

import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.transaction.TransactionBuilder.transaction;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
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
    private QueryAssertions assertions;

    @Parameters({
            "kinesis.awsAccessKey",
            "kinesis.awsSecretKey"
    })
    @BeforeClass
    public void start(String accessKey, String secretKey)
    {
        embeddedKinesisStream = new EmbeddedKinesisStream(TestUtils.noneToBlank(accessKey), TestUtils.noneToBlank(secretKey));
    }

    @AfterClass(alwaysRun = true)
    public void stop()
    {
        embeddedKinesisStream.close();
        embeddedKinesisStream = null;
    }

    @Parameters({
            "kinesis.awsAccessKey",
            "kinesis.awsSecretKey"
    })
    @BeforeMethod
    public void spinUp(String accessKey, String secretKey)
            throws Exception
    {
        streamName = "test_" + UUID.randomUUID().toString().replaceAll("-", "_");

        embeddedKinesisStream.createStream(2, streamName);
        this.queryRunner = new StandaloneQueryRunner(SESSION);
        assertions = new QueryAssertions(queryRunner);
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
    {
        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(streamName);
        List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
            putRecordsRequestEntry.setData(ByteBuffer.wrap(UUID.randomUUID().toString().getBytes(UTF_8)));
            putRecordsRequestEntry.setPartitionKey(Long.toString(i));
            putRecordsRequestEntryList.add(putRecordsRequestEntry);
        }

        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        embeddedKinesisStream.getKinesisClient().putRecords(putRecordsRequest);
    }

    @Test
    public void testStreamExists()
    {
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
    {
        assertThat(assertions.query("SELECT COUNT(1) FROM " + streamName))
                .matches("VALUES 0");

        long count = 500L;
        createMessages(streamName, count);

        assertThat(assertions.query("SELECT COUNT(1) FROM " + streamName))
                .matches("VALUES %s".formatted(count));
    }

    @AfterMethod
    public void tearDown()
    {
        embeddedKinesisStream.deleteStream(streamName);
        queryRunner.close();
        queryRunner = null;
    }
}
