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
package io.trino.plugin.kinesis.s3config;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.plugin.kinesis.KinesisConnector;
import io.trino.plugin.kinesis.KinesisMetadata;
import io.trino.plugin.kinesis.KinesisPlugin;
import io.trino.plugin.kinesis.KinesisTableHandle;
import io.trino.plugin.kinesis.util.TestUtils;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.s3.S3Uri;

import java.net.URI;
import java.util.Map;

import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Test(singleThreaded = true)
public class TestS3TableConfigClient
{
    private static final Logger log = Logger.get(TestS3TableConfigClient.class);

    @Test
    public void testS3URIValues()
    {
        // Verify that S3URI values will work:
        URI uri1 = URI.create("s3://our.data.warehouse/prod/client_actions");
        S3Uri s3Uri1 = S3Uri.builder().uri(uri1).build();
        assertNotNull(s3Uri1.bucket().get());
        assertNotNull(s3Uri1.key().get());

        assertEquals(s3Uri1.toString(), "s3://our.data.warehouse/prod/client_actions");
        assertEquals(s3Uri1.bucket().get(), "our.data.warehouse");
        assertEquals(s3Uri1.key().get(), "prod/client_actions");

        // show info:
        log.info("Tested out URI1 : %s", s3Uri1.toString());

        URI uri2 = URI.create("s3://some.big.bucket/long/complex/path");
        S3Uri s3Uri2 = S3Uri.builder().uri(uri2).build();
        assertNotNull(s3Uri2.bucket().get());
        assertNotNull(s3Uri2.key().get());

        assertEquals(s3Uri2.toString(), "s3://some.big.bucket/long/complex/path");
        assertEquals(s3Uri2.bucket().get(), "some.big.bucket");
        assertEquals(s3Uri2.key().get(), "long/complex/path");

        // info:
        log.info("Tested out URI2 : %s", uri2);

        URI uri3 = URI.create("s3://trino.kinesis.config/unit-test/trino-kinesis");
        S3Uri s3Uri3 = S3Uri.builder().uri(uri3).build();
        assertNotNull(s3Uri3.bucket().get());
        assertNotNull(s3Uri3.key().get());

        assertEquals(uri3.toString(), "s3://trino.kinesis.config/unit-test/trino-kinesis");
        assertEquals(uri3.getHost(), "trino.kinesis.config");
        assertEquals(uri3.getPath().substring(1), "unit-test/trino-kinesis");
    }

    @Parameters({
            "kinesis.test-table-description-location",
            "kinesis.awsAccessKey",
            "kinesis.awsSecretKey"
    })
    @Test
    public void testTableReading(String tableDescriptionS3, String accessKey, String secretKey)
    {
        // To run this test: setup an S3 bucket with a folder for unit testing, and put
        // MinimalTable.json in that folder.

        // Create dependent objects, including the minimal config needed for this test
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("kinesis.table-description-location", tableDescriptionS3)
                .put("kinesis.default-schema", "kinesis")
                .put("kinesis.hide-internal-columns", "false")
                .put("kinesis.access-key", TestUtils.noneToBlank(accessKey))
                .put("kinesis.secret-key", TestUtils.noneToBlank(secretKey))
                .buildOrThrow();

        KinesisPlugin kinesisPlugin = new KinesisPlugin();
        KinesisConnector kinesisConnector = TestUtils.createConnector(kinesisPlugin, properties, false);

        // Sleep for 10 seconds to ensure that we've loaded the tables:
        try {
            Thread.sleep(10000);
            log.info("done sleeping, will now try to read the tables.");
        }
        catch (InterruptedException e) {
            log.error("interrupted ...");
        }

        KinesisMetadata metadata = (KinesisMetadata) kinesisConnector.getMetadata(SESSION, new ConnectorTransactionHandle() {});
        SchemaTableName tblName = new SchemaTableName("default", "test123");
        KinesisTableHandle tableHandle = metadata.getTableHandle(SESSION, tblName);
        assertNotNull(metadata);
        SchemaTableName tableSchemaName = tableHandle.toSchemaTableName();
        assertEquals(tableSchemaName.getSchemaName(), "default");
        assertEquals(tableSchemaName.getTableName(), "test123");
        assertEquals(tableHandle.getStreamName(), "test123");
        assertEquals(tableHandle.getMessageDataFormat(), "json");
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(SESSION, tableHandle);
        assertEquals(columnHandles.size(), 12);
    }
}
