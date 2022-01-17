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

import com.amazonaws.services.s3.AmazonS3URI;
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

import java.util.Map;

import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestS3TableConfigClient
{
    private static final Logger log = Logger.get(TestS3TableConfigClient.class);

    @Test
    public void testS3URIValues()
    {
        // Verify that S3URI values will work:
        AmazonS3URI uri1 = new AmazonS3URI("s3://our.data.warehouse/prod/client_actions");
        assertNotNull(uri1.getKey());
        assertNotNull(uri1.getBucket());

        assertEquals(uri1.toString(), "s3://our.data.warehouse/prod/client_actions");
        assertEquals(uri1.getBucket(), "our.data.warehouse");
        assertEquals(uri1.getKey(), "prod/client_actions");
        assertTrue(uri1.getRegion() == null);

        // show info:
        log.info("Tested out URI1 : %s", uri1);

        AmazonS3URI uri2 = new AmazonS3URI("s3://some.big.bucket/long/complex/path");
        assertNotNull(uri2.getKey());
        assertNotNull(uri2.getBucket());

        assertEquals(uri2.toString(), "s3://some.big.bucket/long/complex/path");
        assertEquals(uri2.getBucket(), "some.big.bucket");
        assertEquals(uri2.getKey(), "long/complex/path");
        assertTrue(uri2.getRegion() == null);

        // info:
        log.info("Tested out URI2 : %s", uri2);

        AmazonS3URI uri3 = new AmazonS3URI("s3://trino.kinesis.config/unit-test/trino-kinesis");
        assertNotNull(uri3.getKey());
        assertNotNull(uri3.getBucket());

        assertEquals(uri3.toString(), "s3://trino.kinesis.config/unit-test/trino-kinesis");
        assertEquals(uri3.getBucket(), "trino.kinesis.config");
        assertEquals(uri3.getKey(), "unit-test/trino-kinesis");
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
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
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

        KinesisMetadata metadata = (KinesisMetadata) kinesisConnector.getMetadata(new ConnectorTransactionHandle() {});
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
