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
package com.qubole.presto.kinesis.s3config;

import com.amazonaws.services.s3.AmazonS3URI;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.qubole.presto.kinesis.KinesisConnector;
import com.qubole.presto.kinesis.KinesisPlugin;
import com.qubole.presto.kinesis.KinesisStreamDescription;
import com.qubole.presto.kinesis.KinesisStreamFieldDescription;
import com.qubole.presto.kinesis.KinesisStreamFieldGroup;
import com.qubole.presto.kinesis.KinesisTableDescriptionSupplier;
import com.qubole.presto.kinesis.util.TestUtils;
import io.airlift.log.Logger;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

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
        log.info("Tested out URI1 : " + uri1.toString());

        AmazonS3URI uri2 = new AmazonS3URI("s3://some.big.bucket/long/complex/path");
        assertNotNull(uri2.getKey());
        assertNotNull(uri2.getBucket());

        assertEquals(uri2.toString(), "s3://some.big.bucket/long/complex/path");
        assertEquals(uri2.getBucket(), "some.big.bucket");
        assertEquals(uri2.getKey(), "long/complex/path");
        assertTrue(uri2.getRegion() == null);

        // info:
        log.info("Tested out URI2 : " + uri2.toString());

        AmazonS3URI uri3 = new AmazonS3URI("s3://presto.kinesis.config/unit-test/presto-kinesis");
        assertNotNull(uri3.getKey());
        assertNotNull(uri3.getBucket());

        assertEquals(uri3.toString(), "s3://presto.kinesis.config/unit-test/presto-kinesis");
        assertEquals(uri3.getBucket(), "presto.kinesis.config");
        assertEquals(uri3.getKey(), "unit-test/presto-kinesis");
    }

    @Parameters({
            "kinesis.tableDescriptionS3",
            "kinesis.awsAccessKey",
            "kinesis.awsSecretKey"
    })
    @Test
    public void testTableReading(String tableDescriptionS3, String accessKey, String secretKey)
    {
        // To run this test: setup an S3 bucket with a folder for unit testing, and put the
        // testtable.json file in that folder.

        // Create dependent objects, including the minimal config needed for this test
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("kinesis.table-description-dir", "etc/kinesis")
                .put("kinesis.table-descriptions-s3", tableDescriptionS3)
                .put("kinesis.default-schema", "kinesis")
                .put("kinesis.hide-internal-columns", "false")
                .put("kinesis.access-key", TestUtils.noneToBlank(accessKey))
                .put("kinesis.secret-key", TestUtils.noneToBlank(secretKey))
                .build();

        KinesisPlugin kinesisPlugin = TestUtils.createPluginInstance();
        KinesisConnector kinesisConnector = TestUtils.createConnector(kinesisPlugin, properties, false);
        Injector injector = kinesisPlugin.getInjector();
        assertNotNull(injector);

        // Get the supplier from the injector
        KinesisTableDescriptionSupplier supplier = TestUtils.getTableDescSupplier(injector);
        assertNotNull(supplier);

        S3TableConfigClient s3TableClient = injector.getInstance(S3TableConfigClient.class);
        assertNotNull(s3TableClient);
        assertTrue(s3TableClient.isUsingS3());

        // Sleep for 10 seconds to ensure that we've loaded the tables:
        try {
            Thread.sleep(10000);
            log.info("done sleeping, will now try to read the tables.");
        }
        catch (InterruptedException e) {
            log.error("interrupted ...");
        }

        // Read table definition and verify
        Map<SchemaTableName, KinesisStreamDescription> readMap = supplier.get();
        assertTrue(!readMap.isEmpty());

        SchemaTableName tblName = new SchemaTableName("prod", "test_table");
        KinesisStreamDescription desc = readMap.get(tblName);

        assertNotNull(desc);
        assertEquals(desc.getSchemaName(), "prod");
        assertEquals(desc.getTableName(), "test_table");
        assertEquals(desc.getStreamName(), "test_kinesis_stream");
        assertNotNull(desc.getMessage());

        // Obtain the message part and verify we can read its fields
        KinesisStreamFieldGroup grp = desc.getMessage();
        assertEquals(grp.getDataFormat(), "json");
        List<KinesisStreamFieldDescription> fieldList = grp.getFields();
        assertEquals(fieldList.size(), 4); // (4 fields in test_table.json)
    }
}
