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

import com.google.common.io.Resources;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.MitmProxy;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Map;

public class TestBigQueryWithProxyTest
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        MitmProxy proxy = closeAfterClass(MitmProxy.builder()
                .withSSLCertificate(fromResources("proxy/cert.pem").toPath())
                .build());
        proxy.start();
        return BigQueryQueryRunner.builder()
                .setConnectorProperties(Map.of(
                        "bigquery.rpc-proxy.enabled", "true",
                        "bigquery.rpc-proxy.uri", proxy.getProxyEndpoint(),
                        "bigquery.rpc-proxy.truststore-path", fromResources("proxy/truststore.jks").getAbsolutePath(),
                        "bigquery.rpc-proxy.truststore-password", "123456"))
                .build();
    }

    @Test
    void testCreateTableAsSelect()
    {
        // This test covers all client (BigQuery, BigQueryReadClient and BigQueryWriteClient)
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test.test_ctas", "AS SELECT 42 x")) {
            assertQuery("SELECT * FROM " + table.getName(), "VALUES 42");
        }
    }

    private static File fromResources(String filename)
    {
        try {
            return new File(Resources.getResource(filename).toURI());
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
