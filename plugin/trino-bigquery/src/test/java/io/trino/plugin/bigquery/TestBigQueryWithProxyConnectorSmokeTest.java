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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.MitmProxy;

import java.io.File;
import java.net.URISyntaxException;

public class TestBigQueryWithProxyConnectorSmokeTest
        extends BaseBigQueryConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        MitmProxy proxy = closeAfterClass(MitmProxy.builder()
                .withSSLCertificate(fromResources("proxy/cert.pem").toPath())
                .build());
        proxy.start();
        QueryRunner queryRunner = BigQueryQueryRunner.createQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.of(
                        "bigquery.rpc-proxy.enabled", "true",
                        "bigquery.rpc-proxy.uri", proxy.getProxyEndpoint(),
                        "bigquery.rpc-proxy.truststore-path", fromResources("proxy/truststore.jks").getAbsolutePath(),
                        "bigquery.rpc-proxy.truststore-password", "123456"),
                REQUIRED_TPCH_TABLES);

        return queryRunner;
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
