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
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.containers.Hive4HttpMetastoreFlociDataLake.Transport;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.google.common.collect.Maps.filterKeys;
import static io.trino.plugin.hive.containers.Hive4HttpMetastoreFlociDataLake.TRUSTSTORE_PASSWORD;
import static io.trino.plugin.hive.containers.Hive4HttpMetastoreFlociDataLake.TRUSTSTORE_RESOURCE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.TestContainers.getPathFromClassPathResource;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Runs the connector smoke tests against a Hive 4 metastore that serves Thrift over HTTPS,
 * using the metastore's own TLS and hostname verification.
 */
public class TestHiveHttpsThriftMetastoreConnectorSmokeTest
        extends TestHiveHttpThriftMetastoreConnectorSmokeTest
{
    @Override
    protected Transport transport()
    {
        return Transport.HTTPS;
    }

    @Override
    protected Map<String, String> transportProperties()
    {
        return ImmutableMap.of(
                "hive.metastore.thrift.client.ssl.trust-certificate", getPathFromClassPathResource(TRUSTSTORE_RESOURCE),
                "hive.metastore.thrift.client.ssl.trust-certificate-password", TRUSTSTORE_PASSWORD);
    }

    @Test
    public void testUntrustedCertificate()
    {
        String catalog = "hive_untrusted_" + randomNameSuffix();
        // same catalog without the trust store, so the JVM default trust store is used
        getQueryRunner().createCatalog(catalog, "hive", ImmutableMap.<String, String>builder()
                .putAll(filterKeys(catalogProperties(), key -> !key.startsWith("hive.metastore.thrift.client.ssl.")))
                .put("hive.metastore.http.client.additional-headers", "x-actor-username:hive")
                .put("hive.metastore.thrift.client.max-retries", "0")
                .buildOrThrow());

        assertThatThrownBy(() -> computeActual("SHOW SCHEMAS FROM " + catalog))
                .hasStackTraceContaining("PKIX path building failed");
    }
}
