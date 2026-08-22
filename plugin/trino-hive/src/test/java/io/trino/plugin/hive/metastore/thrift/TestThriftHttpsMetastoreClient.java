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

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.hive.thrift.metastore.NoSuchObjectException;
import io.trino.testing.TestingNodeManager;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestThriftHttpsMetastoreClient
{
    private static final TestingThriftHttpMetastoreServer.TestingThriftRequestsHandler HANDLER = new TestingThriftHttpMetastoreServer.TestingThriftRequestsHandler()
    {
        @Override
        public java.util.List<String> getAllDatabases()
        {
            return ImmutableList.of("testDbName");
        }

        @Override
        public io.trino.hive.thrift.metastore.Database getDatabase(String name)
                throws NoSuchObjectException
        {
            if (name.equals("testDbName")) {
                return new io.trino.hive.thrift.metastore.Database(name, "testOwner", "testLocation", java.util.Map.of("key", "value"));
            }
            throw new NoSuchObjectException("Database does not exist");
        }

        @Override
        public java.util.List<String> getTables(String databaseName, String pattern)
        {
            return ImmutableList.of();
        }

        @Override
        public java.util.List<String> getTablesByType(String databaseName, String pattern, String tableType)
        {
            return ImmutableList.of();
        }
    };

    @Test
    public void testHttpsThriftConnectionFailsWithoutTruststore()
            throws Exception
    {
        ThriftHttpMetastoreConfig config = new ThriftHttpMetastoreConfig()
                .setAuthenticationMode(ThriftHttpMetastoreConfig.AuthenticationMode.BEARER)
                .setHttpBearerToken("test-token")
                .setVerifyHostname(false);

        try (TestingThriftHttpsMetastoreServer metastoreServer = new TestingThriftHttpsMetastoreServer(HANDLER, _ -> {})) {
            ThriftMetastoreClientFactory factory = new HttpThriftMetastoreClientFactory(
                    config,
                    TestingNodeManager.builder().build(),
                    OpenTelemetry.noop());
            URI metastoreUri = metastoreServer.getHttpsUri();
            ThriftMetastoreClient client = factory.create(metastoreUri, Optional.empty());

            assertThatThrownBy(client::getAllDatabases)
                    .hasMessageContaining("PKIX path building failed");
        }
    }

    @Test
    public void testHttpsThriftConnectionWithTruststore()
            throws Exception
    {
        File truststore = new File(Resources.getResource("thrift-http-metastore-https/truststore.jks").toURI());
        ThriftHttpMetastoreConfig config = new ThriftHttpMetastoreConfig()
                .setAuthenticationMode(ThriftHttpMetastoreConfig.AuthenticationMode.BEARER)
                .setHttpBearerToken("test-token")
                .setTruststorePath(truststore)
                .setTruststorePassword("changeit")
                .setVerifyHostname(false);

        try (TestingThriftHttpsMetastoreServer metastoreServer = new TestingThriftHttpsMetastoreServer(HANDLER, _ -> {})) {
            ThriftMetastoreClientFactory factory = new HttpThriftMetastoreClientFactory(
                    config,
                    TestingNodeManager.builder().build(),
                    OpenTelemetry.noop());
            URI metastoreUri = metastoreServer.getHttpsUri();
            ThriftMetastoreClient client = factory.create(metastoreUri, Optional.empty());

            assertThat(client.getAllDatabases()).containsExactly("testDbName");
        }
    }

    @Test
    public void testHttpsThriftConnectionRequiresBearerToken()
            throws Exception
    {
        ThriftHttpMetastoreConfig config = new ThriftHttpMetastoreConfig()
                .setAuthenticationMode(ThriftHttpMetastoreConfig.AuthenticationMode.BEARER);

        try (TestingThriftHttpsMetastoreServer metastoreServer = new TestingThriftHttpsMetastoreServer(HANDLER, _ -> {})) {
            ThriftMetastoreClientFactory factory = new HttpThriftMetastoreClientFactory(
                    config,
                    TestingNodeManager.builder().build(),
                    OpenTelemetry.noop());

            assertThatThrownBy(() -> factory.create(metastoreServer.getHttpsUri(), Optional.empty()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("hive.metastore.http.client.bearer-token");
        }
    }
}
