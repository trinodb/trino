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
import io.opentelemetry.api.OpenTelemetry;
import io.trino.hive.thrift.metastore.Database;
import io.trino.hive.thrift.metastore.NoSuchObjectException;
import io.trino.testing.TestingNodeManager;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.http.HttpHeaders;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestThriftHttpMetastoreClient
{
    private static ThriftMetastore delegate;

    @BeforeAll
    public static void setup()
            throws Exception
    {
        File tempDir = Files.createTempDirectory(null).toFile();
        tempDir.deleteOnExit();
        delegate = testingThriftHiveMetastoreBuilder().metastoreClient(createFakeMetastoreClient()).build();
    }

    private static ThriftMetastoreClient createFakeMetastoreClient()
    {
        return new MockThriftMetastoreClient()
        {
            @Override
            public Database getDatabase(String databaseName)
                    throws NoSuchObjectException
            {
                if (databaseName.equals("testDbName")) {
                    return new Database(databaseName, "testOwner", "testLocation", Map.of("key", "value"));
                }
                throw new NoSuchObjectException("Database does not exist");
            }

            @Override
            public List<String> getAllDatabases()
            {
                return ImmutableList.of("testDbName");
            }
        };
    }

    @Test
    public void testHttpThriftConnection()
            throws Exception
    {
        ThriftHttpMetastoreConfig config = new ThriftHttpMetastoreConfig();
        config.setAuthenticationMode(ThriftHttpMetastoreConfig.AuthenticationMode.BEARER);
        config.setAdditionalHeaders("key1:value1, key2:value2");

        try (TestingThriftHttpMetastoreServer metastoreServer = new TestingThriftHttpMetastoreServer(delegate, new TestRequestHeaderInterceptor())) {
            ThriftMetastoreClientFactory factory = new HttpThriftMetastoreClientFactory(config, new TestingNodeManager(), OpenTelemetry.noop());
            URI metastoreUri = URI.create("http://localhost:" + metastoreServer.getPort());
            ThriftMetastoreClient client = factory.create(
                    metastoreUri, Optional.empty());
            assertThat(client.getAllDatabases()).containsExactly("testDbName");
            assertThat(client.getDatabase("testDbName")).isEqualTo(new Database("testDbName", "testOwner", "testLocation", Map.of("key", "value")));
            // negative case
            assertThatThrownBy(() -> client.getDatabase("does-not-exist")).isInstanceOf(NoSuchObjectException.class);
        }
    }

    private static class TestRequestHeaderInterceptor
            implements Consumer<HttpServletRequest>
    {
        @Override
        public void accept(HttpServletRequest httpServletRequest)
        {
            assertThat(Collections.list(httpServletRequest.getHeaderNames())).contains("key1", "key2");
            assertThat(httpServletRequest.getHeader("key1")).isEqualTo("value1");
            assertThat(httpServletRequest.getHeader("key2")).isEqualTo("value2");
            assertThat(httpServletRequest.getHeader(HttpHeaders.AUTHORIZATION)).isNull();
        }
    }
}
