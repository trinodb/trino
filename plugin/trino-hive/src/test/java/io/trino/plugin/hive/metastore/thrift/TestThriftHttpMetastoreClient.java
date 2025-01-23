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
import io.trino.hive.thrift.metastore.TableMeta;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.plugin.hive.metastore.thrift.TestingThriftHttpMetastoreServer.TestingThriftRequestsHandler;
import io.trino.testing.TestingNodeManager;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.http.HttpHeaders;
import org.junit.jupiter.api.AfterAll;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestThriftHttpMetastoreClient
{
    private static final AutoCloseableCloser closer = AutoCloseableCloser.create();

    @BeforeAll
    public static void setup()
            throws Exception
    {
        File tempDir = Files.createTempDirectory(null).toFile();
        tempDir.deleteOnExit();
    }

    @AfterAll
    public static void tearDown()
            throws Exception
    {
        closer.close();
    }

    @Test
    public void testHttpThriftConnection()
            throws Exception
    {
        ThriftHttpMetastoreConfig config = new ThriftHttpMetastoreConfig();
        config.setAuthenticationMode(ThriftHttpMetastoreConfig.AuthenticationMode.BEARER);
        config.setAdditionalHeaders("key1:value1, key2:value2");

        TestingThriftRequestsHandler handler = new TestingThriftRequestsHandler()
        {
            @Override
            public List<String> getAllDatabases()
            {
                return ImmutableList.of("testDbName");
            }

            @Override
            public Database getDatabase(String name)
                    throws NoSuchObjectException
            {
                if (name.equals("testDbName")) {
                    return new Database(name, "testOwner", "testLocation", Map.of("key", "value"));
                }
                throw new NoSuchObjectException("Database does not exist");
            }

            @Override
            public List<String> getTables(String databaseName, String pattern)
            {
                if (databaseName.equals("testDbName")) {
                    return ImmutableList.of("testTable1", "testTable2");
                }
                return ImmutableList.of();
            }

            @Override
            public List<String> getTablesByType(String databaseName, String pattern, String tableType)
            {
                if (databaseName.equals("testDbName")) {
                    return ImmutableList.of("testTable3", "testTable4");
                }
                return ImmutableList.of();
            }
        };

        try (TestingThriftHttpMetastoreServer metastoreServer = new TestingThriftHttpMetastoreServer(handler, new TestRequestHeaderInterceptor())) {
            ThriftMetastoreClientFactory factory = new HttpThriftMetastoreClientFactory(config, new TestingNodeManager(), OpenTelemetry.noop());
            URI metastoreUri = URI.create("http://localhost:" + metastoreServer.getPort());
            ThriftMetastoreClient client = factory.create(
                    metastoreUri, Optional.empty());
            assertThat(client.getAllDatabases()).containsExactly("testDbName");
            assertThat(client.getDatabase("testDbName")).isEqualTo(new Database("testDbName", "testOwner", "testLocation", Map.of("key", "value")));
            assertThat(client.getTableMeta("testDbName"))
                    .extracting(TableMeta::getTableName)
                    .containsExactlyInAnyOrder("testTable1", "testTable2", "testTable3", "testTable4");
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
