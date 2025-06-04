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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorMetadata;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHiveConnectorFactory
{
    @Test
    public void testGetClient()
    {
        assertCreateConnector("thrift://localhost:1234");
        assertCreateConnector("thrift://localhost:1234,thrift://192.0.2.3:5678");

        assertCreateConnectorFails("abc", "metastoreUri scheme is missing: abc");
        assertCreateConnectorFails("thrift://:8090", "metastoreUri host is missing: thrift://:8090");
        assertCreateConnectorFails("thrift://localhost", "metastoreUri port is missing: thrift://localhost");
        assertCreateConnectorFails("abc::", "metastoreUri scheme must be thrift: abc::");
        assertCreateConnectorFails("", "metastoreUris must specify at least one URI");
        assertCreateConnectorFails("thrift://localhost:1234,thrift://test-1", "metastoreUri port is missing: thrift://test-1");
    }

    private static void assertCreateConnector(String metastoreUri)
    {
        Map<String, String> config = ImmutableMap.of(
                "bootstrap.quiet", "true",
                "hive.metastore.uri", metastoreUri);

        Connector connector = new HiveConnectorFactory().create("hive-test", config, new TestingConnectorContext());
        ConnectorTransactionHandle transaction = connector.beginTransaction(READ_UNCOMMITTED, true, true);
        assertThat(connector.getMetadata(SESSION, transaction)).isInstanceOf(ClassLoaderSafeConnectorMetadata.class);
        assertThat(connector.getSplitManager()).isInstanceOf(ClassLoaderSafeConnectorSplitManager.class);
        assertThat(connector.getPageSourceProvider()).isInstanceOf(ConnectorPageSourceProvider.class);
        connector.commit(transaction);
    }

    private static void assertCreateConnectorFails(String metastoreUri, String exceptionString)
    {
        assertThatThrownBy(() -> assertCreateConnector(metastoreUri))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining(exceptionString);
    }
}
