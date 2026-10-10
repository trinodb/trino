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
package io.trino.plugin.ydb;

import io.trino.spi.connector.Connector;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestYdbPlugin
{
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testConnectorBootstrap(boolean testingClient)
    {
        YdbPlugin plugin = new YdbPlugin(testingClient ? new TestingYdbJdbcModule() : new YdbClientModule());
        Connector connector = getOnlyElement(plugin.getConnectorFactories())
                .create("ydb_bootstrap",
                        Map.of("connection-url", "jdbc:ydb:grpc://127.0.0.1:2136/local"),
                        new TestingConnectorContext());
        try {
            assertThat(connector.getPageSinkProvider()).isInstanceOf(YdbPageSinkProvider.class);
            assertThat(connector.getCapabilities()).contains(NOT_NULL_COLUMN_CONSTRAINT);
        }
        finally {
            connector.shutdown();
        }
    }
}
