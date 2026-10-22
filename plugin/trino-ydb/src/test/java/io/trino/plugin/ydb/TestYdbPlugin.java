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

import io.trino.plugin.jdbc.JdbcTransactionHandle;
import io.trino.spi.QueryId;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorSecurityContext;
import io.trino.spi.connector.SchemaRoutineName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

public class TestYdbPlugin
{
    @Test
    public void testConnectorBootstrap()
    {
        ConnectorFactory factory = getOnlyElement(new YdbPlugin().getConnectorFactories());
        for (String catalogName : List.of("ydb_first", "ydb_second")) {
            Connector connector = factory.create(
                    catalogName,
                    Map.of("connection-url", "jdbc:ydb:grpc://127.0.0.1:2136/local"),
                    new TestingConnectorContext());
            try {
                assertThat(connector.getPageSourceProvider()).isNotNull();
                assertThat(connector.getCapabilities()).contains(NOT_NULL_COLUMN_CONSTRAINT);
                ConnectorSecurityContext context = new ConnectorSecurityContext(
                        new JdbcTransactionHandle(),
                        ConnectorIdentity.ofUser("test"),
                        new QueryId("native_execute"));
                assertTrinoExceptionThrownBy(() -> connector.getAccessControl()
                        .checkCanExecuteProcedure(context, new SchemaRoutineName("system", "execute")))
                        .hasErrorCode(NOT_SUPPORTED);
                connector.getAccessControl().checkCanExecuteProcedure(context, new SchemaRoutineName("system", "flush_metadata_cache"));
            }
            finally {
                connector.shutdown();
            }
        }
    }
}
