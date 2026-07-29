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
package io.trino.execution;

import com.google.common.collect.ImmutableMap;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SaveMode;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Optional;
import java.util.OptionalLong;

import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestCreateTableAsSelectWithNoData
{
    private static final String CATALOG = "mock";

    private QueryRunner queryRunner;

    @BeforeAll
    void setUp()
    {
        queryRunner = new StandaloneQueryRunner(testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema("default")
                .build());
        queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withName(CATALOG)
                .withMetadataWrapper(NoWriteConnectorMetadata::new)
                .build()));
        queryRunner.createCatalog(CATALOG, CATALOG, ImmutableMap.of());
    }

    @AfterAll
    void tearDown()
    {
        closeAllRuntimeException(queryRunner);
        queryRunner = null;
    }

    @Test
    void testCtasWithNoDataSucceeds()
    {
        assertThat(queryRunner.execute("CREATE TABLE test_no_data AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(id, name) WITH NO DATA").getUpdateCount())
                .isEqualTo(OptionalLong.of(0));
    }

    @Test
    void testCtasWithDataFails()
    {
        assertTrinoExceptionThrownBy(() -> queryRunner.execute("CREATE TABLE test_with_data AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(id, name)"))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("This connector does not support creating tables with data");
    }

    // Simulates a connector that supports creating empty table structures (createTable) but not
    // writing data (beginCreateTable). This matches connectors like Iceberg-Snowflake.
    private static class NoWriteConnectorMetadata
            implements ConnectorMetadata
    {
        private final ConnectorMetadata delegate;

        NoWriteConnectorMetadata(ConnectorMetadata delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
        {
            delegate.createTable(session, tableMetadata, saveMode);
        }

        @Override
        public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace)
        {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with data");
        }
    }
}
