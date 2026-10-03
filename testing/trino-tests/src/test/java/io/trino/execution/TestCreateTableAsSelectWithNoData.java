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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.Test;

import java.util.OptionalLong;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

final class TestCreateTableAsSelectWithNoData
        extends AbstractTestQueryFramework
{
    private static final String CATALOG = "mock";

    @Override
    protected QueryRunner createQueryRunner()
    {
        QueryRunner queryRunner = new StandaloneQueryRunner(testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema("default")
                .build());
        queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withName(CATALOG)
                // Report the destination table as not yet existing so CREATE TABLE AS SELECT proceeds.
                .withGetTableHandle((_, _) -> null)
                // Simulates a connector that supports creating empty table structures (createTable, the
                // default no-op) but not writing data (beginCreateTable).
                .withBeginCreateTable((_, _, _, _, _) -> {
                    throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with data");
                })
                .build()));
        queryRunner.createCatalog(CATALOG, CATALOG, ImmutableMap.of());
        return queryRunner;
    }

    @Test
    void testCtasWithNoDataSucceeds()
    {
        assertThat(getQueryRunner().execute("CREATE TABLE test_no_data AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(id, name) WITH NO DATA").getUpdateCount())
                .isEqualTo(OptionalLong.of(0));
    }

    @Test
    void testCtasWithDataFails()
    {
        assertTrinoExceptionThrownBy(() -> getQueryRunner().execute("CREATE TABLE test_with_data AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(id, name)"))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("This connector does not support creating tables with data");
    }
}
