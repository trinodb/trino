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
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.spi.type.TimestampType;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Optional;

import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestCreateTableColumnTypeCoercion
{
    private static final String catalogName = "mock";
    private QueryRunner queryRunner;

    private QueryRunner createQueryRunner()
    {
        Session session = testSessionBuilder()
                .setCatalog(catalogName)
                .setSchema("default")
                .build();
        QueryRunner queryRunner = new StandaloneQueryRunner(session);
        queryRunner.installPlugin(new MockConnectorPlugin(prepareConnectorFactory(catalogName)));
        queryRunner.createCatalog(catalogName, "mock", ImmutableMap.of());
        return queryRunner;
    }

    private MockConnectorFactory prepareConnectorFactory(String catalogName)
    {
        return MockConnectorFactory.builder()
                .withName(catalogName)
                .withGetTableHandle((session, schemaTableName) -> null)
                .withGetSupportedType((session, type) -> {
                    if (type instanceof TimestampType) {
                        return Optional.of(VARCHAR);
                    }
                    return Optional.empty();
                })
                .build();
    }

    @Test
    public void testIncompatibleTypeForCreateTableAsSelect()
    {
        assertTrinoExceptionThrownBy(() -> queryRunner.execute("CREATE TABLE test_incompatible_type AS SELECT TIMESTAMP '2020-09-27 12:34:56.999' a"))
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("Type 'timestamp(3)' is not compatible with the supplied type 'varchar' in getSupportedType");
    }

    @Test
    public void testIncompatibleTypeForCreateTableAsSelectWithNoData()
    {
        assertTrinoExceptionThrownBy(() -> queryRunner.execute("CREATE TABLE test_incompatible_type AS SELECT TIMESTAMP '2020-09-27 12:34:56.999' a WITH NO DATA"))
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("Type 'timestamp(3)' is not compatible with the supplied type 'varchar' in getSupportedType");
    }

    @BeforeAll
    public final void initQueryRunner()
    {
        this.queryRunner = createQueryRunner();
    }

    @AfterAll
    public final void destroyQueryRunner()
    {
        closeAllRuntimeException(queryRunner);
        queryRunner = null;
    }
}
