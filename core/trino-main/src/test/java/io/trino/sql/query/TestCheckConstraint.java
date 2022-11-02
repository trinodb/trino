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
package io.trino.sql.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.testing.LocalQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.connector.MockConnectorEntities.TPCH_NATION_DATA;
import static io.trino.connector.MockConnectorEntities.TPCH_NATION_SCHEMA;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestCheckConstraint
{
    private static final String LOCAL_CATALOG = "local";
    private static final String MOCK_CATALOG = "mock";
    private static final String USER = "user";

    private static final Session SESSION = testSessionBuilder()
            .setCatalog(LOCAL_CATALOG)
            .setSchema(TINY_SCHEMA_NAME)
            .setIdentity(Identity.forUser(USER).build())
            .build();

    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        LocalQueryRunner runner = LocalQueryRunner.builder(SESSION).build();

        runner.createCatalog(LOCAL_CATALOG, new TpchConnectorFactory(1), ImmutableMap.of());

        MockConnectorFactory mock = MockConnectorFactory.builder()
                .withGetColumns(schemaTableName -> {
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation"))) {
                        return TPCH_NATION_SCHEMA;
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_with_invalid_check_constraint"))) {
                        return TPCH_NATION_SCHEMA;
                    }
                    throw new UnsupportedOperationException();
                })
                .withCheckConstraints(schemaTableName -> {
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation"))) {
                        return ImmutableList.of("nationkey > 100");
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_with_invalid_check_constraint"))) {
                        return ImmutableList.of("invalid_function(nationkey) > 100");
                    }
                    throw new UnsupportedOperationException();
                })
                .withData(schemaTableName -> {
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation"))) {
                        return TPCH_NATION_DATA;
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_with_invalid_check_constraint"))) {
                        return TPCH_NATION_DATA;
                    }
                    throw new UnsupportedOperationException();
                })
                .build();

        runner.createCatalog(MOCK_CATALOG, mock, ImmutableMap.of());

        assertions = new QueryAssertions(runner);
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testInsert()
    {
        assertions.query("INSERT INTO mock.tiny.nation VALUES (101, 'POLAND', 0, 'No comment')")
                .assertThat()
                .skippingTypesCheck()
                .matches("SELECT BIGINT '1'");

        // Outside allowed row filter
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation VALUES (26, 'POLAND', 0, 'No comment')"))
                .hasMessage("Cannot insert row that does not match to a check constraint: (\"nationkey\" > CAST(100 AS bigint))");
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation VALUES "
                + "(26, 'POLAND', 0, 'No comment'),"
                + "(27, 'HOLLAND', 0, 'A comment')"))
                .hasMessage("Cannot insert row that does not match to a check constraint: (\"nationkey\" > CAST(100 AS bigint))");
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation VALUES "
                + "(26, 'POLAND', 0, 'No comment'),"
                + "(27, 'HOLLAND', 0, 'A comment')"))
                .hasMessage("Cannot insert row that does not match to a check constraint: (\"nationkey\" > CAST(100 AS bigint))");
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation(nationkey) VALUES (null)"))
                .hasMessage("Cannot insert row that does not match to a check constraint: (\"nationkey\" > CAST(100 AS bigint))");
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation(regionkey) VALUES (0)"))
                .hasMessage("Cannot insert row that does not match to a check constraint: (\"nationkey\" > CAST(100 AS bigint))");
    }

    @Test
    public void testInsertUnsupportedConstraint()
    {
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation_with_invalid_check_constraint VALUES (101, 'POLAND', 0, 'No comment')"))
                .hasMessageContaining("Function 'invalid_function' not registered");
    }
}
