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
import io.trino.connector.MockConnectorPlugin;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.security.Identity;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Optional;

import static io.trino.connector.MockConnectorEntities.TPCH_NATION_SCHEMA;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestMetadataMismatch
{
    private static final String LOCAL_CATALOG = "local";
    private static final String MOCK_CATALOG = "mock";
    private static final String USER = "user";

    private static final Session SESSION = testSessionBuilder()
            .setCatalog(LOCAL_CATALOG)
            .setSchema(TINY_SCHEMA_NAME)
            .setIdentity(Identity.forUser(USER).build())
            .build();

    private final QueryAssertions assertions;

    public TestMetadataMismatch()
    {
        QueryRunner runner = new StandaloneQueryRunner(SESSION);

        runner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withGetColumns(_ -> TPCH_NATION_SCHEMA)
                .withGetInsertLayout((_, _) ->
                        Optional.of(new ConnectorTableLayout(ImmutableList.of("year"))))  // nonexistent column in TPCH_NATION_SCHEMA
                .build()));
        runner.createCatalog(MOCK_CATALOG, "mock", ImmutableMap.of());

        assertions = new QueryAssertions(runner);
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testGetInsertLayoutMismatchAgainstColumns()
    {
        assertThat(assertions.query("DELETE FROM mock.tiny.nation WHERE nationkey < 3"))
                .failure()
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                // use regex to match the error message to accommodate any ordering of the columns being printed
                .hasMessageMatching("Unable to determine field index for partitioning column 'year' \\(available columns: \\[('(nationkey|regionkey|name|comment)', ){3}'(nationkey|regionkey|name|comment)']\\)");
    }
}
