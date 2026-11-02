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
package io.trino.metadata;

import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Arrays.stream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD) // getActiveQueryIds() is shared mutable state that affects the test outcome
final class TestMetadataManagerQueryCleanup
{
    private StandaloneQueryRunner queryRunner;
    private LanguageFunctionManager languageFunctionManager;

    @BeforeAll
    void setUp()
    {
        queryRunner = new StandaloneQueryRunner(testSessionBuilder().build());
        // Both connectors fail, so the assertions do not depend on the order the catalogs are cleaned up in
        queryRunner.installPlugin(new MockConnectorPlugin(mockConnector("first_cleanup", _ -> {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "First cleanup failed");
        })));
        queryRunner.installPlugin(new MockConnectorPlugin(mockConnector("second_cleanup", _ -> {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Second cleanup failed");
        })));
        queryRunner.createCatalog("first", "first_cleanup");
        queryRunner.createCatalog("second", "second_cleanup");
        languageFunctionManager = queryRunner.getPlannerContext().getLanguageFunctionManager();
    }

    @AfterAll
    void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
        languageFunctionManager = null;
    }

    /**
     * A connector failing to clean up must not stop the other catalogs of the query from being cleaned up
     * or leave the query's functions registered, and every connector failure must still be reported.
     */
    @Test
    void testCleanupContinuesAfterConnectorFailure()
    {
        assertThatThrownBy(() -> queryRunner.execute(
                queryRunner.getDefaultSession(),
                "SELECT count(*) FROM (SELECT 1 FROM first.tiny.t UNION ALL SELECT 1 FROM second.tiny.t)"))
                .hasMessageContaining("cleanup failed")
                .cause()
                .satisfies(failure -> assertThat(reportedFailures(failure))
                        .containsExactlyInAnyOrder("First cleanup failed", "Second cleanup failed"));

        assertThat(languageFunctionManager.getActiveQueryIds()).isEmpty();
    }

    private static List<String> reportedFailures(Throwable failure)
    {
        return Stream.concat(Stream.of(failure), stream(failure.getSuppressed()))
                .map(Throwable::getMessage)
                .collect(toImmutableList());
    }

    private static MockConnectorFactory mockConnector(String name, Consumer<ConnectorSession> cleanupQuery)
    {
        return MockConnectorFactory.builder()
                .withName(name)
                .withGetTableHandle((_, schemaTableName) -> new MockConnectorTableHandle(schemaTableName))
                .withCleanupQuery(cleanupQuery)
                .build();
    }
}
