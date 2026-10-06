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
package io.trino.plugin.openlineage;

import io.airlift.units.Duration;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacet;
import io.openlineage.client.OpenLineage.InputField;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClient;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.openlineage.client.OpenLineage.RunEvent.EventType.COMPLETE;
import static io.openlineage.client.OpenLineage.RunEvent.EventType.FAIL;
import static io.openlineage.client.OpenLineage.RunEvent.EventType.START;
import static io.trino.testing.assertions.Assert.assertEventually;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Execution(ExecutionMode.SAME_THREAD)
final class TestOpenLineageDatasetExclusion
        extends AbstractTestQueryFramework
{
    private static final Duration TIMEOUT = Duration.valueOf("10s");
    private static final String EXCLUDE_PATTERN = ".*\\.excluded_.*";

    private final OpenLineageMemoryTransport transport = new OpenLineageMemoryTransport();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return OpenLineageListenerQueryRunner.builder()
                .setCustomEventListener(new OpenLineageListener(
                        new OpenLineage(URI.create("https://github.com/trinodb/trino/plugin/trino-openlineage")),
                        new OpenLineageClient(transport),
                        new OpenLineageListenerConfig()
                                .setTrinoURI(URI.create("http://trino-integration-test:1337"))
                                .setDatasetExcludePattern(EXCLUDE_PATTERN)))
                .build();
    }

    @AfterEach
    void clearEvents()
    {
        transport.clearProcessedEvents();
    }

    @Test
    void testExcludedOutputIsStripped()
    {
        String queryId = execute("CREATE TABLE marquez.default.excluded_output AS SELECT * FROM tpch.tiny.nation");

        RunEvent completed = awaitEvent(queryId, COMPLETE);
        assertThat(inputNames(completed)).containsExactly("tpch.tiny.nation");
        assertThat(completed.getOutputs()).isEmpty();
    }

    @Test
    void testExcludedInputIsStripped()
    {
        execute("CREATE TABLE marquez.default.excluded_input AS SELECT * FROM tpch.tiny.nation");
        String queryId = execute("CREATE TABLE marquez.default.kept_from_excluded AS SELECT * FROM marquez.default.excluded_input");

        RunEvent completed = awaitEvent(queryId, COMPLETE);
        assertThat(completed.getInputs()).isEmpty();
        assertThat(outputNames(completed)).containsExactly("marquez.default.kept_from_excluded");

        ColumnLineageDatasetFacet columnLineage = columnLineage(completed);
        assertThat(columnLineage.getFields().getAdditionalProperties())
                .containsOnlyKeys("nationkey", "name", "regionkey", "comment");
        assertThat(columnLineage.getFields().getAdditionalProperties().values())
                .allSatisfy(field -> assertThat(field.getInputFields()).isEmpty());
        assertThat(columnLineage.getDataset()).isEmpty();
    }

    @Test
    void testOnlyExcludedSideOfJoinIsStripped()
    {
        execute("CREATE TABLE marquez.default.excluded_join_side AS SELECT * FROM tpch.tiny.nation");
        String queryId = execute(
                """
                CREATE TABLE marquez.default.kept_join AS
                SELECT n.name, e.regionkey
                FROM tpch.tiny.nation n
                JOIN marquez.default.excluded_join_side e ON n.nationkey = e.nationkey
                """);

        RunEvent completed = awaitEvent(queryId, COMPLETE);
        assertThat(inputNames(completed)).containsExactly("tpch.tiny.nation");
        assertThat(outputNames(completed)).containsExactly("marquez.default.kept_join");

        ColumnLineageDatasetFacet columnLineage = columnLineage(completed);
        assertThat(columnLineage.getFields().getAdditionalProperties().get("name").getInputFields())
                .extracting(InputField::getName)
                .containsExactly("tpch.tiny.nation");
        assertThat(columnLineage.getFields().getAdditionalProperties().get("regionkey").getInputFields())
                .isEmpty();
        assertThat(columnLineage.getDataset())
                .extracting(InputField::getName)
                .containsOnly("tpch.tiny.nation");
    }

    @Test
    void testDeleteFromExcludedTableHasNoLineage()
    {
        execute("CREATE SCHEMA blackhole.exclusion");
        execute("CREATE TABLE blackhole.exclusion.excluded_delete AS SELECT custkey FROM tpch.tiny.customer");
        String queryId = execute(
                """
                DELETE FROM blackhole.exclusion.excluded_delete
                WHERE custkey IN (SELECT custkey FROM tpch.tiny.customer WHERE acctbal < 5000)
                """);

        RunEvent completed = awaitEvent(queryId, COMPLETE);
        assertThat(inputNames(completed)).containsExactly("tpch.tiny.customer");
        assertThat(completed.getOutputs()).isEmpty();
    }

    @Test
    void testStartEventIsStillSent()
    {
        String queryId = execute("CREATE TABLE marquez.default.excluded_start AS SELECT * FROM tpch.tiny.region");

        // START never carries datasets; the point is that it is still emitted for an excluded output
        awaitEvent(queryId, COMPLETE);
        awaitEvent(queryId, START);
    }

    @Test
    void testFailedQueryIsStripped()
    {
        RunEvent failed = runFailingInsert("marquez.default.excluded_failed");
        assertThat(inputNames(failed)).containsExactly("tpch.tiny.nation");
        assertThat(failed.getOutputs()).isEmpty();
    }

    @Test
    void testFailedQueryKeepsNotExcludedOutput()
    {
        RunEvent failed = runFailingInsert("marquez.default.kept_failed");
        assertThat(inputNames(failed)).containsExactly("tpch.tiny.nation");
        assertThat(outputNames(failed)).containsExactly("marquez.default.kept_failed");
    }

    private RunEvent runFailingInsert(String table)
    {
        execute("CREATE TABLE %s AS SELECT * FROM tpch.tiny.nation WITH NO DATA".formatted(table));
        assertThatThrownBy(() -> execute(
                """
                INSERT INTO %s
                SELECT nationkey / (regionkey - regionkey), name, regionkey, comment
                FROM tpch.tiny.nation
                """.formatted(table)))
                .hasMessageContaining("Division by zero");

        AtomicReference<RunEvent> found = new AtomicReference<>();
        assertEventually(TIMEOUT, () -> {
            List<RunEvent> failedEvents = runEvents().stream()
                    .filter(event -> event.getEventType() == FAIL)
                    .filter(event -> event.getJob().getFacets().getSql().getQuery().contains("INSERT INTO " + table))
                    .collect(toImmutableList());
            assertThat(failedEvents).hasSize(1);
            found.set(failedEvents.getFirst());
        });
        return found.get();
    }

    private String execute(String sql)
    {
        return getQueryRunner()
                .executeWithPlan(getSession(), sql)
                .queryId()
                .toString();
    }

    private RunEvent awaitEvent(String queryId, RunEvent.EventType eventType)
    {
        AtomicReference<RunEvent> found = new AtomicReference<>();
        assertEventually(TIMEOUT, () -> {
            List<RunEvent> matching = runEvents().stream()
                    .filter(event -> event.getEventType() == eventType)
                    .filter(event -> event.getJob().getName().equals(queryId))
                    .collect(toImmutableList());
            assertThat(matching).hasSize(1);
            found.set(matching.getFirst());
        });
        return found.get();
    }

    private List<RunEvent> runEvents()
    {
        return transport.getProcessedEvents().stream()
                .filter(RunEvent.class::isInstance)
                .map(RunEvent.class::cast)
                .collect(toImmutableList());
    }

    private static List<String> inputNames(RunEvent event)
    {
        return event.getInputs().stream()
                .map(OpenLineage.InputDataset::getName)
                .collect(toImmutableList());
    }

    private static List<String> outputNames(RunEvent event)
    {
        return event.getOutputs().stream()
                .map(OutputDataset::getName)
                .collect(toImmutableList());
    }

    private static ColumnLineageDatasetFacet columnLineage(RunEvent event)
    {
        assertThat(event.getOutputs()).hasSize(1);
        return event.getOutputs().getFirst().getFacets().getColumnLineage();
    }
}
