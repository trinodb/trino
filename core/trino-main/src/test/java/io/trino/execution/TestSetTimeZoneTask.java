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

import com.google.common.collect.ImmutableList;
import io.trino.client.NodeVersion;
import io.trino.execution.warnings.WarningCollector;
import io.trino.spi.TrinoException;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.type.TimeZoneNotSupportedException;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SetTimeZone;
import io.trino.sql.tree.StringLiteral;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.SystemSessionProperties.TIME_ZONE_ID;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.sql.tree.IntervalLiteral.IntervalField.HOUR;
import static io.trino.sql.tree.IntervalLiteral.IntervalField.MINUTE;
import static io.trino.sql.tree.IntervalLiteral.Sign.NEGATIVE;
import static io.trino.sql.tree.IntervalLiteral.Sign.POSITIVE;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestSetTimeZoneTask
{
    private ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
    private LocalQueryRunner localQueryRunner;

    @BeforeClass
    public void setUp()
    {
        localQueryRunner = LocalQueryRunner.create(TEST_SESSION);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        executor = null;
        localQueryRunner.close();
        localQueryRunner = null;
    }

    @Test
    public void testSetTimeZoneLocal()
    {
        QueryStateMachine stateMachine = createQueryStateMachine("SET TIME ZONE LOCAL");
        SetTimeZone setTimeZone = new SetTimeZone(
                new NodeLocation(1, 1),
                Optional.empty());
        executeSetTimeZone(setTimeZone, stateMachine);

        assertThat(stateMachine.getResetSessionProperties()).hasSize(1);
        assertThat(stateMachine.getResetSessionProperties()).contains(TIME_ZONE_ID);
    }

    @Test
    public void testSetTimeZoneStringLiteral()
    {
        QueryStateMachine stateMachine = createQueryStateMachine("SET TIME ZONE 'America/Los_Angeles'");
        SetTimeZone setTimeZone = new SetTimeZone(
                new NodeLocation(1, 1),
                Optional.of(new StringLiteral("America/Los_Angeles")));
        executeSetTimeZone(setTimeZone, stateMachine);

        Map<String, String> setSessionProperties = stateMachine.getSetSessionProperties();
        assertThat(setSessionProperties).hasSize(1);
        assertEquals(setSessionProperties.get(TIME_ZONE_ID), "America/Los_Angeles");
    }

    @Test
    public void testSetTimeZoneVarcharFunctionCall()
    {
        QueryStateMachine stateMachine = createQueryStateMachine("SET TIME ZONE concat_ws('/', 'America', 'Los_Angeles')");
        SetTimeZone setTimeZone = new SetTimeZone(
                new NodeLocation(1, 1),
                Optional.of(new FunctionCall(
                        new NodeLocation(1, 15),
                        QualifiedName.of(ImmutableList.of(new Identifier(new NodeLocation(1, 15), "concat_ws", false))),
                        ImmutableList.of(
                                new StringLiteral(
                                        new NodeLocation(1, 25),
                                        "/"),
                                new StringLiteral(
                                        new NodeLocation(1, 30),
                                        "America"),
                                new StringLiteral(
                                        new NodeLocation(1, 41),
                                        "Los_Angeles")))));
        executeSetTimeZone(setTimeZone, stateMachine);

        Map<String, String> setSessionProperties = stateMachine.getSetSessionProperties();
        assertThat(setSessionProperties).hasSize(1);
        assertEquals(setSessionProperties.get(TIME_ZONE_ID), "America/Los_Angeles");
    }

    @Test
    public void testSetTimeZoneInvalidFunctionCall()
    {
        QueryStateMachine stateMachine = createQueryStateMachine("SET TIME ZONE e()");
        SetTimeZone setTimeZone = new SetTimeZone(
                new NodeLocation(1, 1),
                Optional.of(new FunctionCall(
                        new NodeLocation(1, 15),
                        QualifiedName.of(ImmutableList.of(new Identifier(new NodeLocation(1, 15), "e", false))),
                        ImmutableList.of())));

        assertThatThrownBy(() -> executeSetTimeZone(setTimeZone, stateMachine))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Expected expression of varchar or interval day-time type, but 'e()' has double type");
    }

    @Test
    public void testSetTimeZoneStringLiteralInvalidZoneId()
    {
        QueryStateMachine stateMachine = createQueryStateMachine("SET TIME ZONE 'Matrix/Zion'");
        SetTimeZone setTimeZone = new SetTimeZone(
                new NodeLocation(1, 1),
                Optional.of(new StringLiteral("Matrix/Zion")));
        assertThatThrownBy(() -> executeSetTimeZone(setTimeZone, stateMachine))
                .isInstanceOf(TimeZoneNotSupportedException.class);
    }

    @Test
    public void testSetTimeZoneIntervalLiteral()
    {
        QueryStateMachine stateMachine = createQueryStateMachine("SET TIME ZONE INTERVAL '10' HOUR");
        SetTimeZone setTimeZone = new SetTimeZone(
                new NodeLocation(1, 1),
                Optional.of(new IntervalLiteral("10", POSITIVE, HOUR)));
        executeSetTimeZone(setTimeZone, stateMachine);

        Map<String, String> setSessionProperties = stateMachine.getSetSessionProperties();
        assertThat(setSessionProperties).hasSize(1);
        assertEquals(setSessionProperties.get(TIME_ZONE_ID), "+10:00");
    }

    @Test
    public void testSetTimeZoneIntervalDayTimeTypeFunctionCall()
    {
        QueryStateMachine stateMachine = createQueryStateMachine("SET TIME ZONE parse_duration('8h')");
        SetTimeZone setTimeZone = new SetTimeZone(
                new NodeLocation(1, 1),
                Optional.of(new FunctionCall(
                        new NodeLocation(1, 24),
                        QualifiedName.of(ImmutableList.of(new Identifier(new NodeLocation(1, 24), "parse_duration", false))),
                        ImmutableList.of(
                                new StringLiteral(
                                        new NodeLocation(1, 39),
                                        "8h")))));
        executeSetTimeZone(setTimeZone, stateMachine);

        Map<String, String> setSessionProperties = stateMachine.getSetSessionProperties();
        assertThat(setSessionProperties).hasSize(1);
        assertEquals(setSessionProperties.get(TIME_ZONE_ID), "+08:00");
    }

    @Test
    public void testSetTimeZoneIntervalDayTimeTypeInvalidFunctionCall()
    {
        QueryStateMachine stateMachine = createQueryStateMachine("SET TIME ZONE parse_duration('3601s')");
        SetTimeZone setTimeZone = new SetTimeZone(
                new NodeLocation(1, 1),
                Optional.of(new FunctionCall(
                        new NodeLocation(1, 24),
                        QualifiedName.of(ImmutableList.of(new Identifier(new NodeLocation(1, 24), "parse_duration", false))),
                        ImmutableList.of(
                                new StringLiteral(
                                        new NodeLocation(1, 39),
                                        "3601s")))));
        assertThatThrownBy(() -> executeSetTimeZone(setTimeZone, stateMachine))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Invalid TIME ZONE offset interval: interval contains seconds");
    }

    @Test
    public void testSetTimeZoneIntervalLiteralGreaterThanOffsetTimeZoneMax()
    {
        QueryStateMachine stateMachine = createQueryStateMachine("SET TIME ZONE INTERVAL '15' HOUR");
        SetTimeZone setTimeZone = new SetTimeZone(
                new NodeLocation(1, 1),
                Optional.of(new IntervalLiteral("15", POSITIVE, HOUR)));
        assertThatThrownBy(() -> executeSetTimeZone(setTimeZone, stateMachine))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Invalid offset minutes 900");
    }

    @Test
    public void testSetTimeZoneIntervalLiteralLessThanOffsetTimeZoneMin()
    {
        QueryStateMachine stateMachine = createQueryStateMachine("SET TIME ZONE INTERVAL -'15' HOUR");
        SetTimeZone setTimeZone = new SetTimeZone(
                new NodeLocation(1, 1),
                Optional.of(new IntervalLiteral("15", NEGATIVE, HOUR)));
        assertThatThrownBy(() -> executeSetTimeZone(setTimeZone, stateMachine))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Invalid offset minutes -900");
    }

    @Test
    public void testSetTimeIntervalLiteralZoneHourToMinute()
    {
        QueryStateMachine stateMachine = createQueryStateMachine("SET TIME ZONE INTERVAL -'08:00' HOUR TO MINUTE");
        SetTimeZone setTimeZone = new SetTimeZone(
                new NodeLocation(1, 1),
                Optional.of(new IntervalLiteral("8", NEGATIVE, HOUR, Optional.of(MINUTE))));
        executeSetTimeZone(setTimeZone, stateMachine);

        Map<String, String> setSessionProperties = stateMachine.getSetSessionProperties();
        assertThat(setSessionProperties).hasSize(1);
        assertEquals(setSessionProperties.get(TIME_ZONE_ID), "-08:00");
    }

    private QueryStateMachine createQueryStateMachine(String query)
    {
        return QueryStateMachine.begin(
                Optional.empty(),
                query,
                Optional.empty(),
                TEST_SESSION,
                URI.create("fake://uri"),
                new ResourceGroupId("test"),
                false,
                localQueryRunner.getTransactionManager(),
                localQueryRunner.getAccessControl(),
                executor,
                localQueryRunner.getMetadata(),
                WarningCollector.NOOP,
                createPlanOptimizersStatsCollector(),
                Optional.empty(),
                true,
                new NodeVersion("test"));
    }

    private void executeSetTimeZone(SetTimeZone setTimeZone, QueryStateMachine stateMachine)
    {
        SetTimeZoneTask task = new SetTimeZoneTask(localQueryRunner.getPlannerContext(), localQueryRunner.getAccessControl());
        getFutureValue(task.execute(setTimeZone, stateMachine, emptyList(), WarningCollector.NOOP));
    }
}
