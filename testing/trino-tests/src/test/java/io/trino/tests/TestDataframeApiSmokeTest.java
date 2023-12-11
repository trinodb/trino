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
package io.trino.tests;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starburstdata.dataframe.DataframeException;
import com.starburstdata.dataframe.analyzer.TypeCoercionMode;
import com.starburstdata.dataframe.plan.LogicalPlan;
import com.starburstdata.dataframe.plan.TrinoPlan;
import com.starburstdata.dataframe.plan.leaf.UnresolvedRelation;
import io.airlift.json.JsonCodec;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.testing.DistributedQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDataframeApiSmokeTest
{
    private static final JsonCodec<TrinoPlan> TRINO_PLAN = jsonCodec(TrinoPlan.class);
    private static final JsonCodec<LogicalPlan> LOGICAL_PLAN = jsonCodec(LogicalPlan.class);
    private DistributedQueryRunner runner;

    @Test
    public void testTablePlan()
    {
        assertQuery(
                new UnresolvedRelation(TypeCoercionMode.DEFAULT, "tpch.tiny.customer"),
                ImmutableList.of(
                        """
                                SELECT *
                                FROM
                                  tpch.tiny.customer
                                  """));
    }

    protected void assertQuery(LogicalPlan logicalPlan, List<String> expectedQueries)
    {
        Object result = runner.execute(format("SELECT trino_plan FROM TABLE(analyze_logical_plan('%s'))", LOGICAL_PLAN.toJson(logicalPlan).replace("'", "''"))).getOnlyValue();
        TrinoPlan trinoPlan = TRINO_PLAN.fromJson((String) result);
        assertEquals(trinoPlan.getQueries(), expectedQueries);
    }

    protected void assertQueryFails(LogicalPlan logicalPlan, DataframeException.ErrorCode errorCode)
    {
        try {
            runner.execute(format("SELECT trino_plan FROM TABLE(analyze_logical_plan('%s'", LOGICAL_PLAN.toJson(logicalPlan).replace("'", "''"))).getOnlyValue();
            fail(format("Logical plan expected to fail: %s, with error code: %s", logicalPlan, errorCode));
        }
        catch (DataframeException exception) {
            exception.addSuppressed(new Exception("Logical plan: " + logicalPlan));
            assertEquals(exception.getResponseEntity().getErrorCode(), errorCode);
        }
    }

    @BeforeAll
    public void setUp()
            throws Exception
    {
        runner = DistributedQueryRunner.builder(TEST_SESSION)
                .addExtraProperty("dataframe-api-enabled", "true")
                .build();

        runner.installPlugin(new TpchPlugin());
        runner.createCatalog("tpch", "tpch", ImmutableMap.of());
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        runner.close();
        runner = null;
    }

    private static TestingTrinoServer runTestServer()
    {
        TestingTrinoServer queryRunner = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.of("dataframe-api-enabled", "true"))
                .build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());
        return queryRunner;
    }
}
