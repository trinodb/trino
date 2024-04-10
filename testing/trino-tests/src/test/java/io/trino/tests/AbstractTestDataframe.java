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
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.testing.DistributedQueryRunner;
import jakarta.ws.rs.core.MediaType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class AbstractTestDataframe
{
    protected static final JsonCodec<TrinoPlan> TRINO_PLAN = jsonCodec(TrinoPlan.class);
    protected static final JsonCodec<LogicalPlan> LOGICAL_PLAN = jsonCodec(LogicalPlan.class);

    protected DistributedQueryRunner queryRunner;
    protected HttpClient client;

    @Test
    public void testTablePlan()
    {
        assertQuery(
                new UnresolvedRelation(TypeCoercionMode.DEFAULT, Optional.empty(), "tpch.tiny.customer"),
                ImmutableList.of(
                        """
                                SELECT *
                                FROM
                                  tpch.tiny.customer
                                  """));
    }

    protected void assertQuery(LogicalPlan logicalPlan, List<String> expectedQueries)
    {
        logicalPlan = LOGICAL_PLAN.fromJson(LOGICAL_PLAN.toJson(logicalPlan));
        Request request = prepareRequest(logicalPlan);
        TrinoPlan trinoPlan = resolveLogicalPlan(request);
        assertEquals(trinoPlan.getQueries(), expectedQueries);
    }

    protected void assertQueryFails(LogicalPlan logicalPlan, DataframeException.ErrorCode errorCode)
    {
        try {
            Request request = prepareRequest(logicalPlan);
            resolveLogicalPlan(request);
            fail(format("Logical plan expected to fail: %s, with error code: %s", logicalPlan, errorCode));
        }
        catch (DataframeException exception) {
            exception.addSuppressed(new Exception("Logical plan: " + logicalPlan));
            assertEquals(exception.getResponseEntity().getErrorCode(), errorCode);
        }
    }

    private TrinoPlan resolveLogicalPlan(Request request)
    {
        try {
            return client.execute(request, createJsonResponseHandler(TRINO_PLAN));
        }
        catch (Exception e) {
            throw new DataframeException(e.getMessage(), DataframeException.ErrorCode.SQL_ERROR);
        }
    }

    private Request prepareRequest(LogicalPlan logicalPlan)
    {
        return preparePost()
                .setHeader("X-Trino-User", "admin")
                .setHeader("Content-Type", MediaType.APPLICATION_JSON_TYPE.withCharset(StandardCharsets.UTF_8.name()).toString())
                .setUri(uriBuilderFrom(queryRunner.getCoordinator().getBaseUrl())
                        .replacePath("/v1/dataframe/plan").build())
                .setBodyGenerator(jsonBodyGenerator(LOGICAL_PLAN, logicalPlan))
                .build();
    }

    @BeforeAll
    public void setUp()
            throws Exception
    {
        queryRunner = DistributedQueryRunner.builder(TEST_SESSION)
                .addExtraProperty("dataframe-api-enabled", "true")
                .build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());
        client = new JettyHttpClient();
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        queryRunner.close();
        queryRunner = null;
        client.close();
        client = null;
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
