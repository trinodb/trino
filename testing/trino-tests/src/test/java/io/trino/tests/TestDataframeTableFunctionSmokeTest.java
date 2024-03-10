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
import com.starburstdata.dataframe.DataframeException;
import com.starburstdata.dataframe.plan.LogicalPlan;
import com.starburstdata.dataframe.plan.TrinoPlan;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDataframeTableFunctionSmokeTest
        extends AbstractTestDataframe
{
    @Test
    public void testCompression()
    {
        Object result = queryRunner.execute(format("SELECT trino_plan FROM TABLE(analyze_logical_plan('%s'))", "$zstd:KLUv/WCCB9UTAJacWycAreoBPuZ5NTamzCNt0/AukrembwBiBADEBQlsTwocLEJEVVVVxQpTAFUASwDuLu0uDQh36e7u7l50YxfJhALBLgBNPabIVHEO7XDaW/zbhNO0w1nXYqonAAp4WGY1/AxZw1vFnisVGdeOoyIWCTIduHa4tzIJltkKf/b4TEV2mhGiFZoxgm/W6uznrVqcs1TgNHnL5C1z7Tw4SOBgQeoBgl4seqJMCQ2Mzt2M16AzgmR8HAZsLLphQunKZIlIDBA63c2gFhQQFAy6IvWCqkgP6FTouEuQzkefNiCH8IruD3ze45dfMOMSze31+uRPGOmkFfReVvPhi++B1jMr/1hFZ/Mbzrf6vRvm2tlEYFmE8uyWhGOxohVOf6Jf/1y6JaGbjbMwSdM+Al3fJiY8wRxD4c/+GAFu1ymu4882YyIstp7iZtpjzJKvv06TrYVlFf58LWTTcI8V0khe8ZJUzilfhI+sVUZRfDCqEdI4A3SooUEZIhJJiwpq5mACEbK6OhKglBzGkQwGMaYQc0oCGYkSlZQphQdTMJilUbudbRDbZYMlulURG5lBo9FFQz6+WjG37BqjCsNtDp/OHhVCDalnwIGCX6CE+w/AmEHBaiG12o5bOflYiFhXEgW00Y9wLq2jKfFBJWBK6HnJbiT95ISR/0SZuiycZlpnaYAs148YtW/yJaHR5foQaD5+LJ3Tdpe5Fv8EBPEHy8A5jir+cMXvRUaOrKrAWlIBFXKtcv4BMgQvjTYcCJaaJzkNgIGMWD0j84eMbsseJ+Gv6mCuoUlqqxnAI35EjM0nPGLMLTXTAMu0XZSkQ4TYCB3MQ7vvJlBKCDRkQ6s=")).getOnlyValue();
        TrinoPlan trinoPlan = TRINO_PLAN.fromJson((String) result);
        assertEquals(
                trinoPlan.getQueries(),
                ImmutableList.of(
                        """
                                SELECT *
                                FROM
                                  (
                                   SELECT *
                                   FROM
                                     (
                                      SELECT *
                                      FROM
                                        (
                                         SELECT *
                                         FROM
                                           (
                                            SELECT *
                                            FROM
                                              (
                                               SELECT
                                                 "_1" "a"
                                               , "_2" "b"
                                               FROM
                                                 (
                                                  SELECT
                                                    "_1"
                                                  , "_2"
                                                  FROM
                                                    (
                                                     SELECT
                                                       CAST("_1" AS BIGINT) "_1"
                                                     , CAST("_2" AS BIGINT) "_2"
                                                     FROM
                                                       (
                                 VALUES\s
                                                          ROW (CAST(1 AS BIGINT), CAST(2 AS BIGINT))
                                                        , ROW (CAST(3 AS BIGINT), CAST(4 AS BIGINT))
                                                        , ROW (CAST(5 AS BIGINT), CAST(6 AS BIGINT))
                                                        , ROW (CAST(7 AS BIGINT), CAST(8 AS BIGINT))
                                                        , ROW (CAST(9 AS BIGINT), CAST(10 AS BIGINT))
                                                     )  t ("_1", "_2")
                                                  )\s
                                               )\s
                                            )\s
                                            WHERE (("a" >= CAST(2 AS BIGINT)) AND ("a" <= CAST(6 AS BIGINT)))
                                         )\s
                                         WHERE (("a" >= CAST(2 AS BIGINT)) AND ("a" <= CAST(6 AS BIGINT)))
                                      )\s
                                      WHERE (("a" >= CAST(2 AS BIGINT)) AND ("a" <= CAST(6 AS BIGINT)))
                                   )\s
                                   WHERE (("a" >= CAST(2 AS BIGINT)) AND ("a" <= CAST(6 AS BIGINT)))
                                )\s
                                WHERE (("a" >= CAST(2 AS BIGINT)) AND ("a" <= CAST(6 AS BIGINT)))
                                """));
    }

    @Override
    protected void assertQuery(LogicalPlan logicalPlan, List<String> expectedQueries)
    {
        Object result = queryRunner.execute(format("SELECT trino_plan FROM TABLE(analyze_logical_plan('%s'))", LOGICAL_PLAN.toJson(logicalPlan).replace("'", "''"))).getOnlyValue();
        TrinoPlan trinoPlan = TRINO_PLAN.fromJson((String) result);
        assertEquals(trinoPlan.getQueries(), expectedQueries);
    }

    @Override
    protected void assertQueryFails(LogicalPlan logicalPlan, DataframeException.ErrorCode errorCode)
    {
        try {
            queryRunner.execute(format("SELECT trino_plan FROM TABLE(analyze_logical_plan('%s'", LOGICAL_PLAN.toJson(logicalPlan).replace("'", "''"))).getOnlyValue();
            fail(format("Logical plan expected to fail: %s, with error code: %s", logicalPlan, errorCode));
        }
        catch (DataframeException exception) {
            exception.addSuppressed(new Exception("Logical plan: " + logicalPlan));
            assertEquals(exception.getResponseEntity().getErrorCode(), errorCode);
        }
    }
}
