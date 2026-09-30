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
package io.trino.plugin.ydb;

import io.trino.plugin.base.mapping.DefaultIdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.BasicRelationStatistics;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestYdbJoinCondition
{
    @Test
    public void testEqualityGrammar()
    {
        assertThat(YdbJoinCondition.parse(" `left``key` = `right=key` ")).contains(new YdbJoinCondition("left`key", "right=key"));
        for (String condition : List.of(
                "`left` = ?",
                "`left` >= `right`",
                "`left` = `right` OR true",
                "CAST(`left` AS Int64) = `right`",
                "`left` = `right` + 1",
                "`left` = `unterminated",
                "`left` IS NOT DISTINCT FROM `right`",
                "left = right")) {
            assertThat(YdbJoinCondition.parse(condition)).as(condition).isEmpty();
        }
    }

    @Test
    public void testModernJoinOwnershipAndParameterOrder()
    {
        YdbClient client = client();
        JdbcColumnHandle leftColumn = new JdbcColumnHandle("left_key", YdbTypeUtils.toTypeHandle(BIGINT).orElseThrow(), BIGINT);
        JdbcColumnHandle rightColumn = new JdbcColumnHandle("right_key", YdbTypeUtils.toTypeHandle(BIGINT).orElseThrow(), BIGINT);
        QueryParameter leftParameter = new QueryParameter(BIGINT, Optional.of(11L));
        QueryParameter rightParameter = new QueryParameter(BIGINT, Optional.of(22L));
        PreparedQuery left = new PreparedQuery("SELECT ? AS `left_key`", List.of(leftParameter));
        PreparedQuery right = new PreparedQuery("SELECT ? AS `right_key`", List.of(rightParameter));
        for (JoinType type : JoinType.values()) {
            for (String condition : List.of("`left_alias` = `right_alias`", "`right_alias` = `left_alias`")) {
                PreparedQuery joined = client.implementJoin(
                        SESSION,
                        type,
                        left,
                        Map.of(leftColumn, "left_alias"),
                        right,
                        Map.of(rightColumn, "right_alias"),
                        List.of(new ParameterizedExpression(condition, List.of())),
                        statistics()).orElseThrow();
                assertThat(joined.query()).contains("l.`left_key` = r.`right_key`");
                assertThat(joined.parameters()).containsExactly(leftParameter, rightParameter);
            }
        }
        for (String condition : List.of(
                "`left_alias` = `left_alias`",
                "`missing` = `right_alias`",
                "`left_alias` + 1 = `right_alias`",
                "`left_alias` < `right_alias`")) {
            assertThat(client.implementJoin(
                    SESSION,
                    JoinType.INNER,
                    left,
                    Map.of(leftColumn, "left_alias"),
                    right,
                    Map.of(rightColumn, "right_alias"),
                    List.of(new ParameterizedExpression(condition, List.of())),
                    statistics())).isEmpty();
        }
    }

    private static YdbClient client()
    {
        return new YdbClient(new BaseJdbcConfig(), _ -> (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (_, method, _) -> {
                    if (method.getName().equals("close")) {
                        return null;
                    }
                    throw new UnsupportedOperationException(method.getName());
                }), new YdbQueryBuilder(RemoteQueryModifier.NONE), new DefaultIdentifierMapping(), RemoteQueryModifier.NONE);
    }

    private static JoinStatistics statistics()
    {
        return new JoinStatistics()
        {
            @Override
            public Optional<BasicRelationStatistics> getLeftStatistics()
            {
                return Optional.empty();
            }

            @Override
            public Optional<BasicRelationStatistics> getRightStatistics()
            {
                return Optional.empty();
            }

            @Override
            public Optional<BasicRelationStatistics> getJoinStatistics()
            {
                return Optional.empty();
            }
        };
    }
}
