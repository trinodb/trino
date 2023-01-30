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
package io.trino.plugin.postgresql;

import com.google.inject.Inject;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.WriteFunction;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static io.trino.plugin.postgresql.PostgreSqlClient.isCollatable;
import static io.trino.plugin.postgresql.PostgreSqlSessionProperties.isEnableStringPushdownWithCollate;
import static java.lang.String.format;

public class CollationAwareQueryBuilder
        extends DefaultQueryBuilder
{
    @Inject
    public CollationAwareQueryBuilder(RemoteQueryModifier queryModifier)
    {
        super(queryModifier);
    }

    @Override
    protected String formatJoinCondition(JdbcClient client, String leftRelationAlias, String rightRelationAlias, JdbcJoinCondition condition)
    {
        boolean isCollatable = Stream.of(condition.getLeftColumn(), condition.getRightColumn())
                .anyMatch(PostgreSqlClient::isCollatable);

        if (isCollatable) {
            return format(
                    "%s.%s COLLATE \"C\" %s %s.%s COLLATE \"C\"",
                    leftRelationAlias,
                    buildJoinColumn(client, condition.getLeftColumn()),
                    condition.getOperator().getValue(),
                    rightRelationAlias,
                    buildJoinColumn(client, condition.getRightColumn()));
        }

        return super.formatJoinCondition(client, leftRelationAlias, rightRelationAlias, condition);
    }

    @Override
    protected String toPredicate(JdbcClient client, ConnectorSession session, JdbcColumnHandle column, JdbcTypeHandle jdbcType, Type type, WriteFunction writeFunction, String operator, Object value, Consumer<QueryParameter> accumulator)
    {
        if (isCollatable(column) && isEnableStringPushdownWithCollate(session)) {
            accumulator.accept(new QueryParameter(jdbcType, type, Optional.of(value)));
            return format("%s %s %s COLLATE \"C\"", client.quoted(column.getColumnName()), operator, writeFunction.getBindExpression());
        }

        return super.toPredicate(client, session, column, jdbcType, type, writeFunction, operator, value, accumulator);
    }
}
