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
package io.trino.plugin.mysql;

import com.google.common.base.Joiner;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.WriteFunction;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static java.lang.String.format;
import static java.util.Collections.nCopies;

public class CollationAwareQueryBuilder
        extends DefaultQueryBuilder
{
    private final boolean enableStringPushdownWithCollate;

    @Inject
    public CollationAwareQueryBuilder(MySqlConfig mySqlConfig)
    {
        this.enableStringPushdownWithCollate = mySqlConfig.isEnableStringPushdownWithCollate();
    }

    @Override
    protected String toPredicate(JdbcClient client, ConnectorSession session, JdbcColumnHandle column, JdbcTypeHandle jdbcType, Type type, WriteFunction writeFunction, String operator, Object value, Consumer<QueryParameter> accumulator)
    {
        if (isCollatable(column) && enableStringPushdownWithCollate) {
            accumulator.accept(new QueryParameter(jdbcType, type, Optional.of(value)));
            return format("%s %s %s COLLATE \"utf8mb4_0900_bin\"", client.quoted(column.getColumnName()), operator, writeFunction.getBindExpression());
        }
        return super.toPredicate(client, session, column, jdbcType, type, writeFunction, operator, value, accumulator);
    }

    @Override
    protected String toInPredicate(JdbcClient client, ConnectorSession session, JdbcColumnHandle column, JdbcTypeHandle jdbcType, Type type, WriteFunction writeFunction, List<Object> values, Consumer<QueryParameter> accumulator)
    {
        if (isCollatable(column) && enableStringPushdownWithCollate) {
            for (Object value : values) {
                accumulator.accept(new QueryParameter(jdbcType, type, Optional.of(value)));
            }
            String inValues = Joiner.on(",").join(nCopies(values.size(), format("%s %s", writeFunction.getBindExpression(), "COLLATE \"utf8mb4_0900_bin\"")));
            return client.quoted(column.getColumnName()) + " IN (" + inValues + ")";
        }
        return super.toInPredicate(client, session, column, jdbcType, type, writeFunction, values, accumulator);
    }

    private static boolean isCollatable(JdbcColumnHandle column)
    {
        return column.getColumnType() instanceof CharType || column.getColumnType() instanceof VarcharType;
    }
}
