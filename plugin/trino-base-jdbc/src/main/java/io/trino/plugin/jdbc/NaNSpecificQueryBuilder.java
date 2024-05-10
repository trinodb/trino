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
package io.trino.plugin.jdbc;

import com.google.inject.Inject;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;

import java.util.Optional;
import java.util.function.Consumer;

import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.String.format;

public class NaNSpecificQueryBuilder
        extends DefaultQueryBuilder
{
    @Inject
    public NaNSpecificQueryBuilder(RemoteQueryModifier queryModifier)
    {
        super(queryModifier);
    }

    @Override
    protected String toPredicate(JdbcClient client, ConnectorSession session, JdbcColumnHandle column, JdbcTypeHandle jdbcType, Type type, WriteFunction writeFunction, String operator, Object value, Consumer<QueryParameter> accumulator)
    {
        if ((type == REAL || type == DOUBLE) && (operator.equals(">") || operator.equals(">="))) {
            accumulator.accept(new QueryParameter(jdbcType, type, Optional.of(value)));
            return format("((%s %s %s) AND (%s <> 'NaN'))", client.quoted(column.getColumnName()), operator, writeFunction.getBindExpression(), client.quoted(column.getColumnName()));
        }

        return super.toPredicate(client, session, column, jdbcType, type, writeFunction, operator, value, accumulator);
    }
}
