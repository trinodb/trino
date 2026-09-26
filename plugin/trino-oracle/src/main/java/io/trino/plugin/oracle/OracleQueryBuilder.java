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
package io.trino.plugin.oracle;

import com.google.inject.Inject;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.FloatingPointValueSet;
import io.trino.spi.predicate.ValueSet;
import oracle.jdbc.OracleTypes;

import java.sql.Connection;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;

public class OracleQueryBuilder
        extends DefaultQueryBuilder
{
    @Inject
    public OracleQueryBuilder(RemoteQueryModifier queryModifier)
    {
        super(queryModifier);
    }

    @Override
    protected String toPredicate(JdbcClient client, ConnectorSession session, Connection connection, JdbcColumnHandle column, ValueSet valueSet, Consumer<QueryParameter> accumulator)
    {
        // Oracle FLOAT uses decimal storage. Only BINARY_FLOAT and BINARY_DOUBLE can bind infinities.
        int jdbcType = column.getJdbcTypeHandle().jdbcType();
        if ((jdbcType == OracleTypes.BINARY_FLOAT || jdbcType == OracleTypes.BINARY_DOUBLE) && valueSet instanceof FloatingPointValueSet floatingPoint) {
            checkArgument(!floatingPoint.isNaNAllowed(), "NaN domain must be handled before rendering");
            valueSet = floatingPoint.getOrderedValues();
        }
        return super.toPredicate(client, session, connection, column, valueSet, accumulator);
    }
}
