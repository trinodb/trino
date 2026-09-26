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
import io.trino.spi.predicate.FloatingPointValueSet;
import io.trino.spi.predicate.ValueSet;

import java.sql.Connection;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;

/// Renders ordered floating-point domains with explicit infinity bounds to exclude NaN,
/// regardless of whether the database orders NaN above or below numbers.
/// Use with [PredicatePushdownController#FLOATING_POINT_PUSHDOWN] only for mappings
/// whose write functions can bind infinities.
public class FloatingPointQueryBuilder
        extends DefaultQueryBuilder
{
    @Inject
    public FloatingPointQueryBuilder(RemoteQueryModifier queryModifier)
    {
        super(queryModifier);
    }

    @Override
    protected String toPredicate(JdbcClient client, ConnectorSession session, Connection connection, JdbcColumnHandle column, ValueSet valueSet, Consumer<QueryParameter> accumulator)
    {
        if (valueSet instanceof FloatingPointValueSet floatingPoint) {
            checkArgument(!floatingPoint.isNaNAllowed(), "NaN domain must be handled before rendering");
            valueSet = floatingPoint.getOrderedValues();
        }
        return super.toPredicate(client, session, connection, column, valueSet, accumulator);
    }
}
