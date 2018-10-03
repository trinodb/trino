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
package io.prestosql.plugin.iceberg;

import com.google.common.base.VerifyException;
import io.airlift.slice.Slice;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.EquatableValueSet;
import io.prestosql.spi.predicate.Marker;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.SortedRangeSet;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.ValueSet;
import org.apache.iceberg.expressions.Expression;

import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static io.prestosql.spi.predicate.Marker.Bound.ABOVE;
import static io.prestosql.spi.predicate.Marker.Bound.BELOW;
import static io.prestosql.spi.predicate.Marker.Bound.EXACTLY;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.StandardTypes.TIME;
import static io.prestosql.spi.type.StandardTypes.TIMESTAMP;
import static io.prestosql.spi.type.StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.StandardTypes.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.StandardTypes.VARBINARY;
import static io.prestosql.spi.type.StandardTypes.VARCHAR;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.iceberg.expressions.Expressions.alwaysFalse;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.or;

public final class ExpressionConverter
{
    private ExpressionConverter() {}

    public static Expression toIcebergExpression(TupleDomain<HiveColumnHandle> tupleDomain, ConnectorSession session)
    {
        if (tupleDomain.isAll()) {
            return alwaysTrue();
        }
        if (!tupleDomain.getDomains().isPresent()) {
            return alwaysFalse();
        }
        Map<HiveColumnHandle, Domain> domainMap = tupleDomain.getDomains().get();
        Expression expression = alwaysTrue();
        for (Map.Entry<HiveColumnHandle, Domain> entry : domainMap.entrySet()) {
            HiveColumnHandle columnHandle = entry.getKey();
            Domain domain = entry.getValue();
            if (!columnHandle.isHidden()) {
                expression = and(expression, toIcebergExpression(columnHandle, domain, session));
            }
        }
        return expression;
    }

    private static Expression toIcebergExpression(HiveColumnHandle column, Domain domain, ConnectorSession session)
    {
        String columnName = column.getName();

        if (domain.isAll()) {
            return alwaysTrue();
        }
        if (domain.getValues().isNone()) {
            return domain.isNullAllowed() ? isNull(columnName) : alwaysFalse();
        }

        if (domain.getValues().isAll()) {
            return domain.isNullAllowed() ? alwaysTrue() : not(isNull(columnName));
        }

        ValueSet domainValues = domain.getValues();
        Expression expression = null;
        if (domain.isNullAllowed()) {
            expression = isNull(columnName);
        }

        if (domainValues instanceof EquatableValueSet) {
            expression = firstNonNull(expression, alwaysFalse());
            EquatableValueSet valueSet = (EquatableValueSet) domainValues;
            if (valueSet.isWhiteList()) {
                // if whitelist is true than this is a case of "in", otherwise this is a case of "not in".
                return or(expression, equal(columnName, valueSet.getValues()));
            }
            return or(expression, notEqual(columnName, valueSet.getValues()));
        }

        if (domainValues instanceof SortedRangeSet) {
            List<Range> orderedRanges = ((SortedRangeSet) domainValues).getOrderedRanges();
            expression = firstNonNull(expression, alwaysFalse());

            for (Range range : orderedRanges) {
                Marker low = range.getLow();
                Marker high = range.getHigh();
                Marker.Bound lowBound = low.getBound();
                Marker.Bound highBound = high.getBound();

                // case col <> 'val' is represented as (col < 'val' or col > 'val')
                if (lowBound == EXACTLY && highBound == EXACTLY) {
                    // case ==
                    if (getValue(column, low, session).equals(getValue(column, high, session))) {
                        expression = or(expression, equal(columnName, getValue(column, low, session)));
                    }
                    else { // case between
                        Expression between = and(
                                greaterThanOrEqual(columnName, getValue(column, low, session)),
                                lessThanOrEqual(columnName, getValue(column, high, session)));
                        expression = or(expression, between);
                    }
                }
                else {
                    if (lowBound == EXACTLY && low.getValueBlock().isPresent()) {
                        // case >=
                        expression = or(expression, greaterThanOrEqual(columnName, getValue(column, low, session)));
                    }
                    else if (lowBound == ABOVE && low.getValueBlock().isPresent()) {
                        // case >
                        expression = or(expression, greaterThan(columnName, getValue(column, low, session)));
                    }

                    if (highBound == EXACTLY && high.getValueBlock().isPresent()) {
                        // case <=
                        if (low.getValueBlock().isPresent()) {
                            expression = and(expression, lessThanOrEqual(columnName, getValue(column, high, session)));
                        }
                        else {
                            expression = or(expression, lessThanOrEqual(columnName, getValue(column, high, session)));
                        }
                    }
                    else if (highBound == BELOW && high.getValueBlock().isPresent()) {
                        // case <
                        if (low.getValueBlock().isPresent()) {
                            expression = and(expression, lessThan(columnName, getValue(column, high, session)));
                        }
                        else {
                            expression = or(expression, lessThan(columnName, getValue(column, high, session)));
                        }
                    }
                }
            }
            return expression;
        }

        throw new VerifyException("Did not expect a domain value set other than SortedRangeSet and EquatableValueSet but got " + domainValues.getClass().getSimpleName());
    }

    private static Object getValue(HiveColumnHandle columnHandle, Marker marker, ConnectorSession session)
    {
        switch (columnHandle.getTypeSignature().getBase()) {
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIME_WITH_TIME_ZONE:
                return MILLISECONDS.toMicros(unpackMillisUtc((Long) marker.getValue()));
            case TIME:
            case TIMESTAMP:
                return MILLISECONDS.toMicros((Long) marker.getValue());
            case VARCHAR:
                return ((Slice) marker.getValue()).toStringUtf8();
            case VARBINARY:
                return ((Slice) marker.getValue()).getBytes();
        }
        return marker.getValue();
    }
}
