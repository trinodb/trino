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
package io.trino.execution;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.execution.warnings.WarningCollector;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.ExpressionAnalyzer;
import io.trino.sql.analyzer.Scope;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.SetTimeZone;
import io.trino.type.IntervalDayTimeType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.SystemSessionProperties.TIME_ZONE_ID;
import static io.trino.execution.ParameterExtractor.bindParameters;
import static io.trino.spi.StandardErrorCode.INVALID_LITERAL;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static io.trino.sql.analyzer.ExpressionAnalyzer.createConstantAnalyzer;
import static io.trino.sql.planner.ExpressionInterpreter.evaluateConstantExpression;
import static io.trino.util.Failures.checkCondition;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SetTimeZoneTask
        implements DataDefinitionTask<SetTimeZone>
{
    private final PlannerContext plannerContext;
    private final AccessControl accessControl;

    @Inject
    public SetTimeZoneTask(PlannerContext plannerContext, AccessControl accessControl)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public String getName()
    {
        return "SET TIME ZONE";
    }

    @Override
    public ListenableFuture<Void> execute(
            SetTimeZone statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Optional<String> timeZoneId = statement.getTimeZone()
                .map(timeZone -> getTimeZoneId(timeZone, statement, stateMachine, parameters, warningCollector));
        if (timeZoneId.isPresent()) {
            stateMachine.addSetSessionProperties(TIME_ZONE_ID, timeZoneId.get());
        }
        else {
            stateMachine.addResetSessionProperties(TIME_ZONE_ID);
        }
        return immediateVoidFuture();
    }

    private String getTimeZoneId(
            Expression expression,
            SetTimeZone statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Map<NodeRef<Parameter>, Expression> parameterLookup = bindParameters(statement, parameters);
        ExpressionAnalyzer analyzer = createConstantAnalyzer(
                plannerContext,
                accessControl,
                stateMachine.getSession(),
                parameterLookup,
                warningCollector);

        Type type = analyzer.analyze(expression, Scope.create());
        if (!(type instanceof VarcharType || type instanceof IntervalDayTimeType)) {
            throw new TrinoException(TYPE_MISMATCH, format("Expected expression of varchar or interval day-time type, but '%s' has %s type", expression, type.getDisplayName()));
        }

        Object timeZoneValue = evaluateConstantExpression(
                expression,
                analyzer.getExpressionCoercions(),
                analyzer.getTypeOnlyCoercions(),
                plannerContext,
                stateMachine.getSession(),
                accessControl,
                ImmutableSet.of(),
                parameterLookup);

        TimeZoneKey timeZoneKey;
        if (timeZoneValue instanceof Slice) {
            timeZoneKey = getTimeZoneKey(((Slice) timeZoneValue).toStringUtf8());
        }
        else if (timeZoneValue instanceof Long) {
            timeZoneKey = getTimeZoneKeyForOffset(getZoneOffsetMinutes((Long) timeZoneValue));
        }
        else {
            throw new IllegalStateException(format("TIME ZONE expression '%s' not supported", expression));
        }
        return timeZoneKey.getId();
    }

    private static long getZoneOffsetMinutes(long interval)
    {
        checkCondition((interval % 60_000L) == 0L, INVALID_LITERAL, "Invalid TIME ZONE offset interval: interval contains seconds");
        return interval / 60_000L;
    }
}
