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
package io.trino.sql.planner.iterative.rule;

import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.spi.type.TimeZoneNotSupportedException;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.VarcharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.IrExpressions.Comparison;

import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.sql.ir.IrExpressions.comparison;
import static io.trino.sql.ir.IrExpressions.matchComparison;
import static java.util.Objects.requireNonNull;

/**
 * Given a constant, valid time zone, converts expressions of the form
 * <pre>
 *     at_timezone(ts, zone) ⋈ expression
 * </pre>
 * into
 * <pre>
 *     ts ⋈ expression
 * </pre>
 * <p>
 * {@code at_timezone} changes only the zone a {@code timestamp with time zone} value is rendered
 * in, never the instant, and {@code timestamp with time zone} comparisons compare the instant
 * only, so the call is transparent to every comparison operator. With a constant zone that is
 * validated at plan time, the call cannot fail at runtime, and since it returns null if and only
 * if its first argument is null, null semantics (including {@code IDENTICAL}) are preserved by
 * the rewrite.
 * <p>
 * Non-constant and invalid zones are left untouched: a null zone makes the result null (a bare
 * comparison could then wrongly match) and an invalid zone string fails the query at runtime
 * (unwrapping would silently swallow the failure). The {@code time with time zone} form of
 * {@code at_timezone} interacts differently with comparison semantics and is not rewritten.
 * {@code with_timezone} reinterprets the wall time, changing the instant, and must never be
 * rewritten this way.
 *
 * @see UnwrapCastInComparison
 * @see UnwrapDateTruncInComparison
 */
public class UnwrapAtTimeZoneInComparison
        extends ExpressionRewriteRuleSet
{
    public UnwrapAtTimeZoneInComparison(PlannerContext plannerContext)
    {
        super(createRewrite(plannerContext));
    }

    private static ExpressionRewriter createRewrite(PlannerContext plannerContext)
    {
        requireNonNull(plannerContext, "plannerContext is null");

        return (expression, context) -> unwrapAtTimeZone(plannerContext, context.getSession(), expression);
    }

    private static Expression unwrapAtTimeZone(PlannerContext plannerContext, Session session, Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(plannerContext, session), expression);
    }

    private static class Visitor
            extends io.trino.sql.ir.ExpressionRewriter<Void>
    {
        private final PlannerContext plannerContext;
        private final Session session;

        public Visitor(PlannerContext plannerContext, Session session)
        {
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.session = requireNonNull(session, "session is null");
        }

        @Override
        public Expression rewriteCall(Call node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Call expression = treeRewriter.defaultRewrite(node, null);
            if (!(matchComparison(expression) instanceof Comparison comparison)) {
                return expression;
            }

            Expression left = unwrap(comparison.left());
            Expression right = unwrap(comparison.right());
            if (left == comparison.left() && right == comparison.right()) {
                return expression;
            }
            return comparison(plannerContext.getMetadata(), getCharVarcharCoercion(session), comparison.operator(), left, right);
        }

        private static Expression unwrap(Expression expression)
        {
            while (expression instanceof Call call && isInstantPreservingAtTimeZone(call)) {
                expression = call.arguments().get(0);
            }
            return expression;
        }

        private static boolean isInstantPreservingAtTimeZone(Call call)
        {
            if (!call.function().name().equals(builtinFunctionName("at_timezone")) ||
                    call.arguments().size() != 2 ||
                    !(call.type() instanceof TimestampWithTimeZoneType) ||
                    !call.arguments().get(0).type().equals(call.type())) {
                return false;
            }
            // only a constant zone that is provably valid can be dropped: a null zone makes the
            // result null and an invalid zone fails the query at runtime
            Expression zone = call.arguments().get(1);
            if (!(zone.type() instanceof VarcharType) ||
                    !(zone instanceof Constant constant) ||
                    !(constant.value() instanceof Slice slice)) {
                return false;
            }
            try {
                getTimeZoneKey(slice.toStringUtf8());
                return true;
            }
            catch (TimeZoneNotSupportedException | IllegalArgumentException e) {
                // getTimeZoneKey throws IllegalArgumentException for an empty zone id
                return false;
            }
        }
    }
}
