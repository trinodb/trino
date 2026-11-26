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
package io.trino.plugin.iceberg;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.type.Type;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundLiteralPredicate;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.BoundSetPredicate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.Projections;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.trino.spi.predicate.FloatingPointValueSet.orderedRanges;
import static io.trino.spi.type.DateType.DATE;
import static org.apache.iceberg.expressions.Expression.Operation.EQ;
import static org.apache.iceberg.expressions.Expression.Operation.FALSE;
import static org.apache.iceberg.expressions.Expression.Operation.GT;
import static org.apache.iceberg.expressions.Expression.Operation.GT_EQ;
import static org.apache.iceberg.expressions.Expression.Operation.IN;
import static org.apache.iceberg.expressions.Expression.Operation.LT;
import static org.apache.iceberg.expressions.Expression.Operation.LT_EQ;
import static org.apache.iceberg.expressions.Expression.Operation.TRUE;

/**
 * Determines whether a column constraint is fully enforced by Iceberg partitioning using Iceberg's
 * inclusive and strict projection contract: every partition surviving {@link Projections#inclusive}
 * must satisfy {@link Projections#strict}, guaranteeing that every row in that partition matches
 * the filter.
 */
final class PartitionPredicateEnforcement
{
    private PartitionPredicateEnforcement() {}

    /**
     * Returns the partition specs with the given ids, or the current spec when there are none,
     * bound to {@code tableSchema}, which is where the column handle names come from.
     */
    public static List<PartitionSpec> selectPartitionSpecs(Table table, Schema tableSchema, Set<Integer> partitionSpecIds)
    {
        List<PartitionSpec> partitionSpecs = table.specs().values().stream()
                .filter(partitionSpec -> partitionSpecIds.contains(partitionSpec.specId()))
                .map(partitionSpec -> bindSpecToSchema(partitionSpec, tableSchema))
                .collect(toImmutableList());

        if (partitionSpecs.isEmpty()) {
            return ImmutableList.of(bindSpecToSchema(table.spec(), tableSchema));
        }
        return partitionSpecs;
    }

    public static boolean canEnforceColumnConstraintInSpecs(List<PartitionSpec> partitionSpecs, IcebergColumnHandle columnHandle, Domain domain)
    {
        boolean stepsPastBounds = false;
        List<PartitionSpec> projectedSpecs = new ArrayList<>(partitionSpecs.size());
        for (PartitionSpec spec : partitionSpecs) {
            List<PartitionField> fields = spec.getFieldsBySourceId(columnHandle.getId());
            if (spec.isUnpartitioned() || fields.isEmpty()) {
                return false;
            }
            // Identity projects a predicate to itself, so its inclusive and strict projections always agree
            if (fields.stream().anyMatch(field -> field.transform().isIdentity())) {
                continue;
            }
            stepsPastBounds = true;
            // The day of a date is the date itself, but Iceberg does not project = and IN strictly for it
            if (!isDayOfDate(columnHandle, fields)) {
                projectedSpecs.add(spec);
            }
        }
        if (stepsPastBounds && hasBoundAtTypeLimit(domain)) {
            return false;
        }
        if (projectedSpecs.isEmpty()) {
            return true;
        }

        Expression expression = toIcebergExpression(columnHandle, domain);
        for (PartitionSpec spec : projectedSpecs) {
            Expression inclusive = bindPartitionExpression(spec, Projections.inclusive(spec, true).project(expression));
            Expression strict = bindPartitionExpression(spec, Projections.strict(spec, true).project(expression));
            if (!implies(inclusive, strict)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isDayOfDate(IcebergColumnHandle columnHandle, List<PartitionField> fields)
    {
        return columnHandle.getType().equals(DATE) && fields.stream().anyMatch(field -> field.transform().toString().equals("day"));
    }

    // Iceberg's projections step past a bound with wrapping arithmetic, so they cannot be trusted for a bound at the limit of the type
    private static boolean hasBoundAtTypeLimit(Domain domain)
    {
        Optional<Type.Range> limits = domain.getType().getRange();
        if (limits.isEmpty() || domain.getValues().isNone() || domain.getValues().isAll()) {
            return false;
        }
        // Only the lowest and the highest range can reach a limit
        List<Range> ranges = orderedRanges(domain.getValues());
        return Stream.of(ranges.getFirst(), ranges.getLast())
                .flatMap(range -> Stream.of(range.getLowValue(), range.getHighValue()))
                .flatMap(Optional::stream)
                .anyMatch(bound -> bound.equals(limits.get().getMin()) || bound.equals(limits.get().getMax()));
    }

    private static PartitionSpec bindSpecToSchema(PartitionSpec spec, Schema tableSchema)
    {
        if (spec.schema().schemaId() == tableSchema.schemaId()) {
            return spec;
        }
        return spec.toUnbound().bind(tableSchema, true);
    }

    private static Expression bindPartitionExpression(PartitionSpec spec, Expression projected)
    {
        return Binder.bind(spec.partitionType(), Expressions.rewriteNot(projected), true);
    }

    /**
     * Returns true only when every partition value satisfying {@code antecedent} also satisfies {@code consequent}.
     * Both expressions must be bound to the same partition struct with NOT already pushed down.
     */
    @VisibleForTesting
    static boolean implies(Expression antecedent, Expression consequent)
    {
        return implies(antecedent, consequent, new Disjuncts(consequent));
    }

    private static boolean implies(Expression antecedent, Expression consequent, Disjuncts consequentDisjuncts)
    {
        if (antecedent.op() == FALSE || consequent.op() == TRUE) {
            return true;
        }
        if (antecedent.op() == TRUE || consequent.op() == FALSE) {
            return false;
        }
        if (areEquivalent(antecedent, consequent)) {
            return true;
        }
        if (antecedent instanceof Or or) {
            return implies(or.left(), consequent, consequentDisjuncts) && implies(or.right(), consequent, consequentDisjuncts);
        }
        if (consequent instanceof And and) {
            return implies(antecedent, and.left()) && implies(antecedent, and.right());
        }

        List<Expression> antecedents = flattenAnd(antecedent);
        List<Expression> consequents = consequentDisjuncts.flattened();

        for (int offset = 0; offset < consequents.size(); offset++) {
            int index = (consequentDisjuncts.lastMatch + offset) % consequents.size();
            if (impliesConjunction(antecedents, consequents.get(index))) {
                consequentDisjuncts.lastMatch = index;
                return true;
            }
        }

        return false;
    }

    /**
     * Checks structural equivalence of bound expressions, explicitly verifying term identity on
     * {@link BoundSetPredicate} (whose {@code isEquivalentTo} in Iceberg compares only literal sets).
     */
    private static boolean areEquivalent(Expression left, Expression right)
    {
        if (left == right) {
            return true;
        }
        if (left.op() != right.op()) {
            return false;
        }
        if (left.op() == TRUE || left.op() == FALSE) {
            return true;
        }
        // Both sides are projected from one expression, so operands are compared in order only
        if (left instanceof And leftAnd && right instanceof And rightAnd) {
            return areEquivalent(leftAnd.left(), rightAnd.left()) && areEquivalent(leftAnd.right(), rightAnd.right());
        }
        if (left instanceof Or leftOr && right instanceof Or rightOr) {
            return areEquivalent(leftOr.left(), rightOr.left()) && areEquivalent(leftOr.right(), rightOr.right());
        }
        if (left instanceof BoundPredicate<?> leftPredicate && right instanceof BoundPredicate<?> rightPredicate) {
            return leftPredicate.term().isEquivalentTo(rightPredicate.term()) && leftPredicate.isEquivalentTo(rightPredicate);
        }
        return false;
    }

    private static boolean impliesConjunction(List<Expression> antecedents, Expression consequent)
    {
        if (consequent.op() == TRUE) {
            return true;
        }
        if (consequent.op() == FALSE) {
            return false;
        }
        if (consequent instanceof And and) {
            return impliesConjunction(antecedents, and.left()) && impliesConjunction(antecedents, and.right());
        }
        if (consequent instanceof Or or) {
            return impliesConjunction(antecedents, or.left()) || impliesConjunction(antecedents, or.right());
        }
        if (consequent instanceof BoundPredicate<?> right) {
            for (Expression antecedent : antecedents) {
                if (antecedent instanceof BoundPredicate<?> left && impliesPredicate(left, right)) {
                    return true;
                }
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private static <T> boolean impliesPredicate(BoundPredicate<T> left, BoundPredicate<?> rightRaw)
    {
        if (!left.term().isEquivalentTo(rightRaw.term())) {
            return false;
        }
        if (left.isEquivalentTo(rightRaw)) {
            return true;
        }

        BoundPredicate<T> right = (BoundPredicate<T>) rightRaw;
        if (left instanceof BoundLiteralPredicate<T> leftLiteral) {
            T leftValue = leftLiteral.literal().value();
            if (leftLiteral.op() == EQ) {
                return right.test(leftValue);
            }
            if (right instanceof BoundLiteralPredicate<T> rightLiteral) {
                int cmp = leftLiteral.literal().comparator().compare(leftValue, rightLiteral.literal().value());
                return switch (leftLiteral.op()) {
                    case GT_EQ -> (rightLiteral.op() == GT && cmp > 0) || (rightLiteral.op() == GT_EQ && cmp >= 0);
                    case GT -> (rightLiteral.op() == GT || rightLiteral.op() == GT_EQ) && cmp >= 0;
                    case LT_EQ -> (rightLiteral.op() == LT && cmp < 0) || (rightLiteral.op() == LT_EQ && cmp <= 0);
                    case LT -> (rightLiteral.op() == LT || rightLiteral.op() == LT_EQ) && cmp <= 0;
                    default -> false;
                };
            }
        }
        else if (left instanceof BoundSetPredicate<T> leftSet && leftSet.op() == IN) {
            for (T value : leftSet.literalSet()) {
                if (!right.test(value)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    private static List<Expression> flattenAnd(Expression expression)
    {
        List<Expression> result = new ArrayList<>();
        collectAnd(expression, result);
        return result;
    }

    private static void collectAnd(Expression expression, List<Expression> result)
    {
        if (expression instanceof And and) {
            collectAnd(and.left(), result);
            collectAnd(and.right(), result);
        }
        else {
            result.add(expression);
        }
    }

    private static List<Expression> flattenOr(Expression expression)
    {
        List<Expression> result = new ArrayList<>();
        collectOr(expression, result);
        return result;
    }

    private static void collectOr(Expression expression, List<Expression> result)
    {
        if (expression instanceof Or or) {
            collectOr(or.left(), result);
            collectOr(or.right(), result);
        }
        else {
            result.add(expression);
        }
    }

    /**
     * Disjuncts of a consequent, flattened once for all disjuncts of the antecedent. Both sides are projected
     * from the same ordered ranges, so the search for an implied disjunct resumes where the previous one ended.
     */
    private static final class Disjuncts
    {
        private final Expression expression;
        private List<Expression> flattened;
        private int lastMatch;

        private Disjuncts(Expression expression)
        {
            this.expression = expression;
        }

        private List<Expression> flattened()
        {
            if (flattened == null) {
                flattened = flattenOr(expression);
            }
            return flattened;
        }
    }
}
