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
package io.trino.sql.planner;

import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.sql.ir.ComparisonOperator;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.TypeUtils.isFloatingPointNaN;
import static io.trino.sql.ir.ComparisonOperator.IDENTICAL;
import static java.util.Objects.requireNonNull;

/// Disjoint truth sets for a predicate over one value. Values in neither set produce unknown.
public record TruthDomains(Domain trueDomain, Domain falseDomain)
{
    public TruthDomains
    {
        requireNonNull(trueDomain, "trueDomain is null");
        requireNonNull(falseDomain, "falseDomain is null");
        checkArgument(trueDomain.getType().equals(falseDomain.getType()), "truth domain types differ");
        checkArgument(!trueDomain.overlaps(falseDomain), "truth domains overlap");
    }

    /// The caller establishes that the comparison is non-null on non-null operands and that
    /// the type's comparison truth sets are exactly representable by ordered value sets.
    public static TruthDomains comparison(ComparisonOperator operator, NullableValue constant)
    {
        var type = constant.getType();
        if (constant.isNull()) {
            if (operator == IDENTICAL) {
                return new TruthDomains(Domain.onlyNull(type), Domain.notNull(type));
            }
            return new TruthDomains(Domain.none(type), Domain.none(type));
        }
        Object value = constant.getValue();
        if (isFloatingPointNaN(type, value)) {
            Domain trueValues = switch (operator) {
                case IDENTICAL -> Domain.singleValue(type, value);
                case NOT_EQUAL -> Domain.notNull(type);
                default -> Domain.none(type);
            };
            return new TruthDomains(trueValues, trueValues.complement().intersect(operator == IDENTICAL ? Domain.all(type) : Domain.notNull(type)));
        }
        ValueSet trueValues = switch (operator) {
            case EQUAL, IDENTICAL -> ValueSet.of(type, value);
            case NOT_EQUAL -> ValueSet.of(type, value).complement();
            case LESS_THAN -> ValueSet.ofRanges(Range.lessThan(type, value));
            case LESS_THAN_OR_EQUAL -> ValueSet.ofRanges(Range.lessThanOrEqual(type, value));
            case GREATER_THAN -> ValueSet.ofRanges(Range.greaterThan(type, value));
            case GREATER_THAN_OR_EQUAL -> ValueSet.ofRanges(Range.greaterThanOrEqual(type, value));
        };
        Domain truth = Domain.create(trueValues, false);
        return new TruthDomains(truth, truth.complement().intersect(operator == IDENTICAL ? Domain.all(type) : Domain.notNull(type)));
    }

    public TruthDomains not()
    {
        return new TruthDomains(falseDomain, trueDomain);
    }

    public TruthDomains and(TruthDomains other)
    {
        return new TruthDomains(trueDomain.intersect(other.trueDomain), falseDomain.union(other.falseDomain));
    }

    public TruthDomains or(TruthDomains other)
    {
        return new TruthDomains(trueDomain.union(other.trueDomain), falseDomain.intersect(other.falseDomain));
    }
}
