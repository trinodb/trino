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
package io.trino.cache;

import io.trino.sql.ir.Expression;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public record CacheExpression(Optional<Expression> projection, Optional<CanonicalAggregation> aggregation)
{
    public static CacheExpression ofProjection(Expression projection)
    {
        return new CacheExpression(Optional.of(projection), Optional.empty());
    }

    public static CacheExpression ofAggregation(CanonicalAggregation aggregation)
    {
        return new CacheExpression(Optional.empty(), Optional.of(aggregation));
    }

    public CacheExpression
    {
        checkArgument(projection.isPresent() != aggregation.isPresent(), "Expected exactly one to be present, got %s and %s", projection, aggregation);
    }
}
