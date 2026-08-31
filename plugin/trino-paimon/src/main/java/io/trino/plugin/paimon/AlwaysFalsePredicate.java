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
package io.trino.plugin.paimon;

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateVisitor;

import java.util.Optional;

public class AlwaysFalsePredicate
        implements Predicate
{
    public static final AlwaysFalsePredicate INSTANCE = new AlwaysFalsePredicate();

    private AlwaysFalsePredicate() {}

    @Override
    public boolean test(InternalRow row)
    {
        return false;
    }

    @Override
    public boolean test(long rowCount, InternalRow minValues, InternalRow maxValues, InternalArray nullCounts)
    {
        return false;
    }

    @Override
    public Optional<Predicate> negate()
    {
        // The negation of always-false is always-true, which we represent as empty
        // optional
        return Optional.empty();
    }

    @Override
    public <T> T visit(PredicateVisitor<T> visitor)
    {
        // AlwaysFalsePredicate is a special case that doesn't fit into standard visitor
        // patterns
        // Return null to indicate unsupported operation
        return null;
    }

    @Override
    public String toString()
    {
        return "AlwaysFalsePredicate{}";
    }
}
