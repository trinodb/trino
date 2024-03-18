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
package io.trino.plugin.varada.warmup.model;

import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public class SameTableWarmupPredicateRule
        implements Predicate<SameTableWarmupPredicateRule>
{
    private final String schema;
    private final String table;

    public SameTableWarmupPredicateRule(String schema, String table)
    {
        this.schema = requireNonNull(schema);
        this.table = requireNonNull(table);
    }

    @Override
    public boolean test(SameTableWarmupPredicateRule other)
    {
        return other.schema.equals(this.schema) && other.table.equals(this.table);
    }
}
