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
package io.trino.sql.planner.plan;

import com.google.common.collect.ImmutableList;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record DataOrganizationSpecification(
        List<Symbol> partitionBy,
        Optional<OrderingScheme> orderingScheme)
{
    public DataOrganizationSpecification
    {
        partitionBy = ImmutableList.copyOf(partitionBy);
        requireNonNull(orderingScheme, "orderingScheme is null");
    }
}
