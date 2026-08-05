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
package io.trino.sql.planner.assertions;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record SemiJoinDynamicFilterProducer(Optional<DynamicFilterAlias> alias, boolean ignored)
{
    public SemiJoinDynamicFilterProducer
    {
        requireNonNull(alias, "alias is null");
    }

    public static SemiJoinDynamicFilterProducer ignoreDynamicFilter()
    {
        return new SemiJoinDynamicFilterProducer(Optional.empty(), true);
    }

    public static SemiJoinDynamicFilterProducer dynamicFilter(String alias)
    {
        return new SemiJoinDynamicFilterProducer(Optional.of(new DynamicFilterAlias(alias)), false);
    }

    public static SemiJoinDynamicFilterProducer noDynamicFilter()
    {
        return new SemiJoinDynamicFilterProducer(Optional.empty(), false);
    }
}
