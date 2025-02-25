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
package io.trino.spi.connector;

import io.trino.spi.predicate.TupleDomain;

import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

/**
 * A result of unifying two table handles.
 *
 * @param unifiedHandle -- a handle that satisfies semantics of the first and second unified handles
 * @param firstCompensation -- previously enforced filter and limit of the first handle. If applied over the unifiedHandle, it will restore the semantics of the first handle
 * @param secondCompensation -- previously enforced filter and limit of the second handle. If applied over the unifiedHandle, it will restore the semantics of the second handle
 * @param enforcedProperties -- predicate and limit guaranteed by the unifiedHandle
 * // TODO in addition to TupleDomain, also return optional ConnectorExpression for both unified handles (see: ConstraintApplicationResult)
 */
public record UnificationResult<T>(T unifiedHandle, Properties firstCompensation, Properties secondCompensation, Properties enforcedProperties)
{
    public UnificationResult
    {
        requireNonNull(unifiedHandle, "unifiedHandle is null");
        requireNonNull(firstCompensation, "firstCompensation is null");
        requireNonNull(secondCompensation, "secondCompensation is null");
        requireNonNull(enforcedProperties, "enforcedProperties is null");
    }

    public record Properties(TupleDomain<ColumnHandle> filter, OptionalLong limit)
    {
        public Properties
        {
            requireNonNull(filter, "filter is null");
            requireNonNull(limit, "limit is null");
        }
    }
}
