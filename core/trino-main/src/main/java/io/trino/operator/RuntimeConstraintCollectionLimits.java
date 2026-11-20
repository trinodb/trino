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
package io.trino.operator;

import io.airlift.units.DataSize;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record RuntimeConstraintCollectionLimits(
        int maxDistinctValues,
        DataSize maxFilterSize,
        int minMaxCollectionLimit,
        DataSize maxSizePerOperator)
{
    public RuntimeConstraintCollectionLimits
    {
        checkArgument(maxDistinctValues >= 0, "maxDistinctValues is negative");
        requireNonNull(maxFilterSize, "maxFilterSize is null");
        checkArgument(minMaxCollectionLimit >= 0, "minMaxCollectionLimit is negative");
        requireNonNull(maxSizePerOperator, "maxSizePerOperator is null");
    }
}
