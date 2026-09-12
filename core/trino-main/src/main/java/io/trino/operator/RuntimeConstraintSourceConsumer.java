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

import com.google.common.collect.ImmutableList;
import io.trino.spi.predicate.Domain;

import java.util.List;

import static java.util.Objects.requireNonNull;

public interface RuntimeConstraintSourceConsumer
{
    void addPartition(List<Domain> domains, Observation observation);

    void setPartitionCount(int partitionCount);

    boolean isDomainCollectionComplete();

    record Observation(boolean sawInputRow, List<Boolean> sawNulls)
    {
        public Observation
        {
            sawNulls = ImmutableList.copyOf(requireNonNull(sawNulls, "sawNulls is null"));
        }
    }
}
