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

package io.trino.plugin.hudi;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;

public class HudiPredicates
{
    private final TupleDomain<ColumnHandle> partitionColumnPredicates;
    private final TupleDomain<ColumnHandle> regularColumnPredicates;

    public HudiPredicates(TupleDomain<ColumnHandle> partitionColumnPredicates,
                          TupleDomain<ColumnHandle> regularColumnPredicates)
    {
        this.partitionColumnPredicates = partitionColumnPredicates;
        this.regularColumnPredicates = regularColumnPredicates;
    }

    public TupleDomain<ColumnHandle> getPartitionColumnPredicates()
    {
        return partitionColumnPredicates;
    }

    public TupleDomain<ColumnHandle> getRegularColumnPredicates()
    {
        return regularColumnPredicates;
    }
}
