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

import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class HudiPredicates
{
    private final TupleDomain<HiveColumnHandle> partitionColumnPredicates;
    private final TupleDomain<HiveColumnHandle> regularColumnPredicates;

    public static HudiPredicates from(TupleDomain<ColumnHandle> predicate)
    {
        Map<HiveColumnHandle, Domain> partitionColumnPredicates = new HashMap<>();
        Map<HiveColumnHandle, Domain> regularColumnPredicates = new HashMap<>();

        Optional<Map<ColumnHandle, Domain>> domains = predicate.getDomains();
        domains.ifPresent(columnHandleDomainMap -> columnHandleDomainMap.forEach((key, value) -> {
            HiveColumnHandle columnHandle = (HiveColumnHandle) key;
            if (columnHandle.isPartitionKey()) {
                partitionColumnPredicates.put(columnHandle, value);
            }
            else {
                regularColumnPredicates.put(columnHandle, value);
            }
        }));

        return new HudiPredicates(
                TupleDomain.withColumnDomains(partitionColumnPredicates),
                TupleDomain.withColumnDomains(regularColumnPredicates));
    }

    private HudiPredicates(
            TupleDomain<HiveColumnHandle> partitionColumnPredicates,
            TupleDomain<HiveColumnHandle> regularColumnPredicates)
    {
        this.partitionColumnPredicates = partitionColumnPredicates;
        this.regularColumnPredicates = regularColumnPredicates;
    }

    public TupleDomain<HiveColumnHandle> getPartitionColumnPredicates()
    {
        return partitionColumnPredicates;
    }

    public TupleDomain<HiveColumnHandle> getRegularColumnPredicates()
    {
        return regularColumnPredicates;
    }
}
