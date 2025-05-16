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
package io.trino.plugin.deltalake.util;

import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.fileModifiedTimeColumnHandle;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.fileSizeColumnHandle;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.pathColumnHandle;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.deserializePartitionValue;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;

public final class DeltaLakeDomains
{
    private DeltaLakeDomains() {}

    public static boolean partitionMatchesPredicate(Map<String, Optional<String>> partitionKeys, Map<DeltaLakeColumnHandle, Domain> domains)
    {
        for (Map.Entry<DeltaLakeColumnHandle, Domain> enforcedDomainsEntry : domains.entrySet()) {
            DeltaLakeColumnHandle partitionColumn = enforcedDomainsEntry.getKey();
            Optional<String> partitionValue = partitionKeys.get(partitionColumn.basePhysicalColumnName());
            if (partitionValue == null) {
                continue;
            }
            Domain partitionDomain = enforcedDomainsEntry.getValue();
            if (!partitionDomain.includesNullableValue(deserializePartitionValue(partitionColumn, partitionValue))) {
                return false;
            }
        }
        return true;
    }

    public static Domain getFileModifiedTimeDomain(TupleDomain<DeltaLakeColumnHandle> effectivePredicate)
    {
        return effectivePredicate.getDomains()
                .flatMap(domains -> Optional.ofNullable(domains.get(fileModifiedTimeColumnHandle())))
                .orElseGet(() -> Domain.all(fileModifiedTimeColumnHandle().baseType()));
    }

    public static boolean fileModifiedTimeMatchesPredicate(Domain fileModifiedTimeDomain, long fileModifiedTime)
    {
        return fileModifiedTimeDomain.includesNullableValue(packDateTimeWithZone(fileModifiedTime, UTC_KEY));
    }

    public static Domain getPathDomain(TupleDomain<DeltaLakeColumnHandle> effectivePredicate)
    {
        return effectivePredicate.getDomains()
                .flatMap(domains -> Optional.ofNullable(domains.get(pathColumnHandle())))
                .orElseGet(() -> Domain.all(pathColumnHandle().baseType()));
    }

    public static boolean pathMatchesPredicate(Domain pathDomain, String path)
    {
        return pathDomain.includesNullableValue(utf8Slice(path));
    }

    public static Domain getFileSizeDomain(TupleDomain<DeltaLakeColumnHandle> effectivePredicate)
    {
        return effectivePredicate.getDomains()
                .flatMap(domains -> Optional.ofNullable(domains.get(fileSizeColumnHandle())))
                .orElseGet(() -> Domain.all(fileSizeColumnHandle().baseType()));
    }

    public static boolean fileSizeMatchesPredicate(Domain fileSizeDomain, long fileSize)
    {
        return fileSizeDomain.includesNullableValue(fileSize);
    }
}
