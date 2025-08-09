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
package io.trino.plugin.hive.metastore.polaris;

import io.airlift.stats.CounterStat;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public enum PolarisMetastoreMethod
{
    // Database operations
    CREATE_DATABASE(PolarisMetastoreStats::getCreateDatabase),
    GET_DATABASE(PolarisMetastoreStats::getGetDatabase),
    GET_ALL_DATABASES(PolarisMetastoreStats::getGetAllDatabases),

    // Iceberg table operations (via RESTSessionCatalog)
    CREATE_ICEBERG_TABLE(PolarisMetastoreStats::getCreateIcebergTable),
    LOAD_ICEBERG_TABLE(PolarisMetastoreStats::getLoadIcebergTable),
    DROP_ICEBERG_TABLE(PolarisMetastoreStats::getDropIcebergTable),
    LIST_ICEBERG_TABLES(PolarisMetastoreStats::getListIcebergTables),

    // Generic table operations (via PolarisRestClient)
    CREATE_GENERIC_TABLE(PolarisMetastoreStats::getCreateGenericTable),
    LOAD_GENERIC_TABLE(PolarisMetastoreStats::getLoadGenericTable),
    DROP_GENERIC_TABLE(PolarisMetastoreStats::getDropGenericTable),
    LIST_GENERIC_TABLES(PolarisMetastoreStats::getListGenericTables),

    // Combined table operations
    GET_TABLE(PolarisMetastoreStats::getGetTable),
    GET_ALL_TABLES(PolarisMetastoreStats::getGetAllTables),
    DROP_TABLE(PolarisMetastoreStats::getDropTable),
    /**/;

    private final Function<PolarisMetastoreStats, CounterStat> extractor;

    PolarisMetastoreMethod(Function<PolarisMetastoreStats, CounterStat> extractor)
    {
        this.extractor = requireNonNull(extractor, "extractor is null");
    }

    public CounterStat getStatFrom(PolarisMetastoreStats stats)
    {
        return this.extractor.apply(stats);
    }

    public long getInvocationCount(PolarisMetastoreStats stats)
    {
        return this.getStatFrom(stats).getTotalCount();
    }
}
