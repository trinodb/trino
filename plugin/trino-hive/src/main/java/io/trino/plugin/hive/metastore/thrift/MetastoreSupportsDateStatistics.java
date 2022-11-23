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
package io.trino.plugin.hive.metastore.thrift;

import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.AtomicReference;

import static io.trino.plugin.hive.metastore.thrift.MetastoreSupportsDateStatistics.DateStatisticsSupport.NOT_SUPPORTED;
import static io.trino.plugin.hive.metastore.thrift.MetastoreSupportsDateStatistics.DateStatisticsSupport.SUPPORTED;
import static io.trino.plugin.hive.metastore.thrift.MetastoreSupportsDateStatistics.DateStatisticsSupport.UNKNOWN;
import static java.util.concurrent.TimeUnit.SECONDS;

// Needs to be public to allow for creating
// `io.trino.plugin.hive.metastore.thrift.ThriftHiveMetastoreClient`
@ThreadSafe
public class MetastoreSupportsDateStatistics
{
    private static final int MAX_SET_DATE_STATISTICS_ATTEMPTS = 100;

    public enum DateStatisticsSupport {
        SUPPORTED, NOT_SUPPORTED, UNKNOWN
    }

    private final AtomicReference<DateStatisticsSupport> supported = new AtomicReference<>(UNKNOWN);
    private final CoalescingCounter failures = new CoalescingCounter(new Duration(1, SECONDS));

    public DateStatisticsSupport isSupported()
    {
        return supported.get();
    }

    public void succeeded()
    {
        supported.set(SUPPORTED);
    }

    public void failed()
    {
        // When `dateStatistics.size() > 1` we expect something like "TApplicationException: Required field 'colName' is unset! Struct:ColumnStatisticsObj(colName:null, colType:null, statsData:null)"
        // When `dateStatistics.size() == 1` we expect something like "TTransportException: java.net.SocketTimeoutException: Read timed out"
        if (failures.incrementAndGet() >= MAX_SET_DATE_STATISTICS_ATTEMPTS) {
            supported.set(NOT_SUPPORTED);
        }
    }
}
