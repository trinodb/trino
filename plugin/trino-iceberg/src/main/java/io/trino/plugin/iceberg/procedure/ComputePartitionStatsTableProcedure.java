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
package io.trino.plugin.iceberg.procedure;

import com.google.common.collect.ImmutableList;
import com.google.inject.Provider;
import io.trino.spi.connector.TableProcedureMetadata;

import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.COMPUTE_PARTITION_STATS;
import static io.trino.spi.connector.TableProcedureExecutionMode.coordinatorOnly;
import static io.trino.spi.session.PropertyMetadata.longProperty;

public class ComputePartitionStatsTableProcedure
        implements Provider<TableProcedureMetadata>
{
    @Override
    public TableProcedureMetadata get()
    {
        return new TableProcedureMetadata(
                COMPUTE_PARTITION_STATS.name(),
                coordinatorOnly(),
                ImmutableList.of(longProperty(
                        "snapshot_id",
                        "Snapshot ID, defaults to the current snapshot",
                        null,
                        false)));
    }
}
