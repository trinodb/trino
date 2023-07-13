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
import io.airlift.units.Duration;
import io.trino.spi.connector.TableProcedureMetadata;

import static io.trino.plugin.base.session.PropertyMetadataUtil.durationProperty;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.EXPIRE_SNAPSHOTS;
import static io.trino.spi.connector.TableProcedureExecutionMode.coordinatorOnly;

public class ExpireSnapshotsTableProcedure
        implements Provider<TableProcedureMetadata>
{
    @Override
    public TableProcedureMetadata get()
    {
        return new TableProcedureMetadata(
                EXPIRE_SNAPSHOTS.name(),
                coordinatorOnly(),
                ImmutableList.of(
                        durationProperty(
                                "retention_threshold",
                                "Only snapshots older than threshold should be removed",
                                Duration.valueOf("7d"),
                                false)));
    }
}
