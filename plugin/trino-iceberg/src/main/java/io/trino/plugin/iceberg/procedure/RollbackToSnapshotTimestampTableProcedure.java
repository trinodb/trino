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
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.SqlTimestampWithTimeZone;

import java.time.Instant;

import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.ROLLBACK_TO_TIMESTAMP;
import static io.trino.spi.connector.TableProcedureExecutionMode.coordinatorOnly;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;

public class RollbackToSnapshotTimestampTableProcedure
        implements Provider<TableProcedureMetadata>
{
    @Override
    public TableProcedureMetadata get()
    {
        return new TableProcedureMetadata(
                ROLLBACK_TO_TIMESTAMP.name(),
                coordinatorOnly(),
                ImmutableList.<PropertyMetadata<?>>builder()
                        .add(new PropertyMetadata<>(
                                "snapshot_timestamp",
                                "Snapshot timestamp",
                                TIMESTAMP_TZ_MILLIS,
                                Instant.class,
                                null,
                                false,
                                value -> ((SqlTimestampWithTimeZone) value).toZonedDateTime().toInstant(),
                                instant -> SqlTimestampWithTimeZone.fromInstant(3, instant, UTC_KEY.getZoneId())))
                        .build());
    }
}
