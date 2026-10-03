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
package io.trino.plugin.clickhouse;

import com.google.common.collect.ImmutableList;
import com.google.inject.Provider;
import io.trino.spi.connector.TableProcedureMetadata;

import static io.trino.spi.connector.TableProcedureExecutionMode.coordinatorOnly;
import static io.trino.spi.session.PropertyMetadata.stringProperty;

/**
 * Drops a single partition, which is a metadata operation in ClickHouse and therefore much cheaper
 * than deleting the same rows with {@code DELETE FROM}.
 * <p>
 * Trino has no {@code ALTER TABLE ... DROP PARTITION} statement that a connector could implement,
 * so this is exposed as a table procedure instead.
 */
public class DropPartitionTableProcedure
        implements Provider<TableProcedureMetadata>
{
    public static final String NAME = "DROP_PARTITION";
    public static final String PARTITION_PROPERTY = "partition";

    @Override
    public TableProcedureMetadata get()
    {
        return new TableProcedureMetadata(
                NAME,
                coordinatorOnly(),
                ImmutableList.of(stringProperty(
                        PARTITION_PROPERTY,
                        "Value of the table's partition expression identifying the partition",
                        null,
                        false)));
    }
}
