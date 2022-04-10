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
package io.trino.plugin.hive.procedure;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.spi.connector.TableProcedureMetadata;

import javax.inject.Provider;

import static io.trino.plugin.base.session.PropertyMetadataUtil.dataSizeProperty;
import static io.trino.spi.connector.TableProcedureExecutionMode.distributedWithFilteringAndRepartitioning;

public class OptimizeTableProcedure
        implements Provider<TableProcedureMetadata>
{
    public static final String NAME = "OPTIMIZE";
    public static final String FILE_SIZE_THRESHOLD = "FILE_SIZE_THRESHOLD";

    @Override
    public TableProcedureMetadata get()
    {
        return new TableProcedureMetadata(
                NAME,
                distributedWithFilteringAndRepartitioning(),
                ImmutableList.of(
                        dataSizeProperty(
                                FILE_SIZE_THRESHOLD,
                                "Only compact files smaller than given threshold in bytes",
                                DataSize.of(100, DataSize.Unit.MEGABYTE),
                                false)));
    }
}
