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

import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.FAST_FORWARD;
import static io.trino.spi.connector.TableProcedureExecutionMode.coordinatorOnly;
import static io.trino.spi.session.PropertyMetadata.stringProperty;

public class FastForwardProcedure
        implements Provider<TableProcedureMetadata>
{
    @Override
    public TableProcedureMetadata get()
    {
        return new TableProcedureMetadata(
                FAST_FORWARD.name(),
                coordinatorOnly(),
                ImmutableList.<PropertyMetadata<?>>builder()
                        .add(stringProperty(
                                "from",
                                "Branch to fast-forward",
                                null,
                                false))
                        .add(stringProperty(
                                "to",
                                "Ref for the from branch to be fast forwarded to",
                                null,
                                false))
                        .build());
    }
}
