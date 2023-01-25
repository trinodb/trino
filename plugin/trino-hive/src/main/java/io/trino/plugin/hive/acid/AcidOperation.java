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
package io.trino.plugin.hive.acid;

import com.google.common.collect.ImmutableMap;
import io.trino.hive.thrift.metastore.DataOperationType;
import io.trino.orc.OrcWriter.OrcOperation;

import java.util.Map;
import java.util.Optional;

public enum AcidOperation
{
    NONE,
    CREATE_TABLE,
    INSERT,
    MERGE,
    /**/;

    private static final Map<AcidOperation, DataOperationType> DATA_OPERATION_TYPES = ImmutableMap.of(
            INSERT, DataOperationType.INSERT,
            MERGE, DataOperationType.UPDATE);

    private static final Map<AcidOperation, OrcOperation> ORC_OPERATIONS = ImmutableMap.of(
            INSERT, OrcOperation.INSERT);

    public Optional<DataOperationType> getMetastoreOperationType()
    {
        return Optional.ofNullable(DATA_OPERATION_TYPES.get(this));
    }

    public Optional<OrcOperation> getOrcOperation()
    {
        return Optional.ofNullable(ORC_OPERATIONS.get(this));
    }
}
