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
package io.prestosql.plugin.hive.acid;

import io.prestosql.orc.OrcWriter.OrcOperation;
import org.apache.hadoop.hive.metastore.api.DataOperationType;

public enum AcidOperation
{
    // UPDATE and MERGE will be added when they are implemented
    NONE(DataOperationType.NO_TXN, OrcOperation.NONE),
    CREATE_TABLE(DataOperationType.NO_TXN, OrcOperation.NONE),
    DELETE(DataOperationType.DELETE, OrcOperation.DELETE),
    INSERT(DataOperationType.INSERT, OrcOperation.INSERT);

    private final DataOperationType metastoreOperationType;
    private final OrcOperation orcOperation;

    AcidOperation(DataOperationType metastoreOperationType, OrcOperation orcOperation)
    {
        this.metastoreOperationType = metastoreOperationType;
        this.orcOperation = orcOperation;
    }

    public DataOperationType getMetastoreOperationType()
    {
        return metastoreOperationType;
    }

    public OrcOperation getOrcOperation()
    {
        return orcOperation;
    }
}
