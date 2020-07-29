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
package io.prestosql.plugin.hive;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.hadoop.hive.metastore.api.DataOperationType;

public enum AcidOperation
{
    // INSERT and UPDATE will be added when they are implemented
    NONE,
    DELETE;

    @JsonIgnore
    public DataOperationType getOperationType()
    {
        switch (this) {
            case DELETE:
                return DataOperationType.DELETE;
            default:
                return DataOperationType.NO_TXN;
        }
    }
}
