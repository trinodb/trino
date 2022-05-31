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

package $package;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorInsertTableHandle;

public class ${classPrefix}InsertTableHandle
        implements ConnectorInsertTableHandle
{
    private final ${classPrefix}TableHandle tableHandle;

    @JsonCreator
    public ${classPrefix}InsertTableHandle(
            @JsonProperty("tableHandle") ${classPrefix}TableHandle tableHandle)
    {
        this.tableHandle = tableHandle;
    }

    @JsonProperty("tableHandle")
    public ${classPrefix}TableHandle getTableHandle()
    {
        return tableHandle;
    }
}
