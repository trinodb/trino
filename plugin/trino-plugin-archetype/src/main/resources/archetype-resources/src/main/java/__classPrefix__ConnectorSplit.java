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
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

public class ${classPrefix}ConnectorSplit
        implements ConnectorSplit
{
    private final ${classPrefix}TableHandle tableHandle;
    private final List<HostAddress> addresses;

    @JsonCreator
    public ${classPrefix}ConnectorSplit(
            @JsonProperty("tableHandle") ${classPrefix}TableHandle tableHandle,
            @JsonProperty("addresses") List<HostAddress> addresses)
    {
        this.tableHandle = tableHandle;
        this.addresses = addresses;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    @JsonProperty("addresses")
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return "$classPrefix split";
    }

    @JsonProperty("tableHandle")
    public ${classPrefix}TableHandle getTableHandle()
    {
        return tableHandle;
    }
}
