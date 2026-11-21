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
package io.trino.type;

import io.trino.spi.type.RowType;
import io.trino.spi.type.TypeDescriptor;

import java.util.List;

import static io.trino.spi.type.IntegerType.INTEGER;

public final class IpAddressWithPortType
        extends RowType
{
    public static final String NAME = "ipaddress_with_port";
    public static final IpAddressWithPortType IPADDRESS_WITH_PORT = new IpAddressWithPortType();

    private IpAddressWithPortType()
    {
        super(new TypeDescriptor(NAME),
                List.of(
                        field("ip_address", IpAddressType.IPADDRESS),
                        field("port", INTEGER)));
    }

    @Override
    public String getDisplayName()
    {
        return NAME;
    }
}
