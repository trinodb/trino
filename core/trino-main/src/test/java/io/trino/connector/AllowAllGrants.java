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
package io.trino.connector;

import io.trino.spi.TrinoException;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.TrinoPrincipal;

import java.util.Set;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public class AllowAllGrants<T>
        implements Grants<T>
{
    @Override
    public void grant(TrinoPrincipal principal, T objectName, Set<Privilege> privileges, boolean grantOption)
    {
        throw new TrinoException(NOT_SUPPORTED, "Grant operation is not supported");
    }

    @Override
    public void revoke(TrinoPrincipal principal, T objectName, Set<Privilege> privileges, boolean grantOption)
    {
        throw new TrinoException(NOT_SUPPORTED, "Revoke operation is not supported");
    }

    @Override
    public boolean isAllowed(String user, T objectName, Privilege privilege)
    {
        return true;
    }

    @Override
    public boolean canGrant(String user, T objectName, Privilege privilege)
    {
        return true;
    }
}
