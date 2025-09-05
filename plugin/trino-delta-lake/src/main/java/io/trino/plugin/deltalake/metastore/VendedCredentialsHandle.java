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
package io.trino.plugin.deltalake.metastore;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record VendedCredentialsHandle(
        boolean catalogOwned,
        boolean managed,
        String tableLocation,
        VendedCredentials vendedCredentials)
{
    public VendedCredentialsHandle
    {
        requireNonNull(tableLocation, "tableLocation is null");
        requireNonNull(vendedCredentials, "vendedCredentials is null");

        if (catalogOwned) {
            checkArgument(managed, "catalog-owned table must be managed");
        }
    }

    public static VendedCredentialsHandle empty(String tableLocation)
    {
        return new VendedCredentialsHandle(false, false, tableLocation, VendedCredentials.empty());
    }

    public static VendedCredentialsHandle of(DeltaMetastoreTable table)
    {
        return new VendedCredentialsHandle(table.catalogOwned(), table.managed(), table.location(), table.vendedCredentials().orElse(VendedCredentials.empty()));
    }
}
