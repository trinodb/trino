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
package io.trino.filesystem.azure;

import com.google.common.base.CharMatcher;
import io.trino.filesystem.Location;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

class AzureLocation
{
    private static final String INVALID_LOCATION_MESSAGE = "Invalid Azure location. Expected form is 'abfs://[<containerName>@]<accountName>.dfs.core.windows.net/<filePath>': %s";

    // https://learn.microsoft.com/en-us/azure/azure-resource-manager/management/resource-name-rules
    private static final CharMatcher CONTAINER_VALID_CHARACTERS = CharMatcher.inRange('a', 'z').or(CharMatcher.inRange('0', '9')).or(CharMatcher.is('-'));
    private static final CharMatcher STORAGE_ACCOUNT_VALID_CHARACTERS = CharMatcher.inRange('a', 'z').or(CharMatcher.inRange('0', '9'));

    private final Location location;
    private final String account;

    public AzureLocation(Location location)
    {
        this.location = requireNonNull(location, "location is null");
        // abfss is also supported but not documented
        String scheme = location.scheme().orElseThrow(() -> new IllegalArgumentException(String.format(INVALID_LOCATION_MESSAGE, location)));
        checkArgument("abfs".equals(scheme) || "abfss".equals(scheme), INVALID_LOCATION_MESSAGE, location);

        // container is interpolated into the URL path, so perform extra checks
        location.userInfo().ifPresent(container -> {
            checkArgument(!container.isEmpty(), INVALID_LOCATION_MESSAGE, location);
            checkArgument(
                    CONTAINER_VALID_CHARACTERS.matchesAllOf(container),
                    "Invalid Azure storage container name. Valid characters are 'a-z', '0-9', and '-': %s",
                    location);
            checkArgument(
                    !container.startsWith("-") && !container.endsWith("-"),
                    "Invalid Azure storage container name. Cannot start or end with a hyphen: %s",
                    location);
            checkArgument(
                    !container.contains("--"),
                    "Invalid Azure storage container name. Cannot contain consecutive hyphens: %s",
                    location);
        });

        // storage account is the first label of the host
        checkArgument(location.host().isPresent(), INVALID_LOCATION_MESSAGE, location);
        String host = location.host().get();
        int accountSplit = host.indexOf('.');
        checkArgument(
                accountSplit > 0,
                INVALID_LOCATION_MESSAGE,
                this.location);
        this.account = host.substring(0, accountSplit);

        // host must end with ".dfs.core.windows.net"
        checkArgument(host.substring(accountSplit).equals(".dfs.core.windows.net"), INVALID_LOCATION_MESSAGE, location);

        // storage account is interpolated into URL host name, so perform extra checks
        checkArgument(STORAGE_ACCOUNT_VALID_CHARACTERS.matchesAllOf(account),
                "Invalid Azure storage account name. Valid characters are 'a-z' and '0-9': %s",
                location);
    }

    /**
     * Creates a new {@link AzureLocation} based on the storage account, container and blob path parsed from the location.
     * <p>
     * Locations follow the conventions used by
     * <a href="https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction-abfs-uri">ABFS URI</a>
     * that follows the following convention
     * <pre>{@code abfs://<container-name>@<storage-account-name>.dfs.core.windows.net/<blob_path>}</pre>
     */
    public static AzureLocation from(String location)
    {
        return new AzureLocation(Location.of(location));
    }

    public Location location()
    {
        return location;
    }

    public Optional<String> container()
    {
        return location.userInfo();
    }

    public String account()
    {
        return account;
    }

    public String path()
    {
        return location.path();
    }

    @Override
    public String toString()
    {
        return location.toString();
    }
}
