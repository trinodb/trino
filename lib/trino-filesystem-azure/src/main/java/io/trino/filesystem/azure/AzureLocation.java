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
    private static final String INVALID_LOCATION_MESSAGE = "Invalid Azure location. Expected form is 'abfs://[<containerName>@]<accountName>.dfs.<endpoint>/<filePath>': %s";

    // https://learn.microsoft.com/en-us/azure/azure-resource-manager/management/resource-name-rules
    private static final CharMatcher CONTAINER_VALID_CHARACTERS = CharMatcher.inRange('a', 'z').or(CharMatcher.inRange('0', '9')).or(CharMatcher.is('-'));
    private static final CharMatcher STORAGE_ACCOUNT_VALID_CHARACTERS = CharMatcher.inRange('a', 'z').or(CharMatcher.inRange('0', '9'));

    private final Location location;
    private final String scheme;
    private final String account;
    private final String endpoint;

    /**
     * Creates a new location based on the endpoint, storage account, container and blob path parsed from the location.
     * <p>
     * Locations use the
     * <a href="https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction-abfs-uri">ABFS URI</a> syntax:
     * <pre>{@code abfs://<container-name>@<storage-account-name>.dfs.<endpoint>/<blob_path>}</pre>
     */
    public AzureLocation(Location location)
    {
        this.location = requireNonNull(location, "location is null");
        // abfss is also supported but not documented
        scheme = location.scheme().orElseThrow(() -> new IllegalArgumentException(String.format(INVALID_LOCATION_MESSAGE, location)));
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

        // host must contain ".dfs." before endpoint
        checkArgument(host.substring(accountSplit).startsWith(".dfs."), INVALID_LOCATION_MESSAGE, location);

        // endpoint is the part after ".dfs."
        this.endpoint = host.substring(accountSplit + ".dfs.".length());
        checkArgument(!endpoint.isEmpty(), INVALID_LOCATION_MESSAGE, location);

        // storage account is interpolated into URL host name, so perform extra checks
        checkArgument(STORAGE_ACCOUNT_VALID_CHARACTERS.matchesAllOf(account),
                "Invalid Azure storage account name. Valid characters are 'a-z' and '0-9': %s",
                location);
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

    public String endpoint()
    {
        return endpoint;
    }

    public String path()
    {
        return location.path();
    }

    public String directoryPath()
    {
        String path = location.path();
        if (!path.isEmpty() && !path.endsWith("/")) {
            path += "/";
        }
        return path;
    }

    @Override
    public String toString()
    {
        return location.toString();
    }

    public Location baseLocation()
    {
        return Location.of("%s://%s%s.dfs.%s/".formatted(
                scheme,
                container().map(container -> container + "@").orElse(""),
                account(),
                endpoint));
    }
}
