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

import io.trino.filesystem.Location;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestAzureLocation
{
    @Test
    void test()
    {
        assertValid("abfs://container@account.dfs.core.windows.net/some/path/file", "account", "container", "some/path/file", "abfs", "core.windows.net");
        assertValid("abfss://container@account.dfs.core.windows.net/some/path/file", "account", "container", "some/path/file", "abfss", "core.windows.net");
        assertValid("wasb://container@account.blob.core.windows.net/some/path/file", "account", "container", "some/path/file", "wasb", "core.windows.net");
        assertValid("wasbs://container@account.blob.core.windows.net/some/path/file", "account", "container", "some/path/file", "wasbs", "core.windows.net");

        assertValid("abfs://container-stuff@account.dfs.core.windows.net/some/path/file", "account", "container-stuff", "some/path/file", "abfs", "core.windows.net");
        assertValid("abfs://container2@account.dfs.core.windows.net/some/path/file", "account", "container2", "some/path/file", "abfs", "core.windows.net");
        assertValid("abfs://account.dfs.core.windows.net/some/path/file", "account", null, "some/path/file", "abfs", "core.windows.net");

        assertValid("abfs://container@account.dfs.core.windows.net/file", "account", "container", "file", "abfs", "core.windows.net");
        assertValid("abfs://container@account0.dfs.core.windows.net///f///i///l///e///", "account0", "container", "//f///i///l///e///", "abfs", "core.windows.net");

        // other endpoints are allowed
        assertValid("abfs://container@account.dfs.core.usgovcloudapi.net/some/path/file", "account", "container", "some/path/file", "abfs", "core.usgovcloudapi.net");
        assertValid("abfss://container@account.dfs.core.usgovcloudapi.net/some/path/file", "account", "container", "some/path/file", "abfss", "core.usgovcloudapi.net");
        assertValid("wasb://container@account.blob.core.usgovcloudapi.net/some/path/file", "account", "container", "some/path/file", "wasb", "core.usgovcloudapi.net");
        assertValid("wasbs://container@account.blob.core.usgovcloudapi.net/some/path/file", "account", "container", "some/path/file", "wasbs", "core.usgovcloudapi.net");

        // abfs[s] host must contain ".dfs.", and wasb[s] host must contain ".blob." before endpoint
        assertInvalid("abfs://container@account.invalid.core.usgovcloudapi.net/some/path/file");
        assertInvalid("abfss://container@account.invalid.core.usgovcloudapi.net/some/path/file");
        assertInvalid("wasb://container@account.invalid.core.usgovcloudapi.net/some/path/file");
        assertInvalid("abfs://container@account.blob.core.usgovcloudapi.net/some/path/file");
        assertInvalid("abfss://container@account.blob.core.usgovcloudapi.net/some/path/file");
        assertInvalid("wasb://container@account.dfs.core.usgovcloudapi.net/some/path/file");
        assertInvalid("wasbs://container@account.dfs.core.usgovcloudapi.net/some/path/file");

        // only abfs, abfss, wasb, wasbs schemes allowed
        assertInvalid("https://container@account.dfs.core.windows.net/some/path/file");

        // host must have at least to labels
        assertInvalid("abfs://container@account/some/path/file");
        assertInvalid("abfs://container@/some/path/file");

        // schema and authority are required
        assertInvalid("abfs:///some/path/file");
        assertInvalid("/some/path/file");

        // container is only a-z, 0-9, and dash, and cannot start or end with dash or contain consecutive dashes
        assertInvalid("abfs://ConTainer@account.dfs.core.windows.net/some/path/file");
        assertInvalid("abfs://con_tainer@account.dfs.core.windows.net/some/path/file");
        assertInvalid("abfs://con$tainer@account.dfs.core.windows.net/some/path/file");
        assertInvalid("abfs://-container@account.dfs.core.windows.net/some/path/file");
        assertInvalid("abfs://container-@account.dfs.core.windows.net/some/path/file");
        assertInvalid("abfs://con---tainer@account.dfs.core.windows.net/some/path/file");
        assertInvalid("abfs://con--tainer@account.dfs.core.windows.net/some/path/file");

        // account is only a-z and 0-9
        assertInvalid("abfs://container@ac-count.dfs.core.windows.net/some/path/file");
        assertInvalid("abfs://container@ac_count.dfs.core.windows.net/some/path/file");
        assertInvalid("abfs://container@ac$count.dfs.core.windows.net/some/path/file");

        // host must contain .dfs. after account
        assertInvalid("abfs://container@account.example.com/some/path/file");
        assertInvalid("abfs://container@account.fake.dfs.core.windows.net/some/path/file");
    }

    private static void assertValid(String uri, String expectedAccount, String expectedContainer, String expectedPath, String expectedScheme, String expectedEndpoint)
    {
        Location location = Location.of(uri);
        AzureLocation azureLocation = new AzureLocation(location);
        assertThat(azureLocation.location()).isEqualTo(location);
        assertThat(azureLocation.account()).isEqualTo(expectedAccount);
        assertThat(azureLocation.endpoint()).isEqualTo(expectedEndpoint);
        assertThat(azureLocation.container()).isEqualTo(Optional.ofNullable(expectedContainer));
        assertThat(azureLocation.path()).contains(expectedPath);
        assertThat(azureLocation.baseLocation().scheme()).isEqualTo(Optional.of(expectedScheme));
    }

    private static void assertInvalid(String uri)
    {
        Location location = Location.of(uri);
        assertThatThrownBy(() -> new AzureLocation(location))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(uri);
    }
}
