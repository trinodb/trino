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
package io.trino.plugin.duckdb;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import jakarta.validation.constraints.AssertTrue;

import java.util.Set;

import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.plugin.duckdb.DuckDbClientModule.SECURITY_CONNECTION_PROPERTIES;
import static java.util.Locale.ENGLISH;

public class DuckDbJdbcConfig
        extends BaseJdbcConfig
{
    // Options that would grant file access, unlock settings or run SQL from a file despite SECURITY_CONNECTION_PROPERTIES
    static final Set<String> FORBIDDEN_URL_OPTIONS = ImmutableSet.<String>builder()
            .addAll(SECURITY_CONNECTION_PROPERTIES.keySet())
            .add("allowed_configs")
            .add("allowed_directories")
            .add("allowed_paths")
            .add("session_init_sql_file")
            .add("session_init_sql_file_sha256")
            .build();

    // The DuckDB driver lets options in the URL override the connection properties
    @AssertTrue(message = "DuckDB security options cannot be set in connection-url")
    public boolean isUrlWithoutSecurityOptions()
    {
        // Validation also runs when connection-url is missing, which @NotNull on the getter reports
        // The first entry is the database path. An entry without '=' is checked as a key; the driver rejects it anyway
        return Splitter.on(';').splitToStream(nullToEmpty(getConnectionUrl()))
                .skip(1)
                .map(entry -> Splitter.on('=').limit(2).splitToList(entry).getFirst().trim().toLowerCase(ENGLISH))
                .noneMatch(FORBIDDEN_URL_OPTIONS::contains);
    }
}
