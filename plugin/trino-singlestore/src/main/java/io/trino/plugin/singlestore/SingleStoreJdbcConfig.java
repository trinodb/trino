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
package io.trino.plugin.singlestore;

import io.trino.plugin.jdbc.BaseJdbcConfig;
import jakarta.validation.constraints.AssertFalse;

public class SingleStoreJdbcConfig
        extends BaseJdbcConfig
{
    public static final String DRIVER_PROTOCOL_ERROR = "This connector uses the official Single Store JDBC Driver. As a result, `connection-url` in catalog " +
            " configuration files needs to be updated from `jdbc:mariadb:...` to `jdbc:singlestore:...`";

    @AssertFalse(message = DRIVER_PROTOCOL_ERROR)
    public boolean isLegacyDriverConnectionUrl()
    {
        // including "jdbc:" portion in case-insensitive match in case other url parts also contain "mariadb" literal
        return getConnectionUrl().matches("(?i)jdbc:mariadb:.*");
    }
}
