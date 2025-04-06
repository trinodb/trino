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
package io.trino.plugin.ignite;

import io.trino.plugin.jdbc.BaseJdbcConfig;
import jakarta.validation.constraints.AssertTrue;
import org.apache.ignite.jdbc.IgniteJdbcDriver;

import static org.apache.ignite.internal.jdbc.ConnectionPropertiesImpl.URL_PREFIX;

public class IgniteJdbcConfig
        extends BaseJdbcConfig
{
    @AssertTrue(message = "JDBC URL for Ignite connector should start with " + URL_PREFIX)
    public boolean isUrlValid()
    {
        return new IgniteJdbcDriver().acceptsURL(getConnectionUrl());
    }
}
