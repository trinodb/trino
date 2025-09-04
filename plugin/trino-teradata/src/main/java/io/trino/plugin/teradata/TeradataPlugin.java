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

package io.trino.plugin.teradata;

import io.trino.plugin.jdbc.JdbcPlugin;

/**
 * Teradata plugin for Trino.
 * <p>
 * This class registers the Teradata connector plugin with Trino,
 * extending the base {@link JdbcPlugin} class.
 * It provides the connector name ("teradata") and the client module
 * to use for Teradata JDBC connectivity.
 * </p>
 */
public class TeradataPlugin
        extends JdbcPlugin
{
    /**
     * Constructs a new TeradataPlugin instance.
     * <p>
     * The plugin is identified by the name "teradata" and
     * uses the {@link TeradataClientModule} to configure
     * the JDBC client for Teradata.
     * </p>
     */
    public TeradataPlugin()
    {
        super("teradata", TeradataClientModule::new);
    }
}
