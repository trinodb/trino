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
package io.trino.plugin.jdbc;

import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.TestingConnectorBehavior;

public abstract class BaseJdbcConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_UPDATE: // not supported by any JDBC connector
            case SUPPORTS_MERGE: // not supported by any JDBC connector
                return false;

            case SUPPORTS_CREATE_VIEW: // not supported by DefaultJdbcMetadata
            case SUPPORTS_CREATE_MATERIALIZED_VIEW: // not supported by DefaultJdbcMetadata
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }
}
