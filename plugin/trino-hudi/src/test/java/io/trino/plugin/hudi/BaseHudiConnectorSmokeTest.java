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
package io.trino.plugin.hudi;

import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.TestingConnectorBehavior;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseHudiConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_INSERT:
            case SUPPORTS_DELETE:
            case SUPPORTS_UPDATE:
            case SUPPORTS_MERGE:
                return false;

            case SUPPORTS_CREATE_SCHEMA:
                return false;

            case SUPPORTS_CREATE_TABLE:
            case SUPPORTS_RENAME_TABLE:
                return false;

            case SUPPORTS_CREATE_VIEW:
            case SUPPORTS_CREATE_MATERIALIZED_VIEW:
                return false;

            case SUPPORTS_COMMENT_ON_COLUMN:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    public void testShowCreateTable()
    {
        // Override because Hudi connector contains 'location' table property
        String schema = getSession().getSchema().orElseThrow();
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .matches("\\QCREATE TABLE hudi." + schema + ".region (\n" +
                        "   regionkey bigint,\n" +
                        "   name varchar(25),\n" +
                        "   comment varchar(152)\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   location = \\E'.*/region'\n\\Q" +
                        ")");
    }
}
