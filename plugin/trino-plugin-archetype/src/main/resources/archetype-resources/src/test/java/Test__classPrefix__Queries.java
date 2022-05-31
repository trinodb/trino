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

package $package;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

public class Test${classPrefix}Queries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return ${classPrefix}QueryRunner.createQueryRunner();
    }

    @Test
    public void showTables()
    {
        assertQuery("SHOW SCHEMAS FROM $connectorName", "VALUES 'default', 'information_schema'");
        assertQuery("SHOW TABLES FROM ${connectorName}.default", "VALUES 'single_row'");
    }

    @Test
    public void selectFromTable()
    {
        assertQuery("SELECT name FROM single_row WHERE id = 'x'",
                "VALUES ('my-name')");
    }
}
