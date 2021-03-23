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
package io.trino.tests;

import io.trino.connector.CatalogName;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.SkipException;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.createBogusTestingCatalog;

public class TestLocalEngineOnlyQueries
        extends AbstractTestEngineOnlyQueries
{
    @Override
    protected QueryRunner createQueryRunner()
    {
        LocalQueryRunner queryRunner = TestLocalQueries.createLocalQueryRunner();
        try {
            // for testing session properties
            queryRunner.getMetadata().getSessionPropertyManager().addSystemSessionProperties(TEST_SYSTEM_PROPERTIES);
            queryRunner.getCatalogManager().registerCatalog(createBogusTestingCatalog(TESTING_CATALOG));
            queryRunner.getMetadata().getSessionPropertyManager().addConnectorSessionProperties(new CatalogName(TESTING_CATALOG), TEST_CATALOG_PROPERTIES);
        }
        catch (RuntimeException e) {
            throw closeAllSuppress(e, queryRunner);
        }
        return queryRunner;
    }

    @Override
    public void testSetSession()
    {
        throw new SkipException("SET SESSION is not supported by LocalQueryRunner");
    }

    @Override
    public void testResetSession()
    {
        throw new SkipException("RESET SESSION is not supported by LocalQueryRunner");
    }
}
