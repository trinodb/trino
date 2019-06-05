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
package io.prestosql.plugin.blackhole;

import io.prestosql.testing.QueryRunner;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static io.prestosql.plugin.blackhole.BlackHoleQueryRunner.createQueryRunner;

@Test(singleThreaded = true)
public class TestSplitSourceEnding
{
    private QueryRunner queryRunner;

    @BeforeTest
    public void setUp()
            throws Exception
    {
        queryRunner = createQueryRunner();
    }

    @AfterTest(alwaysRun = true)
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    @Test
    public void testQueryCompletes()
    {
        queryRunner.execute(
                "CREATE TABLE x(i int) WITH (" +
                        "close_split_source = false, " +
                        "split_count = 1, " +
                        "pages_per_split = 10, " +
                        "rows_per_page = 10)");
        queryRunner.execute("TABLE x LIMIT 1");
        queryRunner.execute("DROP TABLE x");
    }
}
