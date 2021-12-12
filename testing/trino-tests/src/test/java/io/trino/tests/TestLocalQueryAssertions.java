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

import io.trino.Session;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test {@link io.trino.sql.query.QueryAssertions} with {@link io.trino.testing.LocalQueryRunner}
 */
public class TestLocalQueryAssertions
        extends BaseQueryAssertionsTest
{
    @Override
    protected QueryRunner createQueryRunner()
    {
        LocalQueryRunner queryRunner = LocalQueryRunner.builder(createSession()).build();

        try {
            configureCatalog(queryRunner);
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }

        return queryRunner;
    }

    @Override
    public void testIsFullyPushedDown()
    {
        assertThatThrownBy(() -> assertThat(query("SELECT name FROM nation")).isFullyPushedDown())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("isFullyPushedDown() currently does not work with LocalQueryRunner");
    }

    @Override
    public void testIsFullyPushedDownWithSession()
    {
        Session baseSession = Session.builder(getSession())
                .setCatalog("jdbc_with_aggregation_pushdown_disabled")
                .build();

        assertThatThrownBy(() -> assertThat(query(baseSession, "SELECT name FROM nation")).isFullyPushedDown())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("isFullyPushedDown() currently does not work with LocalQueryRunner");
    }

    @Test
    public void testNullInErrorMessage()
    {
        assertThatThrownBy(() -> assertThat(query("SELECT CAST(null AS integer)")).matches("SELECT 1"))
                .hasMessage("[Rows] \n" +
                        "Expecting:\n" +
                        "  <(null)>\n" +
                        "to contain exactly in any order:\n" +
                        "  <[(1)]>\n" +
                        "elements not found:\n" +
                        "  <(1)>\n" +
                        "and elements not expected:\n" +
                        "  <(null)>\n");

        assertThatThrownBy(() -> assertThat(query("SELECT 1")).matches("SELECT CAST(null AS integer)"))
                .hasMessage("[Rows] \n" +
                        "Expecting:\n" +
                        "  <(1)>\n" +
                        "to contain exactly in any order:\n" +
                        "  <[(null)]>\n" +
                        "elements not found:\n" +
                        "  <(null)>\n" +
                        "and elements not expected:\n" +
                        "  <(1)>\n");
    }
}
