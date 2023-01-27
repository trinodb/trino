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
package io.trino.plugin.clickhouse;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.QueryRunner;

import static io.trino.plugin.clickhouse.ClickHouseQueryRunner.createClickHouseQueryRunner;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestClickHouseConnectorTest
        extends BaseClickHouseConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.clickhouseServer = closeAfterClass(new TestingClickHouseServer());
        return createClickHouseQueryRunner(
                clickhouseServer,
                ImmutableMap.of(),
                ImmutableMap.of("clickhouse.map-string-as-varchar", "true"),
                REQUIRED_TPCH_TABLES);
    }

    @Override
    public void testCommentTable()
    {
        assertThatThrownBy(super::testCommentTable)
                .hasMessageContaining("Code: 62, e.displayText() = DB::Exception: Syntax error");
    }

    @Override
    public void testCommentTableSpecialCharacter(String comment)
    {
        // Table comment is unsupported in old ClickHouse version
        assertThatThrownBy(() -> super.testCommentTableSpecialCharacter(comment))
                .hasMessageMatching("(?s).* Syntax error: .* COMMENT .*");
    }
}
