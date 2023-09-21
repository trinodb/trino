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
package io.trino.plugin.geospatial;

import com.google.common.collect.ImmutableList;
import io.trino.geospatial.KdbTreeUtils;
import io.trino.geospatial.Rectangle;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.geospatial.KdbTree.buildKdbTree;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestKdbTreeCasts
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        assertions.addPlugin(new GeoPlugin());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void test()
    {
        String kdbTreeJson = makeKdbTreeJson();

        assertThat(assertions.function("typeof", format("cast('%s' AS KdbTree)", kdbTreeJson)))
                .hasType(VARCHAR)
                .isEqualTo("KdbTree");

        assertThat(assertions.function("typeof", format("cast('%s' AS KDBTree)", kdbTreeJson)))
                .hasType(VARCHAR)
                .isEqualTo("KdbTree");

        assertThat(assertions.function("typeof", format("cast('%s' AS kdbTree)", kdbTreeJson)))
                .hasType(VARCHAR)
                .isEqualTo("KdbTree");

        assertThat(assertions.function("typeof", format("cast('%s' AS kdbtree)", kdbTreeJson)))
                .hasType(VARCHAR)
                .isEqualTo("KdbTree");

        assertTrinoExceptionThrownBy(assertions.function("typeof", "cast('' AS KdbTree)")::evaluate)
                .hasMessage("Invalid JSON string for KDB tree")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    private String makeKdbTreeJson()
    {
        ImmutableList.Builder<Rectangle> rectangles = ImmutableList.builder();
        for (double x = 0; x < 10; x += 1) {
            for (double y = 0; y < 5; y += 1) {
                rectangles.add(new Rectangle(x, y, x + 1, y + 2));
            }
        }
        return KdbTreeUtils.toJson(buildKdbTree(100, new Rectangle(0, 0, 9, 4), rectangles.build()));
    }
}
