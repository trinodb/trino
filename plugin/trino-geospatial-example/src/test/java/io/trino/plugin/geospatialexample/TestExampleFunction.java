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
package io.trino.plugin.geospatialexample;

import io.trino.plugin.geospatial.GeoPlugin;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestExampleFunction
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        assertions.addPlugin(new GeoPlugin());
        assertions.addPlugin(new GeospatialExamplePlugin());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testExample()
    {
        assertThat(assertions.expression("geospatial_example(ST_POINT(1.234, 1.234))"))
                .hasType(VARCHAR)
                .isEqualTo("works as expected, type Point");
    }
}
