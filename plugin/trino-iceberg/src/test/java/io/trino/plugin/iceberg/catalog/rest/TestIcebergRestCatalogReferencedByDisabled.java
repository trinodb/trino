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
package io.trino.plugin.iceberg.catalog.rest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestIcebergRestCatalogReferencedByDisabled
        extends BaseIcebergRestCatalogReferencedByTest
{
    public TestIcebergRestCatalogReferencedByDisabled()
    {
        super(false);
    }

    @Test
    void testViewsAreNotSentByDefault()
    {
        String table = "t_" + randomNameSuffix();
        String innerView = "inner_view_" + randomNameSuffix();
        String outerView = "outer_view_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " AS SELECT 1 x", 1);
        assertUpdate("CREATE VIEW " + innerView + " AS SELECT * FROM " + table);
        assertUpdate("CREATE VIEW " + outerView + " AS SELECT * FROM " + innerView);

        assertThat(query("SELECT * FROM " + outerView)).matches("VALUES 1");

        assertThat(referencedByOf(table)).isNotEmpty().allSatisfy(views -> assertThat(views).isEmpty());

        assertUpdate("DROP VIEW " + outerView);
        assertUpdate("DROP VIEW " + innerView);
        assertUpdate("DROP TABLE " + table);
    }
}
