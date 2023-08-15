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
package io.trino.plugin.jmx;

import com.google.common.collect.ImmutableSet;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Locale;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.connector.informationschema.InformationSchemaTable.INFORMATION_SCHEMA;
import static io.trino.plugin.jmx.JmxMetadata.HISTORY_SCHEMA_NAME;
import static io.trino.plugin.jmx.JmxMetadata.JMX_SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestJmxQueries
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
            throws Exception
    {
        assertions = new QueryAssertions(JmxQueryRunner.createJmxQueryRunner());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    private static final Set<String> STANDARD_NAMES = ImmutableSet.<String>builder()
            .add("java.lang:type=ClassLoading")
            .add("java.lang:type=Memory")
            .add("java.lang:type=OperatingSystem")
            .add("java.lang:type=Runtime")
            .add("java.lang:type=Threading")
            .add("java.util.logging:type=Logging")
            .build();

    @Test
    public void testShowSchemas()
    {
        assertThat(assertions.query("SHOW SCHEMAS"))
                .matches(result -> result.getOnlyColumnAsSet().equals(ImmutableSet.of(INFORMATION_SCHEMA, JMX_SCHEMA_NAME, HISTORY_SCHEMA_NAME)));
    }

    @Test
    public void testShowTables()
    {
        Set<String> standardNamesLower = STANDARD_NAMES.stream()
                .map(name -> name.toLowerCase(Locale.ENGLISH))
                .collect(toImmutableSet());

        assertThat(assertions.query("SHOW TABLES"))
                .matches(result -> result.getOnlyColumnAsSet().containsAll(standardNamesLower));
    }

    @Test
    public void testQuery()
    {
        for (String name : STANDARD_NAMES) {
            assertThat(assertions.query("SELECT * FROM \"%s\"".formatted(name)))
                    .succeeds();
        }
    }

    @Test
    public void testNodeCount()
    {
        assertThat(assertions.query("SELECT DISTINCT node FROM \"%s\"".formatted(STANDARD_NAMES.iterator().next())))
                .matches("SELECT node_id FROM system.runtime.nodes");
    }

    @Test
    public void testOrderOfParametersIsIgnored()
    {
        assertThat(assertions.query("SELECT node FROM \"java.nio:type=bufferpool,name=direct\""))
                .matches("SELECT node FROM \"java.nio:name=direct,type=bufferpool\"");
    }

    @Test
    public void testQueryCumulativeTable()
    {
        assertThat(assertions.query("SELECT * FROM \"*:*\""))
                .succeeds();

        assertThat(assertions.query("SELECT * FROM \"java.util.logging:*\""))
                .succeeds();

        assertThat(assertions.query("SELECT * FROM \"java.lang:*\""))
                .matches(result -> result.getRowCount() > 1);

        assertThat(assertions.query("SELECT * FROM \"jAVA.LANg:*\""))
                .matches(result -> result.getRowCount() > 1);
    }
}
