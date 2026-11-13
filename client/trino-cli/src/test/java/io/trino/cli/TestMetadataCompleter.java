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
package io.trino.cli;

import io.trino.cli.MetadataCompleter.Metadata;
import io.trino.cli.MetadataCompleter.TableName;
import org.jline.reader.Candidate;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.cli.MetadataCompleter.complete;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMetadataCompleter
{
    private static final Metadata METADATA = new Metadata()
    {
        private final Map<String, List<String>> schemas = Map.of(
                "tpch", List.of("tiny", "sf1"),
                "hive", List.of("config", "web"));
        private final Map<String, List<String>> tables = Map.of(
                "tpch.tiny", List.of("nation", "region"),
                "hive.config", List.of("settings"));
        private final Map<String, List<String>> columns = Map.of(
                "tpch.tiny.nation", List.of("nationkey", "name", "regionkey"),
                "tpch.tiny.region", List.of("regionkey", "name", "comment"),
                "hive.config.settings", List.of("key", "value"));

        @Override
        public List<String> catalogs()
        {
            return List.of("hive", "tpch");
        }

        @Override
        public List<String> schemas(String catalog)
        {
            return schemas.getOrDefault(catalog, List.of());
        }

        @Override
        public List<String> tables(String catalog, String schema)
        {
            return tables.getOrDefault(catalog + "." + schema, List.of());
        }

        @Override
        public List<String> columns(TableName table)
        {
            return columns.getOrDefault(table.catalog() + "." + table.schema() + "." + table.table(), List.of());
        }

        @Override
        public List<String> functions()
        {
            return List.of("abs", "nullif");
        }
    };

    @Test
    public void testCatalogsWithoutSession()
    {
        assertThat(values(Optional.empty(), Optional.empty(), "SELECT * FROM ", "")).containsExactly("hive", "tpch", "abs", "nullif");
        assertThat(values(Optional.empty(), Optional.empty(), "SELECT * FROM t", "t")).containsExactly("tpch");
    }

    @Test
    public void testSchemasAndTables()
    {
        assertThat(values(Optional.empty(), Optional.empty(), "SELECT * FROM tpch.", "tpch.")).containsExactly("tpch.tiny", "tpch.sf1");
        assertThat(values(Optional.empty(), Optional.empty(), "SELECT * FROM tpch.tiny.n", "tpch.tiny.n")).containsExactly("tpch.tiny.nation");
        assertThat(values(Optional.of("tpch"), Optional.empty(), "SELECT * FROM tiny.r", "tiny.r")).containsExactly("tiny.region");
        assertThat(values(Optional.of("tpch"), Optional.of("tiny"), "SELECT * FROM n", "n")).containsExactly("nation", "nullif");
    }

    @Test
    public void testCatalogAndSchemaAreNotTerminated()
    {
        List<Candidate> candidates = complete(METADATA, Optional.empty(), Optional.empty(), "SELECT * FROM tp", "tp");
        assertThat(candidates).extracting(Candidate::complete).containsExactly(false);
    }

    @Test
    public void testUseStatement()
    {
        assertThat(values(Optional.of("hive"), Optional.of("config"), "USE ", "")).containsExactly("hive", "tpch", "config", "web");
        assertThat(values(Optional.empty(), Optional.empty(), "use tpch.t", "tpch.t")).containsExactly("tpch.tiny");
        List<Candidate> candidates = complete(METADATA, Optional.empty(), Optional.empty(), "USE tpch.t", "tpch.t");
        assertThat(candidates).extracting(Candidate::complete).containsExactly(true);
    }

    @Test
    public void testColumnsOfReferencedTables()
    {
        assertThat(values(Optional.of("tpch"), Optional.of("tiny"), "SELECT * FROM nation WHERE re", "re")).containsExactly("region", "regionkey");
        assertThat(values(Optional.empty(), Optional.empty(), "SELECT na FROM tpch.tiny.nation", "na")).containsExactly("nationkey", "name");
        assertThat(values(Optional.of("hive"), Optional.of("config"), "SELECT k FROM settings JOIN tpch.tiny.nation ON true", "k"))
                .containsExactly("key");
        assertThat(values(Optional.of("hive"), Optional.of("web"), "SELECT count(k FROM config.settings", "count(k"))
                .containsExactly("count(key");
    }

    @Test
    public void testQualifiedColumns()
    {
        assertThat(values(Optional.of("tpch"), Optional.of("tiny"), "SELECT n.n FROM nation n", "n.n")).containsExactly("n.nationkey", "n.name");
        assertThat(values(Optional.of("tpch"), Optional.of("tiny"), "SELECT r.c FROM nation JOIN region AS r ON true", "r.c")).containsExactly("r.comment");
        assertThat(values(Optional.of("tpch"), Optional.of("tiny"), "SELECT nation.r FROM nation WHERE true", "nation.r")).containsExactly("nation.regionkey");
        assertThat(values(Optional.of("tpch"), Optional.empty(), "SELECT tiny.region.c", "tiny.region.c")).containsExactly("tiny.region.comment");
        assertThat(values(Optional.empty(), Optional.empty(), "SELECT hive.config.settings.v", "hive.config.settings.v")).containsExactly("hive.config.settings.value");
    }

    private static List<String> values(Optional<String> catalog, Optional<String> schema, String statement, String word)
    {
        return complete(METADATA, catalog, schema, statement, word).stream()
                .map(Candidate::value)
                .collect(toImmutableList());
    }
}
