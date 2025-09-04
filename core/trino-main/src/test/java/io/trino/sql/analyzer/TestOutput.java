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
package io.trino.sql.analyzer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.trino.execution.Column;
import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.connector.CatalogVersion;
import io.trino.sql.analyzer.Analysis.SourceColumn;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestOutput
{
    private static final JsonCodec<Output> codec = JsonCodec.jsonCodec(Output.class);

    @Test
    public void testRoundTrip()
    {
        Output expected = new Output(
                "connectorId",
                new CatalogVersion("default"),
                "schema",
                "table",
                Optional.of(
                        ImmutableList.of(
                                new OutputColumn(
                                        new Column("column", "type"),
                                        ImmutableSet.of(
                                                new SourceColumn(QualifiedObjectName.valueOf("catalog.schema.table"), "column"))))));

        String json = codec.toJson(expected);
        Output actual = codec.fromJson(json);

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testRoundWithComplexIdentifiers()
    {
        Output expected = new Output(
                "catalog.Mój",
                new CatalogVersion("default"),
                "ści.e-Ma",
                "ta.b-Elką go",
                Optional.of(
                        ImmutableList.of(
                                new OutputColumn(
                                        new Column("ko.LU-mieńka", "type"),
                                        ImmutableSet.of(
                                                new SourceColumn(new QualifiedObjectName("catalog.twój", "schema.ściema", "tabel.tabelkówna"), "co-lumn.słodziak\""))))));

        String json = codec.toJson(expected);
        Output actual = codec.fromJson(json);

        assertThat(actual).isEqualTo(expected);
    }
}
