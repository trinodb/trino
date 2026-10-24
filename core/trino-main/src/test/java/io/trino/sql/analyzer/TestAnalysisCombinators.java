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

import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.eventlistener.ColumnDetail;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static io.trino.spi.eventlistener.ColumnTransformationType.AGGREGATION;
import static io.trino.spi.eventlistener.ColumnTransformationType.IDENTITY;
import static io.trino.spi.eventlistener.ColumnTransformationType.TRANSFORMATION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAnalysisCombinators
{
    @Test
    public void testMergeAcrossPathsKeepsEveryDistinctSubtype()
    {
        // A source column reaching an output through several paths keeps every subtype, not just the most exposing one.
        assertThat(Analysis.mergeAcrossPaths(Set.of(IDENTITY), Set.of(AGGREGATION))).containsExactlyInAnyOrder(IDENTITY, AGGREGATION);
        assertThat(Analysis.mergeAcrossPaths(Set.of(AGGREGATION), Set.of(TRANSFORMATION))).containsExactlyInAnyOrder(AGGREGATION, TRANSFORMATION);
        assertThat(Analysis.mergeAcrossPaths(Set.of(AGGREGATION), Set.of(AGGREGATION))).containsExactly(AGGREGATION);
    }

    @Test
    public void testCombineAlongPathAggregationSticks()
    {
        assertThat(Analysis.combineAlongPath(IDENTITY, TRANSFORMATION)).isEqualTo(TRANSFORMATION);
        assertThat(Analysis.combineAlongPath(AGGREGATION, TRANSFORMATION)).isEqualTo(AGGREGATION);
        assertThat(Analysis.combineAlongPath(IDENTITY, IDENTITY)).isEqualTo(IDENTITY);
    }

    @Test
    public void testSourceColumnCarriesSubtypesButExcludesThemFromEquals()
    {
        QualifiedObjectName table = new QualifiedObjectName("c", "s", "t");
        Analysis.SourceColumn plain = new Analysis.SourceColumn(table, "col");
        Analysis.SourceColumn typed = plain.withTransformationTypes(Set.of(AGGREGATION));

        assertThat(plain.getTransformationTypes()).isEmpty();
        assertThat(typed.getTransformationTypes()).containsExactly(AGGREGATION);
        assertThat(typed).isEqualTo(plain);
        assertThat(typed.hashCode()).isEqualTo(plain.hashCode());

        ColumnDetail detail = typed.getColumnDetail();
        assertThat(detail.getTransformationTypes()).containsExactly(AGGREGATION);
        assertThat(detail.getCatalog()).isEqualTo("c");
        assertThat(detail.getColumnName()).isEqualTo("col");
    }
}
