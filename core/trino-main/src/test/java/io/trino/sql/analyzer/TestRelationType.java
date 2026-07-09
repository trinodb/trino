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

import io.trino.sql.tree.QualifiedName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRelationType
{
    @Test
    public void testResolveFieldsByName()
    {
        Field column1 = qualifiedField("t", "c1");
        Field column2 = qualifiedField("t", "c2");
        RelationType relationType = new RelationType(column1, column2);

        assertThat(relationType.resolveFields(QualifiedName.of("c1"))).containsExactly(column1);
        assertThat(relationType.resolveFields(QualifiedName.of("c2"))).containsExactly(column2);
        assertThat(relationType.resolveFields(QualifiedName.of("c3"))).isEmpty();

        assertThat(relationType.canResolve(QualifiedName.of("c1"))).isTrue();
        assertThat(relationType.canResolve(QualifiedName.of("c3"))).isFalse();
    }

    @Test
    public void testResolveFieldsCaseInsensitively()
    {
        Field column = qualifiedField("t", "MixedCase");
        RelationType relationType = new RelationType(column);

        assertThat(relationType.resolveFields(QualifiedName.of("mixedcase"))).containsExactly(column);
        assertThat(relationType.resolveFields(QualifiedName.of("MIXEDCASE"))).containsExactly(column);
        assertThat(relationType.resolveFields(QualifiedName.of("MixedCase"))).containsExactly(column);
    }

    @Test
    public void testResolveFieldsMatchesEqualsIgnoreCaseSemantics()
    {
        // GREEK SMALL LETTER FINAL SIGMA is a canary for the difference between
        // String.equalsIgnoreCase and String.toLowerCase based comparisons
        Field column = qualifiedField("t", "colς");
        RelationType relationType = new RelationType(column);

        assertThat(relationType.resolveFields(QualifiedName.of("colς"))).containsExactly(column);
        assertThat(relationType.resolveFields(QualifiedName.of("colσ"))).containsExactly(column);
        assertThat(relationType.resolveFields(QualifiedName.of("colΣ"))).containsExactly(column);
    }

    @Test
    public void testResolveFieldsWithRelationPrefix()
    {
        Field column = qualifiedField("t", "c1");
        RelationType relationType = new RelationType(column);

        assertThat(relationType.resolveFields(QualifiedName.of("t", "c1"))).containsExactly(column);
        assertThat(relationType.resolveFields(QualifiedName.of("other", "c1"))).isEmpty();
    }

    @Test
    public void testResolveFieldsWithDuplicateNames()
    {
        Field leftColumn = qualifiedField("left", "c1");
        Field rightColumn = qualifiedField("right", "c1");
        RelationType relationType = new RelationType(leftColumn, rightColumn);

        assertThat(relationType.resolveFields(QualifiedName.of("c1"))).containsExactly(leftColumn, rightColumn);
        assertThat(relationType.resolveFields(QualifiedName.of("left", "c1"))).containsExactly(leftColumn);
        assertThat(relationType.resolveFields(QualifiedName.of("right", "c1"))).containsExactly(rightColumn);
    }

    @Test
    public void testResolveHiddenField()
    {
        Field hidden = Field.newQualified(QualifiedName.of("t"), Optional.of("c1"), BIGINT, true, Optional.empty(), Optional.empty(), Optional.empty(), false);
        RelationType relationType = new RelationType(hidden);

        assertThat(relationType.resolveFields(QualifiedName.of("c1"))).containsExactly(hidden);
    }

    @Test
    public void testAnonymousFieldsDoNotResolve()
    {
        Field anonymous = Field.newUnqualified(Optional.empty(), BIGINT);
        RelationType relationType = new RelationType(anonymous);

        assertThat(relationType.resolveFields(QualifiedName.of("c1"))).isEmpty();
    }

    private static Field qualifiedField(String relation, String name)
    {
        return Field.newQualified(QualifiedName.of(relation), Optional.of(name), BIGINT, false, Optional.empty(), Optional.empty(), Optional.empty(), false);
    }
}
