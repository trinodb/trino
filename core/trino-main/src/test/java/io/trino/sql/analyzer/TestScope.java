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

import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestScope
{
    @Test
    public void test()
    {
        Scope root = Scope.create();

        Field outerColumn1 = Field.newQualified(QualifiedName.of("outer", "column1"), Optional.of("c1"), BIGINT, false, Optional.empty(), Optional.empty(), false);
        Field outerColumn2 = Field.newQualified(QualifiedName.of("outer", "column2"), Optional.of("c2"), BIGINT, false, Optional.empty(), Optional.empty(), false);
        Scope outer = Scope.builder().withParent(root).withRelationType(RelationId.anonymous(), new RelationType(outerColumn1, outerColumn2)).build();

        Field innerColumn2 = Field.newQualified(QualifiedName.of("inner", "column2"), Optional.of("c2"), BIGINT, false, Optional.empty(), Optional.empty(), false);
        Field innerColumn3 = Field.newQualified(QualifiedName.of("inner", "column3"), Optional.of("c3"), BIGINT, false, Optional.empty(), Optional.empty(), false);
        Scope inner = Scope.builder().withOuterQueryParent(outer).withRelationType(RelationId.anonymous(), new RelationType(innerColumn2, innerColumn3)).build();

        Expression c1 = name("c1");
        Expression c2 = name("c2");
        Expression c3 = name("c3");
        Expression c4 = name("c4");

        assertThat(root.tryResolveField(c1)).isEmpty();

        assertThat(outer.tryResolveField(c1)).isPresent();
        assertThat(outer.tryResolveField(c1).get().getField()).isEqualTo(outerColumn1);
        assertThat(outer.tryResolveField(c1).get().isLocal()).isTrue();
        assertThat(outer.tryResolveField(c1).get().getHierarchyFieldIndex()).isEqualTo(0);
        assertThat(outer.tryResolveField(c2)).isPresent();
        assertThat(outer.tryResolveField(c2).get().getField()).isEqualTo(outerColumn2);
        assertThat(outer.tryResolveField(c2).get().isLocal()).isTrue();
        assertThat(outer.tryResolveField(c2).get().getHierarchyFieldIndex()).isEqualTo(1);
        assertThat(outer.tryResolveField(c3)).isEmpty();
        assertThat(outer.tryResolveField(c4)).isEmpty();

        assertThat(inner.tryResolveField(c1)).isPresent();
        assertThat(inner.tryResolveField(c1).get().getField()).isEqualTo(outerColumn1);
        assertThat(inner.tryResolveField(c1).get().isLocal()).isFalse();
        assertThat(inner.tryResolveField(c1).get().getHierarchyFieldIndex()).isEqualTo(0);
        assertThat(inner.tryResolveField(c1).get().getRelationFieldIndex()).isEqualTo(0);
        assertThat(inner.tryResolveField(c2)).isPresent();
        assertThat(inner.tryResolveField(c2).get().getField()).isEqualTo(innerColumn2);
        assertThat(inner.tryResolveField(c2).get().isLocal()).isTrue();
        assertThat(inner.tryResolveField(c2).get().getHierarchyFieldIndex()).isEqualTo(0);
        assertThat(inner.tryResolveField(c2)).isPresent();
        assertThat(inner.tryResolveField(c3).get().getField()).isEqualTo(innerColumn3);
        assertThat(inner.tryResolveField(c3).get().isLocal()).isTrue();
        assertThat(inner.tryResolveField(c3).get().getHierarchyFieldIndex()).isEqualTo(1);
        assertThat(inner.tryResolveField(c4)).isEmpty();

        assertThat(inner.getOuterQueryParent()).hasValue(outer);
    }

    private static Expression name(String first, String... parts)
    {
        return DereferenceExpression.from(QualifiedName.of(first, parts));
    }
}
