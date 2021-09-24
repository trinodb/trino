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
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

        assertFalse(root.tryResolveField(c1).isPresent());

        assertTrue(outer.tryResolveField(c1).isPresent());
        assertEquals(outerColumn1, outer.tryResolveField(c1).get().getField());
        assertTrue(outer.tryResolveField(c1).get().isLocal());
        assertEquals(0, outer.tryResolveField(c1).get().getHierarchyFieldIndex());
        assertTrue(outer.tryResolveField(c2).isPresent());
        assertEquals(outerColumn2, outer.tryResolveField(c2).get().getField());
        assertTrue(outer.tryResolveField(c2).get().isLocal());
        assertEquals(1, outer.tryResolveField(c2).get().getHierarchyFieldIndex());
        assertFalse(outer.tryResolveField(c3).isPresent());
        assertFalse(outer.tryResolveField(c4).isPresent());

        assertTrue(inner.tryResolveField(c1).isPresent());
        assertEquals(outerColumn1, inner.tryResolveField(c1).get().getField());
        assertFalse(inner.tryResolveField(c1).get().isLocal());
        assertEquals(0, inner.tryResolveField(c1).get().getHierarchyFieldIndex());
        assertEquals(0, inner.tryResolveField(c1).get().getRelationFieldIndex());
        assertTrue(inner.tryResolveField(c2).isPresent());
        assertEquals(innerColumn2, inner.tryResolveField(c2).get().getField());
        assertTrue(inner.tryResolveField(c2).get().isLocal());
        assertEquals(0, inner.tryResolveField(c2).get().getHierarchyFieldIndex());
        assertTrue(inner.tryResolveField(c2).isPresent());
        assertEquals(innerColumn3, inner.tryResolveField(c3).get().getField());
        assertTrue(inner.tryResolveField(c3).get().isLocal());
        assertEquals(1, inner.tryResolveField(c3).get().getHierarchyFieldIndex());
        assertFalse(inner.tryResolveField(c4).isPresent());

        assertEquals(Optional.of(outer), inner.getOuterQueryParent());
    }

    private static Expression name(String first, String... parts)
    {
        return DereferenceExpression.from(QualifiedName.of(first, parts));
    }
}
