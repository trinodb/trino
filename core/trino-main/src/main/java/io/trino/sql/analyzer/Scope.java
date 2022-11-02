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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.type.RowType;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.WithQuery;

import javax.annotation.concurrent.Immutable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.StandardErrorCode.AMBIGUOUS_NAME;
import static io.trino.sql.analyzer.ExpressionTreeUtils.asQualifiedName;
import static io.trino.sql.analyzer.Scope.BasisType.FIELD;
import static io.trino.sql.analyzer.Scope.BasisType.TABLE;
import static io.trino.sql.analyzer.SemanticExceptions.ambiguousAttributeException;
import static io.trino.sql.analyzer.SemanticExceptions.missingAttributeException;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

@Immutable
public class Scope
{
    private final Optional<Scope> parent;
    private final boolean queryBoundary;
    private final RelationId relationId;
    private final RelationType relation;
    private final Map<String, WithQuery> namedQueries;

    public static Scope create()
    {
        return builder().build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    private Scope(
            Optional<Scope> parent,
            boolean queryBoundary,
            RelationId relationId,
            RelationType relation,
            Map<String, WithQuery> namedQueries)
    {
        this.parent = requireNonNull(parent, "parent is null");
        this.relationId = requireNonNull(relationId, "relationId is null");
        this.queryBoundary = queryBoundary;
        this.relation = requireNonNull(relation, "relation is null");
        this.namedQueries = ImmutableMap.copyOf(requireNonNull(namedQueries, "namedQueries is null"));
    }

    public Scope withRelationType(RelationType relationType)
    {
        return new Scope(parent, queryBoundary, relationId, relationType, namedQueries);
    }

    public Scope getQueryBoundaryScope()
    {
        Scope scope = this;
        Optional<Scope> parent = scope.getLocalParent();
        while (parent.isPresent()) {
            scope = parent.get();
            parent = scope.getLocalParent();
        }
        return scope;
    }

    public Optional<Scope> getOuterQueryParent()
    {
        Scope scope = this;
        while (scope.parent.isPresent()) {
            if (scope.queryBoundary) {
                return scope.parent;
            }
            scope = scope.parent.get();
        }

        return Optional.empty();
    }

    public boolean hasOuterParent(Scope parent)
    {
        return getOuterQueryParent()
                .map(scope -> scope.isLocalScope(parent) || scope.hasOuterParent(parent))
                .orElse(false);
    }

    public Optional<Scope> getLocalParent()
    {
        if (!queryBoundary) {
            return parent;
        }

        return Optional.empty();
    }

    public int getLocalScopeFieldCount()
    {
        int parent = getLocalParent()
                .map(Scope::getLocalScopeFieldCount)
                .orElse(0);

        return parent + getRelationType().getAllFieldCount();
    }

    public RelationId getRelationId()
    {
        return relationId;
    }

    public RelationType getRelationType()
    {
        return relation;
    }

    /**
     * Starting from this, finds the closest scope which satisfies given predicate,
     * within the query boundary.
     */
    private Optional<Scope> findLocally(Predicate<Scope> match)
    {
        Scope scope = this;

        do {
            if (match.test(scope)) {
                return Optional.of(scope);
            }

            Optional<Scope> parent = scope.getLocalParent();
            if (parent.isEmpty()) {
                break;
            }

            scope = parent.get();
        }
        while (true);

        return Optional.empty();
    }

    /**
     * This method analyzes asterisked identifier chain in 'all fields reference', according to SQL standard.
     * The following rules are applied:
     * 1. if the identifier chain's length is lower than 4, it is attempted to resolve it as a table reference,
     * either in the local scope or in outer scope.
     * 2. if the identifier chain's length is greater than 1, it is attempted to match the chain's prefix of length 2
     * to a field of type Row, either in the local scope or in outer scope.
     * Any available match in the innermost scope is picked and the 'identifier chain basis' is defined.
     * If more than one basis is defined, it results in a failure (ambiguous identifier chain).
     * <p>
     * NOTE: only ambiguity between table reference and field reference is currently detected.
     * If two or more tables in the same scope match the identifier chain, they are indistinguishable in current Scope implementation,
     * and such query results in returning fields from all of those tables.
     * If two or more fields in the same scope match the identifier chain, such query will fail later during analysis.
     * <p>
     * NOTE: Although references to outer scope tables are found, referencing all fields from outer scope tables is not yet supported.
     */
    public Optional<AsteriskedIdentifierChainBasis> resolveAsteriskedIdentifierChainBasis(QualifiedName identifierChain, AllColumns selectItem)
    {
        int length = identifierChain.getParts().size();

        Optional<Scope> scopeForTableReference = Optional.empty();
        Optional<Scope> scopeForFieldReference = Optional.empty();

        if (length <= 3) {
            scopeForTableReference = findLocally(scope -> scope.getRelationType()
                    .getAllFields().stream()
                    .anyMatch(field -> field.matchesPrefix(Optional.of(identifierChain))));
        }

        if (length >= 2) {
            scopeForFieldReference = findLocally(scope -> scope.getRelationType()
                    .getAllFields().stream()
                    .anyMatch(field -> field.matchesPrefix(Optional.of(QualifiedName.of(identifierChain.getParts().get(0))))
                            && field.getName().isPresent()
                            && field.getName().get().equals(identifierChain.getParts().get(1))
                            && field.getType() instanceof RowType));
        }

        if (scopeForTableReference.isPresent() && scopeForFieldReference.isPresent()) {
            throw semanticException(AMBIGUOUS_NAME, selectItem, "Reference '%s' is ambiguous", identifierChain);
        }

        if (scopeForTableReference.isPresent()) {
            return Optional.of(new AsteriskedIdentifierChainBasis(TABLE, scopeForTableReference, Optional.of(scopeForTableReference.get().getRelationType())));
        }

        if (scopeForFieldReference.isPresent()) {
            return Optional.of(new AsteriskedIdentifierChainBasis(FIELD, Optional.empty(), Optional.empty()));
        }

        return getOuterQueryParent()
                .flatMap(parent -> parent.resolveAsteriskedIdentifierChainBasis(identifierChain, selectItem));
    }

    // check if other is within the query boundary starting from this
    public boolean isLocalScope(Scope other)
    {
        return findLocally(scope -> scope.getRelationId().equals(other.getRelationId())).isPresent();
    }

    public ResolvedField resolveField(Expression expression, QualifiedName name)
    {
        return tryResolveField(expression, name).orElseThrow(() -> missingAttributeException(expression, name));
    }

    public Optional<ResolvedField> tryResolveField(Expression expression)
    {
        QualifiedName qualifiedName = asQualifiedName(expression);
        if (qualifiedName != null) {
            return tryResolveField(expression, qualifiedName);
        }
        return Optional.empty();
    }

    public Optional<ResolvedField> tryResolveField(Expression node, QualifiedName name)
    {
        return resolveField(node, name, true);
    }

    private Optional<ResolvedField> resolveField(Expression node, QualifiedName name, boolean local)
    {
        List<Field> matches = relation.resolveFields(name);
        if (matches.size() > 1) {
            throw ambiguousAttributeException(node, name);
        }
        if (matches.size() == 1) {
            int parentFieldCount = getLocalParent()
                    .map(Scope::getLocalScopeFieldCount)
                    .orElse(0);

            Field field = getOnlyElement(matches);
            return Optional.of(asResolvedField(field, parentFieldCount, local));
        }
        if (isColumnReference(name, relation)) {
            return Optional.empty();
        }
        if (parent.isPresent()) {
            if (queryBoundary) {
                return parent.get().resolveField(node, name, false);
            }
            return parent.get().resolveField(node, name, local);
        }
        return Optional.empty();
    }

    public ResolvedField getField(int index)
    {
        int parentFieldCount = getLocalParent()
                .map(Scope::getLocalScopeFieldCount)
                .orElse(0);

        return asResolvedField(relation.getFieldByIndex(index), parentFieldCount, true);
    }

    private ResolvedField asResolvedField(Field field, int fieldIndexOffset, boolean local)
    {
        int relationFieldIndex = relation.indexOf(field);
        int hierarchyFieldIndex = relation.indexOf(field) + fieldIndexOffset;
        return new ResolvedField(this, field, hierarchyFieldIndex, relationFieldIndex, local);
    }

    public boolean isColumnReference(QualifiedName name)
    {
        Scope current = this;
        while (current != null) {
            if (isColumnReference(name, current.relation)) {
                return true;
            }
            current = current.parent.orElse(null);
        }

        return false;
    }

    private static boolean isColumnReference(QualifiedName name, RelationType relation)
    {
        while (name.getPrefix().isPresent()) {
            name = name.getPrefix().get();
            if (!relation.resolveFields(name).isEmpty()) {
                return true;
            }
        }
        return false;
    }

    public Optional<WithQuery> getNamedQuery(String name)
    {
        if (namedQueries.containsKey(name)) {
            return Optional.of(namedQueries.get(name));
        }

        if (parent.isPresent()) {
            return parent.get().getNamedQuery(name);
        }

        return Optional.empty();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(relationId)
                .toString();
    }

    public static final class Builder
    {
        private RelationId relationId = RelationId.anonymous();
        private RelationType relationType = new RelationType();
        private final Map<String, WithQuery> namedQueries = new HashMap<>();
        private Optional<Scope> parent = Optional.empty();
        private boolean queryBoundary;

        public Builder like(Scope other)
        {
            relationId = other.relationId;
            relationType = other.relation;
            namedQueries.putAll(other.namedQueries);
            parent = other.parent;
            queryBoundary = other.queryBoundary;
            return this;
        }

        public Builder withRelationType(RelationId relationId, RelationType relationType)
        {
            this.relationId = requireNonNull(relationId, "relationId is null");
            this.relationType = requireNonNull(relationType, "relationType is null");
            return this;
        }

        public Builder withParent(Scope parent)
        {
            checkArgument(this.parent.isEmpty(), "parent is already set");
            this.parent = Optional.of(parent);
            return this;
        }

        public Builder withOuterQueryParent(Scope parent)
        {
            checkArgument(this.parent.isEmpty(), "parent is already set");
            this.parent = Optional.of(parent);
            this.queryBoundary = true;
            return this;
        }

        public Builder withNamedQuery(String name, WithQuery withQuery)
        {
            checkArgument(!containsNamedQuery(name), "Query '%s' is already added", name);
            namedQueries.put(name, withQuery);
            return this;
        }

        public boolean containsNamedQuery(String name)
        {
            return namedQueries.containsKey(name);
        }

        public Scope build()
        {
            return new Scope(parent, queryBoundary, relationId, relationType, namedQueries);
        }
    }

    public static class AsteriskedIdentifierChainBasis
    {
        private final BasisType basisType;
        private final Optional<Scope> scope;
        private final Optional<RelationType> relationType;

        public AsteriskedIdentifierChainBasis(BasisType basisType, Optional<Scope> scope, Optional<RelationType> relationType)
        {
            this.basisType = requireNonNull(basisType, "basisType is null");
            this.scope = requireNonNull(scope, "scope is null");
            this.relationType = requireNonNull(relationType, "relationType is null");
            checkArgument(basisType == FIELD || scope.isPresent(), "missing scope");
            checkArgument(basisType == FIELD || relationType.isPresent(), "missing relationType");
        }

        public BasisType getBasisType()
        {
            return basisType;
        }

        public Optional<Scope> getScope()
        {
            return scope;
        }

        public Optional<RelationType> getRelationType()
        {
            return relationType;
        }
    }

    enum BasisType
    {
        TABLE,
        FIELD
    }
}
