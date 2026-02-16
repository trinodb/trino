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
package io.trino.sql.tree;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.isEmpty;
import static java.util.Objects.requireNonNull;

public class QualifiedName
{
    private final List<Identifier> originalParts;
    private final Optional<QualifiedName> prefix;
    private final List<String> parts;

    // Following fields are not part of the equals/hashCode methods as
    // they are exist solely to speed-up certain method calls.
    private final String name;
    private final String suffix;

    public static QualifiedName of(Function<Identifier, String> canonicalizer, QualifiedName qualifiedName)
    {
        requireNonNull(canonicalizer, "canonicalizer is null");
        requireNonNull(qualifiedName, "qualifiedName is null");
        return new QualifiedName(canonicalizer, qualifiedName.originalParts, qualifiedName.originalParts.size() > 2);
    }

    public static QualifiedName of(Function<Identifier, String> canonicalizer, Identifier identifier)
    {
        requireNonNull(canonicalizer, "canonicalizer is null");
        requireNonNull(identifier, "identifier is null");
        return new QualifiedName(canonicalizer, ImmutableList.of(identifier), false);
    }

    public static QualifiedName of(Function<Identifier, String> canonicalizer, List<Identifier> identifiers)
    {
        requireNonNull(canonicalizer, "canonicalizer is null");
        requireNonNull(identifiers, "identifiers is null");
        return new QualifiedName(canonicalizer, identifiers, identifiers.size() > 2);
    }

    public static QualifiedName of(String first, String... rest)
    {
        requireNonNull(first, "first is null");
        return of(Lists.asList(first, rest).stream().map(Identifier::new).collect(toImmutableList()));
    }

    public static QualifiedName of(String name)
    {
        requireNonNull(name, "name is null");
        return of(ImmutableList.of(new Identifier(name)));
    }

    public static QualifiedName of(boolean delimited, String first, String... rest)
    {
        requireNonNull(first, "first is null");
        return of(Lists.asList(first, rest).stream().map(name -> new Identifier(name, delimited)).collect(toImmutableList()));
    }

    public static QualifiedName of(String name, boolean delimited)
    {
        requireNonNull(name, "name is null");
        return of(ImmutableList.of(new Identifier(name, delimited)));
    }

    public static QualifiedName of(Identifier identifier)
    {
        requireNonNull(identifier, "identifier is null");
        return of(ImmutableList.of(identifier));
    }

    public static QualifiedName of(Iterable<Identifier> originalParts)
    {
        requireNonNull(originalParts, "originalParts is null");
        return of(ImmutableList.copyOf(originalParts));
    }

    public static QualifiedName of(List<Identifier> originalParts)
    {
        return of(originalParts, false);
    }

    public static QualifiedName of(List<Identifier> originalParts, boolean withCatalog)
    {
        requireNonNull(originalParts, "originalParts is null");
        checkArgument(!isEmpty(originalParts), "originalParts is empty");
        return new QualifiedName(Identifier::getValue, ImmutableList.copyOf(originalParts), withCatalog);
    }

    public QualifiedName(Function<Identifier, String> canonicalizer, List<Identifier> originalParts, boolean withCatalog)
    {
        int size = originalParts.size();
        this.originalParts = originalParts;

        if (size == 1) {
            this.prefix = Optional.empty();
            this.suffix = withCatalog ? originalParts.getFirst().getValue() : canonicalizer.apply(originalParts.getFirst());
            this.parts = ImmutableList.of(suffix);
            this.name = suffix;
        }
        else {
            // Iteration instead of stream for performance reasons
            List<Identifier> subList = originalParts.subList(0, size - 1);
            this.prefix = Optional.of(new QualifiedName(canonicalizer, subList, withCatalog));
            ImmutableList.Builder<String> partsBuilder = ImmutableList.builderWithExpectedSize(size);
            for (Identifier identifier : originalParts) {
                if (withCatalog) {
                    partsBuilder.add(identifier.getValue());
                    withCatalog = false;
                    continue;
                }
                partsBuilder.add(canonicalizer.apply(identifier));
            }
            this.parts = partsBuilder.build();
            this.name = String.join(".", parts);
            this.suffix = parts.getLast();
        }
    }

    public List<String> getParts()
    {
        return parts;
    }

    public List<Identifier> getOriginalParts()
    {
        return originalParts;
    }

    /**
     * For an identifier of the form "a.b.c.d", returns "a.b.c"
     * For an identifier of the form "a", returns absent
     */
    public Optional<QualifiedName> getPrefix()
    {
        return this.prefix;
    }

    public boolean matchesSuffix(String name)
    {
        // FIXME: If we want to be able to differentiate between fields that only differ in their case,
        //        then we must resolve the fields taking case and resolver into account?
        //return resolver.isPresent() ? suffix.equalsIgnoreCase(name) : suffix.equals(name);
        return suffix.equals(name);
    }

    public boolean hasSuffix(QualifiedName suffix)
    {
        if (parts.size() < suffix.getParts().size()) {
            return false;
        }

        int start = parts.size() - suffix.getParts().size();
        return getParts().subList(start, parts.size()).equals(suffix.getParts());
    }

    public String getSuffix()
    {
        return this.suffix;
    }

    public boolean isDelimited()
    {
        return originalParts.stream().allMatch(Identifier::isDelimited);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return parts.equals(((QualifiedName) o).parts);
    }

    @Override
    public int hashCode()
    {
        return parts.hashCode();
    }

    @Override
    public String toString()
    {
        return name;
    }
}
