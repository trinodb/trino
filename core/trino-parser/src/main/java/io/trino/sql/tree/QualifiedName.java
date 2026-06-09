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
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class QualifiedName
{
    private final List<Identifier> originalParts;
    private final Optional<QualifiedName> prefix;
    private final List<String> parts;
    private final boolean isResolved;

    // Following fields are not part of the equals/hashCode methods as
    // they are exist solely to speed-up certain method calls.
    private final String name;
    private final String suffix;

    public static QualifiedName of(Optional<Function<Identifier, String>> canonicalizer, QualifiedName name)
    {
        requireNonNull(canonicalizer, "canonicalizer is null");
        requireNonNull(name, "name is null");
        return canonicalizer.map(function -> of(function, name)).orElse(name);
    }

    public static QualifiedName of(Optional<Function<Identifier, String>> canonicalizer, Identifier identifier)
    {
        requireNonNull(canonicalizer, "canonicalizer is null");
        requireNonNull(identifier, "identifier is null");
        return of(canonicalizer, identifier);
    }

    public static QualifiedName of(Function<Identifier, String> canonicalizer, QualifiedName name)
    {
        return of(canonicalizer, name, name.originalParts.size() > 2);
    }

    public static QualifiedName of(Function<Identifier, String> canonicalizer, QualifiedName name, boolean withCatalog)
    {
        requireNonNull(canonicalizer, "canonicalizer is null");
        requireNonNull(name, "name is null");
        return new QualifiedName(Optional.of(canonicalizer), name.originalParts, withCatalog);
    }

    public static QualifiedName of(Function<Identifier, String> canonicalizer, Identifier identifier)
    {
        requireNonNull(canonicalizer, "canonicalizer is null");
        requireNonNull(identifier, "identifier is null");
        return new QualifiedName(Optional.of(canonicalizer), ImmutableList.of(identifier), false);
    }

    public static QualifiedName of(Optional<Function<Identifier, String>> canonicalizer, List<Identifier> identifiers)
    {
        requireNonNull(canonicalizer, "canonicalizer is null");
        requireNonNull(identifiers, "identifiers is null");
        return of(canonicalizer, identifiers);
    }

    public static QualifiedName of(Function<Identifier, String> canonicalizer, List<Identifier> identifiers)
    {
        return of(canonicalizer, identifiers, identifiers.size() > 2);
    }

    public static QualifiedName of(Function<Identifier, String> canonicalizer, List<Identifier> identifiers, boolean withCatalog)
    {
        requireNonNull(canonicalizer, "canonicalizer is null");
        requireNonNull(identifiers, "identifiers is null");
        return new QualifiedName(Optional.of(canonicalizer), identifiers, withCatalog);
    }

    public static QualifiedName of(String first, String... rest)
    {
        requireNonNull(first, "first is null");
        return of(Lists.asList(first, rest).stream().map(Identifier::new).collect(toImmutableList()));
    }

    public static QualifiedName ofDelimited(String name)
    {
        requireNonNull(name, "name is null");
        return of(ImmutableList.of(new Identifier(name, true)));
    }

    public static QualifiedName ofDelimited(String first, String... rest)
    {
        return of(true, first, rest);
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
        return new QualifiedName(Optional.empty(), ImmutableList.copyOf(originalParts), withCatalog);
    }

    public QualifiedName(Optional<Function<Identifier, String>> canonicalizer, List<Identifier> originalParts, boolean withCatalog)
    {
        int size = originalParts.size();
        this.originalParts = originalParts;
        this.isResolved = canonicalizer.isPresent();

        if (size == 1) {
            this.prefix = Optional.empty();
            this.suffix = withCatalog || canonicalizer.isEmpty() ? originalParts.getFirst().getValue() : canonicalizer.get().apply(originalParts.getFirst());
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
                partsBuilder.add(canonicalizer.map(function -> function.apply(identifier)).orElse(identifier.getValue()));
            }
            this.parts = partsBuilder.build();
            this.name = String.join(".", parts);
            this.suffix = parts.getLast();
        }
    }

    public boolean isResolved()
    {
        return isResolved;
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

    public boolean matchesSuffix(String name, boolean resolved)
    {
        // FIXME: If we want to be able to differentiate between fields that only differ in their case,
        //        then we must resolve the fields taking case into account if resolved (ie: coming from connector)?
        return resolved ? suffix.equals(name) : suffix.equalsIgnoreCase(name);
    }

    public boolean hasSuffix(QualifiedName suffix)
    {
        return hasSuffix(suffix, true);
    }

    public boolean hasSuffix(QualifiedName suffix, boolean resolved)
    {
        if (parts.size() < suffix.getParts().size()) {
            return false;
        }

        int size = parts.size();
        int start =  size - suffix.getParts().size();
        List<String> subParts = getParts().subList(start, size);
        if (resolved) {
            return subParts.equals(suffix.getParts());
        }
        return subParts.stream().map(value -> value.toLowerCase(ENGLISH)).toList()
                .equals(suffix.getParts().stream().map(value -> value.toLowerCase(ENGLISH)).toList());
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
