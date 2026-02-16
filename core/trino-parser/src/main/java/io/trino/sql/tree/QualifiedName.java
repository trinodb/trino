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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.isEmpty;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class QualifiedName
{
    private final List<Identifier> originalParts;
    private final List<String> priorParts;
    private final Optional<QualifiedName> prefix;

    private List<String> parts;
    private Optional<Resolver> resolver = Optional.empty();

    // Following fields are not part of the equals/hashCode methods as
    // they are exist solely to speed-up certain method calls.
    private String name;
    private String suffix;

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
        requireNonNull(originalParts, "originalParts is null");
        checkArgument(!isEmpty(originalParts), "originalParts is empty");
        return new QualifiedName(ImmutableList.copyOf(originalParts));
    }

    public QualifiedName(List<Identifier> originalParts)
    {
        this.originalParts = originalParts;
        // Iteration instead of stream for performance reasons
        ImmutableList.Builder<String> partsBuilder = ImmutableList.builderWithExpectedSize(originalParts.size());
        for (Identifier identifier : originalParts) {
            if (resolver.isEmpty() && identifier.isResolved()) {
                resolver = identifier.getResolver();
            }
            else if (resolver.isPresent() && !identifier.isResolved()) {
                identifier.setResolver(resolver);
            }
            partsBuilder.add(identifier.getCanonicalizedValue());
        }

        if (resolver.isPresent() && needResolver()) {
            System.out.println("QualifiedName::new needResolver *********************************************");
        }

        if (originalParts.size() == 1) {
            this.prefix = Optional.empty();
        }
        else {
            List<Identifier> subList = originalParts.subList(0, originalParts.size() - 1);
            this.prefix = Optional.of(new QualifiedName(subList));
        }
        setQualifiedName(partsBuilder.build());
        this.priorParts = isResolved() ? getPriorParts(originalParts) : ImmutableList.copyOf(parts);
    }

    private static List<String> getPriorParts(List<Identifier> originalParts)
    {
        return originalParts.stream()
                .map(id -> id.isCatalog() ? id.getValue().toLowerCase(ENGLISH) : id.getValue())
                .toList();
    }

    public QualifiedName resolveIdentifiers(Resolver resolver)
    {
        return resolveIdentifiers(Optional.of(resolver));
    }

    public QualifiedName resolveIdentifiers(Optional<Resolver> resolver)
    {
        prefix.ifPresent(qualifiedName -> qualifiedName.resolveIdentifiers(resolver));

        ImmutableList.Builder<String> partsBuilder = ImmutableList.builderWithExpectedSize(originalParts.size());
        for (Identifier identifier : originalParts) {
            String value;
            if (identifier.isResolved()) {
                value = identifier.getCanonicalizedValue();
            }
            else {
                value = identifier.setResolver(resolver).getCanonicalizedValue();
            }
            partsBuilder.add(value);
        }
        this.resolver = resolver;
        setQualifiedName(partsBuilder.build());
        return this;
    }

    public boolean isResolved()
    {
        return resolver.isPresent();
    }

    public boolean needResolver()
    {
        return originalParts.stream().anyMatch(id -> id.getResolver().isEmpty());
    }

    public Optional<Resolver> getResolver()
    {
        return resolver;
    }

    private void setQualifiedName(List<String> parts)
    {
        this.parts = parts;
        this.name = String.join(".", parts);
        this.suffix = parts.getLast();
    }

    public List<String> getParts()
    {
        return parts;
    }

    public List<String> getParts(Optional<Resolver> resolver)
    {
        return this.resolver.isPresent() && (resolver.isEmpty() || !this.resolver.get().equals(resolver.get())) ? priorParts : parts;
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
        return getParts(suffix.getResolver()).subList(start, parts.size()).equals(suffix.getParts(resolver));
    }

    public String getSuffix()
    {
        return isResolved() ? this.suffix : this.suffix.toLowerCase(ENGLISH);
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
