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
    private List<String> parts;

    // Following fields are not part of the equals/hashCode methods as
    // they are exist solely to speed-up certain method calls.
    private String name;
    private final Optional<QualifiedName> prefix;
    private String suffix;
    private int canonicalizeCount;

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
            partsBuilder.add(identifier.getCanonicalizedValue());
        }

        if (originalParts.size() == 1) {
            this.prefix = Optional.empty();
        }
        else {
            List<Identifier> subList = originalParts.subList(0, originalParts.size() - 1);
            this.prefix = Optional.of(new QualifiedName(subList));
        }
        setQualifiedName(partsBuilder.build());
    }

    public QualifiedName canonicalizeCatalog(Function<Identifier, String> canonicalizer)
    {
        canonicalize(canonicalizer, true);
        canonicalizeCount++;
        return this;
    }

    public void canonicalizeSchema(Function<Identifier, String> canonicalizer)
    {
        canonicalize(canonicalizer, originalParts.size() > 1);
        canonicalizeCount++;
    }

    public void canonicalizeTable(Function<Identifier, String> canonicalizer)
    {
        canonicalize(canonicalizer, originalParts.size() > 2);
        canonicalizeCount++;
    }

    // This method is called from:
    // - AddColumnTask.execute()
    // - DropColumnTask.execute()
    public QualifiedName canonicalizeColumn(Function<Identifier, String> columnCanonicalizer)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builderWithExpectedSize(originalParts.size());
        for (Identifier identifier : originalParts) {
            if (identifier == originalParts.getFirst()) {
                builder.add(identifier.setCanonicalizer(columnCanonicalizer).getCanonicalizedValue());
            }
            else {
                builder.add(identifier.getCanonicalizedValue());
            }
        }
        setQualifiedName(builder.build());
        canonicalizeCount++;
        return this;
    }

    public QualifiedName canonicalizeColumn(Function<Identifier, String> canonicalizer, Function<Identifier, String> columnCanonicalizer)
    {
        prefix.ifPresent(qualifiedName -> qualifiedName.canonicalize(canonicalizer, originalParts.size() > 3));
        ImmutableList.Builder<String> builder = ImmutableList.builderWithExpectedSize(originalParts.size());
        for (Identifier identifier : originalParts) {
            if (identifier == originalParts.getLast()) {
                builder.add(identifier.setCanonicalizer(columnCanonicalizer).getCanonicalizedValue());
            }
            else {
                builder.add(identifier.setCanonicalizer(canonicalizer).getCanonicalizedValue());
            }
        }
        setQualifiedName(builder.build());
        canonicalizeCount++;
        return this;
    }

    private void canonicalize(Function<Identifier, String> canonicalizer, boolean withCatalog)
    {
        if (prefix.isPresent()) {
            prefix.get().canonicalize(canonicalizer, withCatalog);
        }
        else if (withCatalog) {
            return;
        }
        ImmutableList.Builder<String> builder = ImmutableList.builderWithExpectedSize(originalParts.size());
        for (Identifier identifier : originalParts) {
            if (!withCatalog) {
                builder.add(identifier.setCanonicalizer(canonicalizer).getCanonicalizedValue());
                continue;
            }
            builder.add(identifier.getCanonicalizedValue());
            withCatalog = false;
        }
        setQualifiedName(builder.build());
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

    public List<Identifier> getOriginalParts()
    {
        return originalParts;
    }

    public int getCanonicalizeCount()
    {
        return canonicalizeCount;
    }

    /**
     * For an identifier of the form "a.b.c.d", returns "a.b.c"
     * For an identifier of the form "a", returns absent
     */
    public Optional<QualifiedName> getPrefix()
    {
        return this.prefix;
    }

    public boolean hasSuffix(QualifiedName suffix)
    {
        if (parts.size() < suffix.getParts().size()) {
            return false;
        }

        int start = parts.size() - suffix.getParts().size();
        return parts.subList(start, parts.size()).equals(suffix.getParts());
    }

    public String getSuffix()
    {
        return this.suffix;
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
