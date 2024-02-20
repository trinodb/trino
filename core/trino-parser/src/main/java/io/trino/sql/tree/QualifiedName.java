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
    private final List<String> parts;

    // Following fields are not part of the equals/hashCode methods as
    // they are exist solely to speed-up certain method calls.
    private final String name;
    private final Optional<QualifiedName> prefix;
    private final String suffix;

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

    public static QualifiedName of(Iterable<Identifier> originalParts)
    {
        requireNonNull(originalParts, "originalParts is null");
        checkArgument(!isEmpty(originalParts), "originalParts is empty");

        return new QualifiedName(ImmutableList.copyOf(originalParts));
    }

    private QualifiedName(List<Identifier> originalParts)
    {
        this.originalParts = originalParts;
        // Iteration instead of stream for performance reasons
        ImmutableList.Builder<String> partsBuilder = ImmutableList.builderWithExpectedSize(originalParts.size());
        for (Identifier identifier : originalParts) {
            partsBuilder.add(mapIdentifier(identifier));
        }
        this.parts = partsBuilder.build();
        this.name = String.join(".", parts);

        if (originalParts.size() == 1) {
            this.prefix = Optional.empty();
            this.suffix = mapIdentifier(originalParts.get(0));
        }
        else {
            List<Identifier> subList = originalParts.subList(0, originalParts.size() - 1);
            this.prefix = Optional.of(new QualifiedName(subList));
            this.suffix = mapIdentifier(originalParts.get(originalParts.size() - 1));
        }
    }

    private static String mapIdentifier(Identifier identifier)
    {
        return identifier.getValue().toLowerCase(ENGLISH);
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
