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

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class LanguageCharacteristic
        extends RoutineCharacteristic
{
    private final Identifier language;

    public LanguageCharacteristic(Identifier language)
    {
        this(Optional.empty(), language);
    }

    public LanguageCharacteristic(NodeLocation location, Identifier language)
    {
        this(Optional.of(location), language);
    }

    private LanguageCharacteristic(Optional<NodeLocation> location, Identifier language)
    {
        super(location);
        this.language = requireNonNull(language, "comment is null");
    }

    public Identifier getLanguage()
    {
        return language;
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitLanguageCharacteristic(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object obj)
    {
        return (obj instanceof LanguageCharacteristic other) &&
                language.equals(other.language);
    }

    @Override
    public int hashCode()
    {
        return language.hashCode();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("language", language)
                .toString();
    }
}
