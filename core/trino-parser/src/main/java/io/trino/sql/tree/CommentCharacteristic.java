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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class CommentCharacteristic
        extends RoutineCharacteristic
{
    private final String comment;

    public CommentCharacteristic(NodeLocation location, String comment)
    {
        super(location);
        this.comment = requireNonNull(comment, "comment is null");
    }

    public String getComment()
    {
        return comment;
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCommentCharacteristic(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object obj)
    {
        return (obj instanceof CommentCharacteristic other) &&
                comment.equals(other.comment);
    }

    @Override
    public int hashCode()
    {
        return comment.hashCode();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("comment", comment)
                .toString();
    }
}
