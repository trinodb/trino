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
import io.trino.sql.tree.JsonPathParameter.JsonFormat;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class JsonObject
        extends Expression
{
    private final List<JsonObjectMember> members;
    private final boolean nullOnNull;
    private final boolean uniqueKeys;
    private final Optional<DataType> returnedType;
    private final Optional<JsonFormat> outputFormat;

    public JsonObject(Optional<NodeLocation> location, List<JsonObjectMember> members, boolean nullOnNull, boolean uniqueKeys, Optional<DataType> returnedType, Optional<JsonFormat> outputFormat)
    {
        super(location);

        requireNonNull(members, "members is null");
        requireNonNull(returnedType, "returnedType is null");
        requireNonNull(outputFormat, "outputFormat is null");

        this.members = ImmutableList.copyOf(members);
        this.nullOnNull = nullOnNull;
        this.uniqueKeys = uniqueKeys;
        this.returnedType = returnedType;
        this.outputFormat = outputFormat;
    }

    public List<JsonObjectMember> getMembers()
    {
        return members;
    }

    public boolean isNullOnNull()
    {
        return nullOnNull;
    }

    public boolean isUniqueKeys()
    {
        return uniqueKeys;
    }

    public Optional<DataType> getReturnedType()
    {
        return returnedType;
    }

    public Optional<JsonFormat> getOutputFormat()
    {
        return outputFormat;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitJsonObject(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return members;
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

        JsonObject that = (JsonObject) o;
        return Objects.equals(members, that.members) &&
                nullOnNull == that.nullOnNull &&
                uniqueKeys == that.uniqueKeys &&
                Objects.equals(returnedType, that.returnedType) &&
                Objects.equals(outputFormat, that.outputFormat);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(members, nullOnNull, uniqueKeys, returnedType, outputFormat);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        JsonObject that = (JsonObject) other;
        return nullOnNull == that.nullOnNull &&
                uniqueKeys == that.uniqueKeys &&
                Objects.equals(returnedType, that.returnedType) &&
                Objects.equals(outputFormat, that.outputFormat);
    }
}
