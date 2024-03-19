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
package io.trino.sql.ir;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

@JsonSerialize
public record Row(List<Expression> items)
        implements Expression
{
    public Row
    {
        requireNonNull(items, "items is null");
        items = ImmutableList.copyOf(items);
    }

    @Deprecated
    public List<Expression> getItems()
    {
        return items;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitRow(this, context);
    }

    @Override
    public List<? extends Expression> getChildren()
    {
        return items;
    }
}
