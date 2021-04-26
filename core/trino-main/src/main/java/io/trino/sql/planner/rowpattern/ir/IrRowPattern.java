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
package io.trino.sql.planner.rowpattern.ir;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = IrAnchor.class, name = "anchor"),
        @JsonSubTypes.Type(value = IrEmpty.class, name = "empty"),
        @JsonSubTypes.Type(value = IrExclusion.class, name = "exclusion"),
        @JsonSubTypes.Type(value = IrLabel.class, name = "label"),
        @JsonSubTypes.Type(value = IrAlternation.class, name = "alternation"),
        @JsonSubTypes.Type(value = IrConcatenation.class, name = "concatenation"),
        @JsonSubTypes.Type(value = IrPermutation.class, name = "permutation"),
        @JsonSubTypes.Type(value = IrQuantified.class, name = "quantified"),
})
public abstract class IrRowPattern
{
    protected <R, C> R accept(IrRowPatternVisitor<R, C> visitor, C context)
    {
        return visitor.visitIrRowPattern(this, context);
    }
}
