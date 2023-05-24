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
package io.trino.json.ir;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.trino.spi.type.Type;

import java.util.Optional;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = IrAbsMethod.class, name = "abs"),
        @JsonSubTypes.Type(value = IrArithmeticBinary.class, name = "binary"),
        @JsonSubTypes.Type(value = IrArithmeticUnary.class, name = "unary"),
        @JsonSubTypes.Type(value = IrArrayAccessor.class, name = "arrayaccessor"),
        @JsonSubTypes.Type(value = IrCeilingMethod.class, name = "ceiling"),
        @JsonSubTypes.Type(value = IrComparisonPredicate.class, name = "comparison"),
        @JsonSubTypes.Type(value = IrConjunctionPredicate.class, name = "conjunction"),
        @JsonSubTypes.Type(value = IrConstantJsonSequence.class, name = "jsonsequence"),
        @JsonSubTypes.Type(value = IrContextVariable.class, name = "contextvariable"),
        @JsonSubTypes.Type(value = IrDatetimeMethod.class, name = "datetime"),
        @JsonSubTypes.Type(value = IrDescendantMemberAccessor.class, name = "descendantmemberaccessor"),
        @JsonSubTypes.Type(value = IrDisjunctionPredicate.class, name = "disjunction"),
        @JsonSubTypes.Type(value = IrDoubleMethod.class, name = "double"),
        @JsonSubTypes.Type(value = IrExistsPredicate.class, name = "exists"),
        @JsonSubTypes.Type(value = IrFilter.class, name = "filter"),
        @JsonSubTypes.Type(value = IrFloorMethod.class, name = "floor"),
        @JsonSubTypes.Type(value = IrIsUnknownPredicate.class, name = "isunknown"),
        @JsonSubTypes.Type(value = IrJsonNull.class, name = "jsonnull"),
        @JsonSubTypes.Type(value = IrKeyValueMethod.class, name = "keyvalue"),
        @JsonSubTypes.Type(value = IrLastIndexVariable.class, name = "last"),
        @JsonSubTypes.Type(value = IrLiteral.class, name = "literal"),
        @JsonSubTypes.Type(value = IrMemberAccessor.class, name = "memberaccessor"),
        @JsonSubTypes.Type(value = IrNamedJsonVariable.class, name = "namedjsonvariable"),
        @JsonSubTypes.Type(value = IrNamedValueVariable.class, name = "namedvaluevariable"),
        @JsonSubTypes.Type(value = IrNegationPredicate.class, name = "negation"),
        @JsonSubTypes.Type(value = IrPredicateCurrentItemVariable.class, name = "currentitem"),
        @JsonSubTypes.Type(value = IrSizeMethod.class, name = "size"),
        @JsonSubTypes.Type(value = IrStartsWithPredicate.class, name = "startswith"),
        @JsonSubTypes.Type(value = IrTypeMethod.class, name = "type"),
})

public sealed interface IrPathNode
        permits
        IrAbsMethod,
        IrArithmeticBinary,
        IrArithmeticUnary,
        IrArrayAccessor,
        IrCeilingMethod,
        IrConstantJsonSequence,
        IrContextVariable,
        IrDatetimeMethod,
        IrDescendantMemberAccessor,
        IrDoubleMethod,
        IrFilter,
        IrFloorMethod,
        IrJsonNull,
        IrKeyValueMethod,
        IrLastIndexVariable,
        IrLiteral,
        IrMemberAccessor,
        IrNamedJsonVariable,
        IrNamedValueVariable,
        IrPredicate,
        IrPredicateCurrentItemVariable,
        IrSizeMethod,
        IrTypeMethod
{
    default <R, C> R accept(IrJsonPathVisitor<R, C> visitor, C context)
    {
        return visitor.visitIrPathNode(this, context);
    }

    /**
     * Get the result type, whenever known.
     * Type might be known for IrPathNodes returning a singleton sequence (e.g. IrArithmeticBinary),
     * as well as for IrPathNodes returning a sequence of arbitrary length (e.g. IrSizeMethod).
     * If the node potentially returns a non-singleton sequence, this method shall return Type
     * only if the type is the same for all elements of the sequence.
     * NOTE: Type is not applicable to every IrPathNode. If the IrPathNode produces an empty sequence,
     * a JSON null, or a sequence containing non-literal JSON items, Type cannot be determined.
     */
    default Optional<Type> type()
    {
        return Optional.empty();
    }
}
