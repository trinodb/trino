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

import io.trino.spi.type.Type;

import java.util.Optional;

import static io.trino.spi.type.BooleanType.BOOLEAN;

public sealed interface IrPredicate
        extends IrPathNode
        permits
        IrComparisonPredicate,
        IrConjunctionPredicate,
        IrDisjunctionPredicate,
        IrExistsPredicate,
        IrIsUnknownPredicate,
        IrLikeRegexPredicate,
        IrNegationPredicate,
        IrStartsWithPredicate
{
    @Override
    default <R, C> R accept(IrJsonPathVisitor<R, C> visitor, C context)
    {
        return visitor.visitIrPredicate(this, context);
    }

    @Override
    default Optional<Type> type()
    {
        return Optional.of(BOOLEAN);
    }
}
