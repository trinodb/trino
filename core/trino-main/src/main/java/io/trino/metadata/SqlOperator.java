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
package io.trino.metadata;

import io.trino.spi.function.OperatorType;
import io.trino.spi.type.TypeSignature;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.metadata.FunctionKind.SCALAR;
import static io.trino.metadata.Signature.mangleOperatorName;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.trino.spi.function.OperatorType.SUBSCRIPT;
import static java.util.Collections.nCopies;

public abstract class SqlOperator
        extends SqlScalarFunction
{
    protected SqlOperator(
            OperatorType operatorType,
            List<TypeVariableConstraint> typeVariableConstraints,
            List<LongVariableConstraint> longVariableConstraints,
            TypeSignature returnType,
            List<TypeSignature> argumentTypes,
            boolean nullable)
    {
        // TODO This should take Signature!
        super(new FunctionMetadata(
                new Signature(
                        mangleOperatorName(operatorType),
                        typeVariableConstraints,
                        longVariableConstraints,
                        returnType,
                        argumentTypes,
                        false),
                new FunctionNullability(nullable, nCopies(argumentTypes.size(), operatorType == IS_DISTINCT_FROM || operatorType == INDETERMINATE)),
                true,
                true,
                "",
                SCALAR));
        if (operatorType == EQUAL || operatorType == SUBSCRIPT) {
            checkArgument(nullable, "%s operator for %s must be nullable", operatorType, argumentTypes.get(0));
        }
    }
}
