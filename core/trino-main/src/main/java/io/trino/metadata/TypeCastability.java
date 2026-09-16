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

import io.trino.connector.system.GlobalSystemConnector;
import io.trino.spi.TrinoException;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.Signature;
import io.trino.spi.function.TypeVariableConstraint;
import io.trino.spi.function.VariableDeclaration;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeTemplates;
import io.trino.type.TypeResolutionPolicy;
import io.trino.type.UnknownType;

import java.util.List;

import static io.trino.metadata.GlobalFunctionCatalog.BUILTIN_SCHEMA;
import static io.trino.metadata.OperatorNameUtil.mangleOperatorName;
import static io.trino.spi.function.OperatorType.CAST;

/// Checks explicit cast capabilities, including recursive row casts, without re-entering
/// the cache for the cast whose signature is currently being bound.
final class TypeCastability
{
    private final Metadata metadata;
    private final TypeResolutionPolicy policy;

    TypeCastability(Metadata metadata, TypeResolutionPolicy policy)
    {
        this.metadata = metadata;
        this.policy = policy;
    }

    public boolean canCast(Type fromType, Type toType)
    {
        // NULL can be cast to any type; avoid re-entering coercion cache.
        if (fromType instanceof UnknownType || toType instanceof UnknownType) {
            return true;
        }
        if (fromType instanceof RowType fromRowType) {
            if (toType instanceof RowType toRowType) {
                List<Type> fromTypeParameters = fromRowType.getFieldTypes();
                List<Type> toTypeParameters = toRowType.getFieldTypes();
                if (fromTypeParameters.size() != toTypeParameters.size()) {
                    return false;
                }
                for (int fieldIndex = 0; fieldIndex < fromTypeParameters.size(); fieldIndex++) {
                    if (!canCast(fromTypeParameters.get(fieldIndex), toTypeParameters.get(fieldIndex))) {
                        return false;
                    }
                }
                return true;
            }
            if (isRecursiveCastFromRow(toType)) {
                return fromType.getTypeParameters().stream()
                        .allMatch(fromTypeParameter -> canCast(fromTypeParameter, toType));
            }
            return false;
        }
        if (toType instanceof RowType toRowType) {
            if (isRecursiveCastToRow(fromType)) {
                return toRowType.getFieldTypes().stream()
                        .allMatch(toTypeParameter -> canCast(fromType, toTypeParameter));
            }
        }
        try {
            metadata.getCoercion(policy, fromType, toType);
            return true;
        }
        catch (TrinoException e) {
            return false;
        }
    }

    /// Check if there is a recursive variadic CAST from ROW.
    /// This needs special handling because the cast is applied to each field of ROW individually.
    private boolean isRecursiveCastFromRow(Type toType)
    {
        return metadata.getFunctions(null, new CatalogSchemaFunctionName(GlobalSystemConnector.NAME, BUILTIN_SCHEMA, mangleOperatorName(CAST))).stream()
                .map(cast -> cast.functionMetadata().getSignature())
                .anyMatch(signature -> isRecursiveCastFromRow(toType, signature));
    }

    private static boolean isRecursiveCastFromRow(Type toType, Signature signature)
    {
        // the return type must match toType
        if (!signature.getReturnType().equals(TypeTemplates.fromTypeDescriptor(toType.getTypeDescriptor()))) {
            return false;
        }

        // there must be exactly one variable, a type variable
        if (signature.getVariables().size() != 1 || !(signature.getVariables().getFirst() instanceof VariableDeclaration.TypeVariable(TypeVariableConstraint typeVariableConstraint))) {
            return false;
        }

        // The argument type must be a type variable with variadic bound of "row"
        return signature.getArgumentTypes().size() == 1 &&
                signature.getArgumentTypes().getFirst().baseName().equals(typeVariableConstraint.getName()) &&
                typeVariableConstraint.isRowType();
    }

    /// Check if there is a recursive variadic CAST to ROW.
    /// This needs special handling because the cast is applied to each field of ROW individually.
    private boolean isRecursiveCastToRow(Type fromType)
    {
        return metadata.getFunctions(null, new CatalogSchemaFunctionName(GlobalSystemConnector.NAME, BUILTIN_SCHEMA, mangleOperatorName(CAST))).stream()
                .map(cast -> cast.functionMetadata().getSignature())
                .anyMatch(signature -> isRecursiveCastToRow(fromType, signature));
    }

    private static boolean isRecursiveCastToRow(Type fromType, Signature signature)
    {
        // the argument type must match fromType
        if (signature.getArgumentTypes().size() != 1 || !signature.getArgumentTypes().getFirst().equals(TypeTemplates.fromTypeDescriptor(fromType.getTypeDescriptor()))) {
            return false;
        }

        // there must be exactly one variable, a type variable
        if (signature.getVariables().size() != 1 || !(signature.getVariables().getFirst() instanceof VariableDeclaration.TypeVariable(TypeVariableConstraint typeVariableConstraint))) {
            return false;
        }

        // The return type must be a type variable with variadic bound of "row"
        return signature.getReturnType().baseName().equals(typeVariableConstraint.getName()) &&
                typeVariableConstraint.isRowType();
    }
}
