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

import com.google.common.collect.ImmutableList;
import io.trino.spi.function.Signature;
import io.trino.spi.function.TypeVariableConstraint;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeParameter;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.analyzer.TypeSignatureProvider;
import io.trino.type.TypeCoercion;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.util.Objects.requireNonNull;

/**
 * Compares already-applicable functions to determine which one is more specific for overload
 * selection in {@link FunctionBinder}.
 * <p>
 * This helper is intentionally specialized to FunctionBinder's current needs. It is not a general
 * partial order over arbitrary signatures or bindings.
 * <p>
 * The comparison is performed in two phases:
 * <ol>
 * <li>Compare already-bound applicable functions using the existing forwardability rule: a bound
 * call handled by one function is more specific if it can also bind to the other function's
 * declared signature.</li>
 * <li>If that relation is mutual, compare the declared signatures structurally to choose the
 * narrower declaration deterministically.</li>
 * </ol>
 */
final class FunctionSpecificityComparator
{
    enum Specificity
    {
        MORE_SPECIFIC,
        LESS_SPECIFIC,
        EQUIVALENT,
        INCOMPARABLE;

        Specificity reverse()
        {
            return switch (this) {
                case MORE_SPECIFIC -> LESS_SPECIFIC;
                case LESS_SPECIFIC -> MORE_SPECIFIC;
                case EQUIVALENT, INCOMPARABLE -> this;
            };
        }
    }

    private final Metadata metadata;
    private final TypeManager typeManager;
    private final TypeCoercion typeCoercion;

    FunctionSpecificityComparator(Metadata metadata, TypeManager typeManager)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.typeCoercion = new TypeCoercion(typeManager::getType);
    }

    /**
     * Compares two already-applicable functions for overload selection in {@link FunctionBinder}.
     * <p>
     * The caller must provide both the bound signature selected for the actual call site and the
     * original declared signature for each function. Usage outside this exact FunctionBinder flow
     * has not been considered.
     */
    Specificity compareSpecificity(
            Signature leftBoundSignature,
            Signature leftDeclaredSignature,
            Signature rightBoundSignature,
            Signature rightDeclaredSignature)
    {
        requireNonNull(leftBoundSignature, "leftBoundSignature is null");
        requireNonNull(leftDeclaredSignature, "leftDeclaredSignature is null");
        requireNonNull(rightBoundSignature, "rightBoundSignature is null");
        requireNonNull(rightDeclaredSignature, "rightDeclaredSignature is null");

        int arity = leftBoundSignature.getArgumentTypes().size();
        verify(arity == rightBoundSignature.getArgumentTypes().size(), "bound signatures have different arities");

        boolean leftMoreSpecific = isMoreSpecificThan(leftBoundSignature, rightDeclaredSignature);
        boolean rightMoreSpecific = isMoreSpecificThan(rightBoundSignature, leftDeclaredSignature);

        if (leftMoreSpecific && !rightMoreSpecific) {
            return Specificity.MORE_SPECIFIC;
        }
        if (rightMoreSpecific && !leftMoreSpecific) {
            return Specificity.LESS_SPECIFIC;
        }
        if (!leftMoreSpecific) {
            return Specificity.INCOMPARABLE;
        }

        return compareDeclaredSignatures(leftDeclaredSignature, rightDeclaredSignature, arity);
    }

    /**
     * Compares two declared signatures structurally after applicability has already been
     * established. This is only used as a deterministic tie-break for mutually specific bindings.
     */
    Specificity compareDeclaredSignatures(Signature left, Signature right, int arity)
    {
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");
        checkArgument(arity >= 0, "arity is negative");

        List<TypeSignature> leftArguments = expandArguments(left, arity);
        List<TypeSignature> rightArguments = expandArguments(right, arity);

        Map<String, TypeVariableConstraint> leftConstraints = constraintMap(left);
        Map<String, TypeVariableConstraint> rightConstraints = constraintMap(right);

        Specificity comparison = Specificity.EQUIVALENT;
        // compare each argument position
        for (int i = 0; i < arity; i++) {
            comparison = combine(comparison, compareTypeSignatures(leftArguments.get(i), rightArguments.get(i), leftConstraints, rightConstraints));
            if (comparison == Specificity.INCOMPARABLE) {
                return Specificity.INCOMPARABLE;
            }
        }

        comparison = combine(comparison, compareEqualityPatterns(leftArguments, rightArguments, leftConstraints, rightConstraints));
        if (comparison == Specificity.INCOMPARABLE) {
            return Specificity.INCOMPARABLE;
        }

        if (comparison == Specificity.EQUIVALENT && left.isVariableArity() != right.isVariableArity()) {
            return left.isVariableArity() ? Specificity.LESS_SPECIFIC : Specificity.MORE_SPECIFIC;
        }

        return comparison;
    }

    private boolean isMoreSpecificThan(Signature boundSignature, Signature declaredSignature)
    {
        List<TypeSignatureProvider> resolvedTypes = TypeSignatureProvider.fromTypeSignatures(boundSignature.getArgumentTypes());
        return new SignatureBinder(metadata, typeManager, declaredSignature, true)
                .canBind(resolvedTypes);
    }

    /**
     * Compares two type signatures structurally after applicability has already been established.
     * Concrete types are always narrower than type variables at this stage.
     */
    private Specificity compareTypeSignatures(
            TypeSignature left,
            TypeSignature right,
            Map<String, TypeVariableConstraint> leftConstraints,
            Map<String, TypeVariableConstraint> rightConstraints)
    {
        boolean leftTypeVariable = isTypeVariable(left, leftConstraints);
        boolean rightTypeVariable = isTypeVariable(right, rightConstraints);
        if (leftTypeVariable && rightTypeVariable) {
            return Specificity.EQUIVALENT;
        }
        if (leftTypeVariable) {
            return Specificity.LESS_SPECIFIC;
        }
        if (rightTypeVariable) {
            return Specificity.MORE_SPECIFIC;
        }

        if (left.equals(right)) {
            return Specificity.EQUIVALENT;
        }

        Optional<Type> leftConcrete = resolveConcrete(left);
        if (leftConcrete.isPresent()) {
            Optional<Type> rightConcrete = resolveConcrete(right);
            if (rightConcrete.isPresent()) {
                return compareConcreteTypes(leftConcrete.get(), rightConcrete.get());
            }
        }
        return compareParametricTypes(left, right, leftConstraints, rightConstraints);
    }

    private Specificity compareConcreteTypes(Type left, Type right)
    {
        if (left.equals(right)) {
            return Specificity.EQUIVALENT;
        }

        boolean leftToRight = typeCoercion.canCoerce(left, right);
        boolean rightToLeft = typeCoercion.canCoerce(right, left);
        if (leftToRight && !rightToLeft) {
            return Specificity.MORE_SPECIFIC;
        }
        if (rightToLeft && !leftToRight) {
            return Specificity.LESS_SPECIFIC;
        }
        return Specificity.INCOMPARABLE;
    }

    private Specificity compareParametricTypes(TypeSignature left, TypeSignature right, Map<String, TypeVariableConstraint> leftConstraints, Map<String, TypeVariableConstraint> rightConstraints)
    {
        if (!left.getBase().equalsIgnoreCase(right.getBase())) {
            return Specificity.INCOMPARABLE;
        }
        if (left.getParameters().size() != right.getParameters().size()) {
            return Specificity.INCOMPARABLE;
        }

        Specificity comparison = Specificity.EQUIVALENT;
        for (int i = 0; i < left.getParameters().size(); i++) {
            comparison = combine(comparison, compareTypeParameters(left.getParameters().get(i), right.getParameters().get(i), leftConstraints, rightConstraints));
            if (comparison == Specificity.INCOMPARABLE) {
                return Specificity.INCOMPARABLE;
            }
        }
        return comparison;
    }

    private Specificity compareTypeParameters(
            TypeParameter left,
            TypeParameter right,
            Map<String, TypeVariableConstraint> leftConstraints,
            Map<String, TypeVariableConstraint> rightConstraints)
    {
        return switch (left) {
            case TypeParameter.Numeric leftNumeric -> switch (right) {
                case TypeParameter.Numeric rightNumeric -> leftNumeric.value() == rightNumeric.value() ? Specificity.EQUIVALENT : Specificity.INCOMPARABLE;
                case TypeParameter.Variable _ -> Specificity.MORE_SPECIFIC;
                case TypeParameter.Type _ -> Specificity.INCOMPARABLE;
            };
            case TypeParameter.Variable _ -> switch (right) {
                case TypeParameter.Numeric _ -> Specificity.LESS_SPECIFIC;
                case TypeParameter.Variable _ -> Specificity.EQUIVALENT;
                case TypeParameter.Type _ -> Specificity.INCOMPARABLE;
            };
            case TypeParameter.Type leftType -> switch (right) {
                case TypeParameter.Numeric _, TypeParameter.Variable _ -> Specificity.INCOMPARABLE;
                case TypeParameter.Type rightType -> compareTypeSignatures(leftType.type(), rightType.type(), leftConstraints, rightConstraints);
            };
        };
    }

    /**
     * Compares the equality pairs required by two signatures.
     * <p>
     * The per-argument comparison only looks at each argument position independently. This method
     * adds the missing cross-position constraints.
     * <p>
     * For example, {@code (T, T)} is narrower than {@code (T, U)} because it requires the two
     * argument locations to be equal. Likewise, {@code decimal(p, s), decimal(p, s)} is narrower
     * than {@code decimal(p1, s1), decimal(p2, s2)} because it requires both {@code p} locations
     * to match and both {@code s} locations to match.
     * <p>
     * Each signature is reduced to the set of equality pairs returned by
     * {@link #collectEqualityPatterns(List, Map)}. If one signature requires every equality pair
     * required by the other, plus possibly more, it is more specific.
     */
    private static Specificity compareEqualityPatterns(
            List<TypeSignature> leftArguments,
            List<TypeSignature> rightArguments,
            Map<String, TypeVariableConstraint> leftConstraints,
            Map<String, TypeVariableConstraint> rightConstraints)
    {
        Set<EqualityPattern> leftEqualities = collectEqualityPatterns(leftArguments, leftConstraints);
        Set<EqualityPattern> rightEqualities = collectEqualityPatterns(rightArguments, rightConstraints);

        boolean leftImpliesRight = leftEqualities.containsAll(rightEqualities);
        boolean rightImpliesLeft = rightEqualities.containsAll(leftEqualities);
        if (leftImpliesRight && rightImpliesLeft) {
            return Specificity.EQUIVALENT;
        }
        if (leftImpliesRight) {
            return Specificity.MORE_SPECIFIC;
        }
        if (rightImpliesLeft) {
            return Specificity.LESS_SPECIFIC;
        }
        return Specificity.INCOMPARABLE;
    }

    /**
     * Returns every pair of locations in the expanded argument list that are required to be equal
     * because they reuse the same type variable or calculated literal variable.
     * <p>
     * For example, {@code (T, T)} produces one equality pair between the two argument locations,
     * while {@code decimal(p, s), decimal(p, s)} produces one pair for {@code p} and one pair for
     * {@code s}. If the same variable appears in three locations, this returns all three pairwise
     * equalities between those locations.
     */
    private static Set<EqualityPattern> collectEqualityPatterns(List<TypeSignature> argumentTypes, Map<String, TypeVariableConstraint> constraints)
    {
        Map<String, List<Occurrence>> occurrences = new HashMap<>();
        for (int i = 0; i < argumentTypes.size(); i++) {
            collectOccurrences(argumentTypes.get(i), i, new IntArrayList(), constraints, occurrences);
        }

        Set<EqualityPattern> equalities = new HashSet<>();
        for (List<Occurrence> paths : occurrences.values()) {
            for (int i = 0; i < paths.size(); i++) {
                for (int j = i + 1; j < paths.size(); j++) {
                    equalities.add(new EqualityPattern(paths.get(i), paths.get(j)));
                }
            }
        }
        return equalities;
    }

    /**
     * Records where each logical variable occurs in the expanded argument list.
     * <p>
     * Type variables are keyed by variable name, so repeated uses of the same variable contribute
     * multiple paths to the same occurrence list. Calculated literal parameters are handled the
     * same way. Concrete numeric parameters do not participate because they do not represent
     * equality constraints; they are already compared directly in {@link #compareTypeParameters}.
     */
    private static void collectOccurrences(
            TypeSignature signature,
            int argumentIndex,
            IntArrayList parameterPath,
            Map<String, TypeVariableConstraint> constraints,
            Map<String, List<Occurrence>> occurrences)
    {
        if (isTypeVariable(signature, constraints)) {
            occurrences.computeIfAbsent("type:" + signature.getBase(), _ -> new ArrayList<>())
                    .add(new Occurrence(argumentIndex, parameterPath));
            return;
        }

        List<TypeParameter> parameters = signature.getParameters();
        for (int i = 0; i < parameters.size(); i++) {
            switch (parameters.get(i)) {
                case TypeParameter.Numeric _ -> {}
                case TypeParameter.Variable variable -> occurrences.computeIfAbsent("literal:" + variable.name(), _ -> new ArrayList<>())
                        .add(new Occurrence(argumentIndex, parameterPath, i));
                case TypeParameter.Type type -> {
                    parameterPath.push(i);
                    collectOccurrences(type.type(), argumentIndex, parameterPath, constraints, occurrences);
                    parameterPath.popInt();
                }
            }
        }
    }

    private record EqualityPattern(Occurrence left, Occurrence right)
    {
        private EqualityPattern
        {
            requireNonNull(left, "left is null");
            requireNonNull(right, "right is null");
            if (left.compareTo(right) > 0) {
                Occurrence temporary = left;
                left = right;
                right = temporary;
            }
        }
    }

    private record Occurrence(int argumentIndex, IntArrayList parameterPath)
            implements Comparable<Occurrence>
    {
        private Occurrence
        {
            parameterPath = new IntArrayList(requireNonNull(parameterPath, "parameterPath is null"));
        }

        private Occurrence(int argumentIndex, IntArrayList parameterPath, int parameter)
        {
            this(argumentIndex, parameterPath);
            this.parameterPath.add(parameter);
        }

        @Override
        public int compareTo(Occurrence other)
        {
            int comparison = Integer.compare(argumentIndex, other.argumentIndex);
            if (comparison != 0) {
                return comparison;
            }
            return parameterPath.compareTo(other.parameterPath);
        }
    }

    private Optional<Type> resolveConcrete(TypeSignature signature)
    {
        if (signature.isCalculated()) {
            return Optional.empty();
        }
        try {
            return Optional.ofNullable(typeManager.getType(signature));
        }
        catch (RuntimeException ignored) {
            return Optional.empty();
        }
    }

    private static boolean isTypeVariable(TypeSignature signature, Map<String, TypeVariableConstraint> constraints)
    {
        return signature.getParameters().isEmpty() && constraints.containsKey(signature.getBase());
    }

    private static Map<String, TypeVariableConstraint> constraintMap(Signature signature)
    {
        Map<String, TypeVariableConstraint> constraints = new TreeMap<>(CASE_INSENSITIVE_ORDER);
        for (TypeVariableConstraint constraint : signature.getTypeVariableConstraints()) {
            constraints.put(constraint.getName(), constraint);
        }
        return constraints;
    }

    private static List<TypeSignature> expandArguments(Signature signature, int arity)
    {
        List<TypeSignature> formalTypeSignatures = signature.getArgumentTypes();
        if (!signature.isVariableArity()) {
            verify(formalTypeSignatures.size() == arity);
            return formalTypeSignatures;
        }

        verify(arity >= formalTypeSignatures.size() - 1);

        int variableArityArgumentsCount = arity - formalTypeSignatures.size() + 1;
        if (variableArityArgumentsCount == 0) {
            return formalTypeSignatures.subList(0, formalTypeSignatures.size() - 1);
        }
        if (variableArityArgumentsCount == 1) {
            return formalTypeSignatures;
        }

        ImmutableList.Builder<TypeSignature> builder = ImmutableList.builder();
        builder.addAll(formalTypeSignatures);
        TypeSignature lastTypeSignature = formalTypeSignatures.getLast();
        for (int i = 1; i < variableArityArgumentsCount; i++) {
            builder.add(lastTypeSignature);
        }
        return builder.build();
    }

    private static Specificity combine(Specificity current, Specificity next)
    {
        if (current == Specificity.INCOMPARABLE || next == Specificity.INCOMPARABLE) {
            return Specificity.INCOMPARABLE;
        }
        if (current == Specificity.EQUIVALENT) {
            return next;
        }
        if (next == Specificity.EQUIVALENT) {
            return current;
        }
        if (current == next) {
            return current;
        }
        return Specificity.INCOMPARABLE;
    }
}
