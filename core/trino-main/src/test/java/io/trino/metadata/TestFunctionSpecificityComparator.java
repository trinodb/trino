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
import com.google.common.collect.ImmutableSet;
import io.trino.spi.function.Signature;
import io.trino.spi.function.TypeVariableConstraint;
import io.trino.spi.type.TypeSignature;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static io.trino.metadata.FunctionSpecificityComparator.Specificity.EQUIVALENT;
import static io.trino.metadata.FunctionSpecificityComparator.Specificity.INCOMPARABLE;
import static io.trino.metadata.FunctionSpecificityComparator.Specificity.LESS_SPECIFIC;
import static io.trino.metadata.FunctionSpecificityComparator.Specificity.MORE_SPECIFIC;
import static io.trino.spi.function.TypeVariableConstraint.typeVariable;
import static io.trino.sql.analyzer.TypeSignatureTranslator.parseTypeSignature;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestFunctionSpecificityComparator
{
    private static final Set<String> LITERAL_PARAMETERS = ImmutableSet.of("p", "s", "p1", "s1", "p2", "s2", "x", "y");

    private final FunctionSpecificityComparator comparator = new FunctionSpecificityComparator(PLANNER_CONTEXT.getMetadata(), PLANNER_CONTEXT.getTypeManager());

    @Test
    void testCompareSpecificityPreservesConcreteCoercionOrdering()
    {
        Signature decimalAndDecimalBound = functionSignature(ImmutableList.of("decimal(19,0)", "decimal(19,0)"), "boolean").build();
        Signature decimalAndDecimalDeclared = functionSignature(ImmutableList.of("decimal(p,s)", "decimal(p,s)"), "boolean").build();
        Signature decimalAndDoubleBound = functionSignature(ImmutableList.of("decimal(19,0)", "double"), "boolean").build();
        Signature decimalAndDoubleDeclared = functionSignature(ImmutableList.of("decimal(p,s)", "double"), "boolean").build();
        Signature doubleAndDouble = functionSignature(ImmutableList.of("double", "double"), "boolean").build();

        assertSpecificity(decimalAndDecimalBound, decimalAndDecimalDeclared, decimalAndDoubleBound, decimalAndDoubleDeclared, MORE_SPECIFIC);
        assertSpecificity(decimalAndDecimalBound, decimalAndDecimalDeclared, doubleAndDouble, doubleAndDouble, MORE_SPECIFIC);
        assertSpecificity(decimalAndDoubleBound, decimalAndDoubleDeclared, doubleAndDouble, doubleAndDouble, MORE_SPECIFIC);
    }

    @Test
    void testCompareSpecificityUsesDeclaredSignaturesForExactVsGenericTie()
    {
        Signature exact = functionSignature(ImmutableList.of("boolean", "double"), "bigint").build();
        Signature generic = functionSignature(ImmutableList.of("T", "double"), "bigint", ImmutableList.of(typeVariable("T"))).build();

        assertSpecificity(exact, exact, exact, generic, MORE_SPECIFIC);
    }

    @Test
    void testCompareSpecificityUsesDeclaredSignaturesForFixedArityVsVarargsTie()
    {
        Signature fixedArity = functionSignature(ImmutableList.of("boolean", "double", "double"), "boolean").build();
        Signature varargsBound = fixedArity;
        Signature varargsDeclared = functionSignature(ImmutableList.of("boolean", "double"), "boolean")
                .variableArity()
                .build();

        assertSpecificity(fixedArity, fixedArity, varargsBound, varargsDeclared, MORE_SPECIFIC);
    }

    @Test
    void testCompareSpecificityUsesDeclaredSignaturesForConcreteVsCalculatedTie()
    {
        Signature concrete = functionSignature(ImmutableList.of("decimal(3,1)", "double"), "boolean").build();
        Signature calculated = functionSignature(ImmutableList.of("decimal(p,s)", "double"), "boolean").build();

        assertSpecificity(concrete, concrete, concrete, calculated, MORE_SPECIFIC);
    }

    @Test
    void testCompareSpecificityUsesDeclaredSignaturesForRepeatedVariablesTie()
    {
        Signature bound = functionSignature(ImmutableList.of("bigint", "bigint"), "boolean").build();
        Signature repeated = functionSignature(ImmutableList.of("T", "T"), "boolean", ImmutableList.of(typeVariable("T"))).build();
        Signature independent = functionSignature(ImmutableList.of("T", "U"), "boolean", ImmutableList.of(typeVariable("T"), typeVariable("U"))).build();

        assertSpecificity(bound, repeated, bound, independent, MORE_SPECIFIC);
    }

    @Test
    void testCompareSpecificityReturnsIncomparableWhenBoundFunctionsAreNotMutuallySpecific()
    {
        Signature bigint = functionSignature(ImmutableList.of("bigint"), "boolean").build();
        Signature varchar = functionSignature(ImmutableList.of("varchar(3)"), "boolean").build();

        assertSpecificity(bigint, bigint, varchar, varchar, INCOMPARABLE);
    }

    @Test
    void testEquivalentSignatures()
    {
        Signature concrete = functionSignature(ImmutableList.of("bigint"), "boolean").build();
        assertThat(comparator.compareDeclaredSignatures(concrete, concrete, 1)).isEqualTo(EQUIVALENT);

        Signature genericLeft = functionSignature(ImmutableList.of("T"), "boolean", ImmutableList.of(typeVariable("T"))).build();
        Signature genericRight = functionSignature(ImmutableList.of("U"), "boolean", ImmutableList.of(typeVariable("U"))).build();
        assertThat(comparator.compareDeclaredSignatures(genericLeft, genericRight, 1)).isEqualTo(EQUIVALENT);

        Signature calculatedLeft = functionSignature(ImmutableList.of("decimal(p,s)"), "boolean").build();
        Signature calculatedRight = functionSignature(ImmutableList.of("decimal(x,y)"), "boolean").build();
        assertThat(comparator.compareDeclaredSignatures(calculatedLeft, calculatedRight, 1)).isEqualTo(EQUIVALENT);
    }

    @Test
    void testExactIsMoreSpecificThanGeneric()
    {
        Signature exact = functionSignature(ImmutableList.of("boolean", "double"), "bigint").build();
        Signature generic = functionSignature(ImmutableList.of("T", "double"), "bigint", ImmutableList.of(typeVariable("T"))).build();

        assertThat(comparator.compareDeclaredSignatures(exact, generic, 2)).isEqualTo(MORE_SPECIFIC);
        assertThat(comparator.compareDeclaredSignatures(generic, exact, 2)).isEqualTo(LESS_SPECIFIC);
    }

    @Test
    void testApproxDistinctSpecializationShape()
    {
        Signature specialized = functionSignature(ImmutableList.of("boolean", "double"), "bigint").build();
        Signature genericComparable = Signature.builder()
                .returnType(typeSignature("bigint"))
                .argumentType(typeSignature("T"))
                .argumentType(typeSignature("double"))
                .comparableTypeParameter("T")
                .build();

        assertThat(comparator.compareDeclaredSignatures(specialized, genericComparable, 2)).isEqualTo(MORE_SPECIFIC);
        assertThat(comparator.compareDeclaredSignatures(genericComparable, specialized, 2)).isEqualTo(LESS_SPECIFIC);
    }

    @Test
    void testFixedArityIsMoreSpecificThanVarargs()
    {
        Signature fixedArity = functionSignature(ImmutableList.of("boolean", "double", "double"), "boolean").build();
        Signature varargs = functionSignature(ImmutableList.of("boolean", "double"), "boolean")
                .variableArity()
                .build();

        assertThat(comparator.compareDeclaredSignatures(fixedArity, varargs, 3)).isEqualTo(MORE_SPECIFIC);
        assertThat(comparator.compareDeclaredSignatures(varargs, fixedArity, 3)).isEqualTo(LESS_SPECIFIC);
    }

    @Test
    void testConcreteIsMoreSpecificThanCalculated()
    {
        Signature concreteDecimal = functionSignature(ImmutableList.of("decimal(3,1)", "double"), "boolean").build();
        Signature calculatedDecimal = functionSignature(ImmutableList.of("decimal(p,s)", "double"), "boolean").build();

        assertThat(comparator.compareDeclaredSignatures(concreteDecimal, calculatedDecimal, 2)).isEqualTo(MORE_SPECIFIC);
        assertThat(comparator.compareDeclaredSignatures(calculatedDecimal, concreteDecimal, 2)).isEqualTo(LESS_SPECIFIC);

        Signature concreteVarchar = functionSignature(ImmutableList.of("varchar(3)"), "boolean").build();
        Signature calculatedVarchar = functionSignature(ImmutableList.of("varchar(x)"), "boolean").build();

        assertThat(comparator.compareDeclaredSignatures(concreteVarchar, calculatedVarchar, 1)).isEqualTo(MORE_SPECIFIC);
        assertThat(comparator.compareDeclaredSignatures(calculatedVarchar, concreteVarchar, 1)).isEqualTo(LESS_SPECIFIC);
    }

    @Test
    void testConcreteCoercionSpecificity()
    {
        Signature bigint = functionSignature(ImmutableList.of("bigint"), "boolean").build();
        Signature doubleType = functionSignature(ImmutableList.of("double"), "boolean").build();
        Signature varchar = functionSignature(ImmutableList.of("varchar(3)"), "boolean").build();
        Signature arrayBigint = functionSignature(ImmutableList.of("array(bigint)"), "boolean").build();
        Signature arrayDouble = functionSignature(ImmutableList.of("array(double)"), "boolean").build();

        assertThat(comparator.compareDeclaredSignatures(bigint, doubleType, 1)).isEqualTo(MORE_SPECIFIC);
        assertThat(comparator.compareDeclaredSignatures(doubleType, bigint, 1)).isEqualTo(LESS_SPECIFIC);
        assertThat(comparator.compareDeclaredSignatures(bigint, varchar, 1)).isEqualTo(INCOMPARABLE);
        assertThat(comparator.compareDeclaredSignatures(arrayBigint, arrayDouble, 1)).isEqualTo(MORE_SPECIFIC);
    }

    @Test
    void testTypeVariableConstraintsAreNotOrdered()
    {
        Signature unconstrained = Signature.builder()
                .returnType(typeSignature("boolean"))
                .argumentType(typeSignature("T"))
                .typeVariable("T")
                .build();
        Signature comparable = Signature.builder()
                .returnType(typeSignature("boolean"))
                .argumentType(typeSignature("T"))
                .comparableTypeParameter("T")
                .build();
        Signature orderable = Signature.builder()
                .returnType(typeSignature("boolean"))
                .argumentType(typeSignature("T"))
                .orderableTypeParameter("T")
                .build();

        assertThat(comparator.compareDeclaredSignatures(orderable, comparable, 1)).isEqualTo(EQUIVALENT);
        assertThat(comparator.compareDeclaredSignatures(comparable, orderable, 1)).isEqualTo(EQUIVALENT);
        assertThat(comparator.compareDeclaredSignatures(comparable, unconstrained, 1)).isEqualTo(EQUIVALENT);
        assertThat(comparator.compareDeclaredSignatures(unconstrained, comparable, 1)).isEqualTo(EQUIVALENT);
    }

    @Test
    void testConcreteIsMoreSpecificThanConstrainedGenericAfterApplicability()
    {
        Signature json = functionSignature(ImmutableList.of("json"), "boolean").build();
        Signature orderable = Signature.builder()
                .returnType(typeSignature("boolean"))
                .argumentType(typeSignature("T"))
                .orderableTypeParameter("T")
                .build();

        assertThat(comparator.compareDeclaredSignatures(json, orderable, 1)).isEqualTo(MORE_SPECIFIC);
        assertThat(comparator.compareDeclaredSignatures(orderable, json, 1)).isEqualTo(LESS_SPECIFIC);
    }

    @Test
    void testRepeatedTypeVariablesAreMoreSpecificThanIndependentOnes()
    {
        Signature repeated = functionSignature(ImmutableList.of("T", "T"), "boolean", ImmutableList.of(typeVariable("T"))).build();
        Signature independent = functionSignature(ImmutableList.of("T", "U"), "boolean", ImmutableList.of(typeVariable("T"), typeVariable("U"))).build();

        assertThat(comparator.compareDeclaredSignatures(repeated, independent, 2)).isEqualTo(MORE_SPECIFIC);
        assertThat(comparator.compareDeclaredSignatures(independent, repeated, 2)).isEqualTo(LESS_SPECIFIC);
    }

    @Test
    void testRepeatedCalculatedVariablesAreMoreSpecificThanIndependentOnes()
    {
        Signature repeated = functionSignature(ImmutableList.of("decimal(p,s)", "decimal(p,s)"), "boolean").build();
        Signature independent = functionSignature(ImmutableList.of("decimal(p1,s1)", "decimal(p2,s2)"), "boolean").build();

        assertThat(comparator.compareDeclaredSignatures(repeated, independent, 2)).isEqualTo(MORE_SPECIFIC);
        assertThat(comparator.compareDeclaredSignatures(independent, repeated, 2)).isEqualTo(LESS_SPECIFIC);
    }

    private static Signature.Builder functionSignature(List<String> arguments, String returnType)
    {
        return functionSignature(arguments, returnType, ImmutableList.of());
    }

    private static Signature.Builder functionSignature(List<String> arguments, String returnType, List<TypeVariableConstraint> typeVariableConstraints)
    {
        List<TypeSignature> argumentSignatures = arguments.stream()
                .map(TestFunctionSpecificityComparator::typeSignature)
                .toList();

        return Signature.builder()
                .returnType(typeSignature(returnType))
                .argumentTypes(argumentSignatures)
                .typeVariableConstraints(typeVariableConstraints);
    }

    private static TypeSignature typeSignature(String signature)
    {
        return parseTypeSignature(signature, LITERAL_PARAMETERS);
    }

    private void assertSpecificity(
            Signature leftBoundSignature,
            Signature leftDeclaredSignature,
            Signature rightBoundSignature,
            Signature rightDeclaredSignature,
            FunctionSpecificityComparator.Specificity expectedSpecificity)
    {
        assertThat(comparator.compareSpecificity(leftBoundSignature, leftDeclaredSignature, rightBoundSignature, rightDeclaredSignature))
                .isEqualTo(expectedSpecificity);
        assertThat(comparator.compareSpecificity(rightBoundSignature, rightDeclaredSignature, leftBoundSignature, leftDeclaredSignature))
                .isEqualTo(opposite(expectedSpecificity));
    }

    private static FunctionSpecificityComparator.Specificity opposite(FunctionSpecificityComparator.Specificity specificity)
    {
        return switch (specificity) {
            case MORE_SPECIFIC -> LESS_SPECIFIC;
            case LESS_SPECIFIC -> MORE_SPECIFIC;
            case EQUIVALENT, INCOMPARABLE -> specificity;
        };
    }
}
