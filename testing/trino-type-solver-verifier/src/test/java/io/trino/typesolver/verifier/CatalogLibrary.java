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
package io.trino.typesolver.verifier;

import io.trino.lib.TrinoPreset;
import io.trino.lib.TrinoSpecificity;
import io.trino.spi.function.FunctionKind;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.OperatorType;
import org.weakref.solver.PatternCoercion;
import org.weakref.solver.Specificity;
import org.weakref.solver.TypeLibrary;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static io.trino.metadata.OperatorNameUtil.isOperatorName;
import static io.trino.metadata.OperatorNameUtil.unmangleOperator;
import static org.weakref.solver.Expression.apply;
import static org.weakref.solver.Expression.literal;
import static org.weakref.solver.Expression.symbol;

/// Builds a solver [TypeLibrary] fed from Trino's live function catalog: the preset contributes the
/// type catalog and coercion/cast rules, while every function definition comes from the catalog's
/// real signatures via [SignatureBridge] — no hand-modeled functions.
///
/// Operators are bridged under the symbols the parser produces. Comparison operators are symmetric
/// in their argument types, so the flipped forms share the canonical operator's scheme, mirroring
/// how the analyzer canonicalizes them (`a > b` resolves `LESS_THAN(b, a)`).
final class CatalogLibrary
{
    private static final Map<OperatorType, List<String>> OPERATOR_SYMBOLS = Map.of(
            OperatorType.ADD, List.of("+"),
            OperatorType.SUBTRACT, List.of("-"),
            OperatorType.MULTIPLY, List.of("*"),
            OperatorType.DIVIDE, List.of("/"),
            OperatorType.MODULO, List.of("%"),
            OperatorType.EQUAL, List.of("=", "<>"),
            OperatorType.LESS_THAN, List.of("<", ">"),
            OperatorType.LESS_THAN_OR_EQUAL, List.of("<=", ">="),
            OperatorType.SUBSCRIPT, List.of("[]"));

    private CatalogLibrary() {}

    static TypeLibrary fromCatalog(Collection<FunctionMetadata> functions)
    {
        TypeLibrary.Builder builder = TypeLibrary.builder();
        TrinoPreset.typeConstructors().forEach(builder::registerType);
        TrinoPreset.coercionRules().forEach(builder::registerCoercion);
        TrinoPreset.castRules().forEach(builder::registerCast);
        builder.withSpecificity(Specificity.BY_COERCION_COUNT.then(new TrinoSpecificity(TrinoPreset.typeSystem())));

        // The preset models unbounded varchar as a distinct parameterless type, but the catalog's
        // signatures are parametric over varchar(x) with the MAX_VALUE sentinel standing in for
        // unbounded. Let an unbounded varchar flow into those signatures by pinning the length to
        // the sentinel — the same reinterpretation Trino itself performs.
        builder.registerCoercion(new PatternCoercion(symbol("varchar"), apply("varchar", literal(Integer.MAX_VALUE)), List.of()));

        for (FunctionMetadata metadata : functions) {
            if (metadata.getKind() != FunctionKind.SCALAR) {
                continue;
            }
            if (isOperatorName(metadata.getCanonicalName())) {
                for (String symbol : OPERATOR_SYMBOLS.getOrDefault(unmangleOperator(metadata.getCanonicalName()), List.of())) {
                    builder.registerFunction(symbol, SignatureBridge.toTypeScheme(metadata.getSignature()));
                }
                continue;
            }
            if (metadata.isHidden() || metadata.getCanonicalName().startsWith("$")) {
                continue;
            }
            for (String name : metadata.getNames()) {
                if (!name.startsWith("$")) {
                    builder.registerFunction(name, SignatureBridge.toTypeScheme(metadata.getSignature()));
                }
            }
        }
        return builder.build();
    }
}
