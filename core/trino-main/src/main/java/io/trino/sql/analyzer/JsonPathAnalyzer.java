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
package io.trino.sql.analyzer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.OperatorNotFoundException;
import io.trino.spi.TrinoException;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.sql.jsonpath.PathNodeRef;
import io.trino.sql.jsonpath.PathParser;
import io.trino.sql.jsonpath.PathParser.Location;
import io.trino.sql.jsonpath.tree.AbsMethod;
import io.trino.sql.jsonpath.tree.ArithmeticBinary;
import io.trino.sql.jsonpath.tree.ArithmeticUnary;
import io.trino.sql.jsonpath.tree.ArrayAccessor;
import io.trino.sql.jsonpath.tree.ArrayAccessor.Subscript;
import io.trino.sql.jsonpath.tree.CeilingMethod;
import io.trino.sql.jsonpath.tree.ComparisonPredicate;
import io.trino.sql.jsonpath.tree.ConjunctionPredicate;
import io.trino.sql.jsonpath.tree.ContextVariable;
import io.trino.sql.jsonpath.tree.DatetimeMethod;
import io.trino.sql.jsonpath.tree.DescendantMemberAccessor;
import io.trino.sql.jsonpath.tree.DisjunctionPredicate;
import io.trino.sql.jsonpath.tree.DoubleMethod;
import io.trino.sql.jsonpath.tree.ExistsPredicate;
import io.trino.sql.jsonpath.tree.Filter;
import io.trino.sql.jsonpath.tree.FloorMethod;
import io.trino.sql.jsonpath.tree.IsUnknownPredicate;
import io.trino.sql.jsonpath.tree.JsonNullLiteral;
import io.trino.sql.jsonpath.tree.JsonPath;
import io.trino.sql.jsonpath.tree.JsonPathTreeVisitor;
import io.trino.sql.jsonpath.tree.KeyValueMethod;
import io.trino.sql.jsonpath.tree.LastIndexVariable;
import io.trino.sql.jsonpath.tree.LikeRegexPredicate;
import io.trino.sql.jsonpath.tree.MemberAccessor;
import io.trino.sql.jsonpath.tree.NamedVariable;
import io.trino.sql.jsonpath.tree.NegationPredicate;
import io.trino.sql.jsonpath.tree.PathNode;
import io.trino.sql.jsonpath.tree.PredicateCurrentItemVariable;
import io.trino.sql.jsonpath.tree.SizeMethod;
import io.trino.sql.jsonpath.tree.SqlValueLiteral;
import io.trino.sql.jsonpath.tree.StartsWithPredicate;
import io.trino.sql.jsonpath.tree.TypeMethod;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.StringLiteral;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.StandardErrorCode.INVALID_PATH;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.function.OperatorType.NEGATION;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.ExpressionAnalyzer.isCharacterStringType;
import static io.trino.sql.analyzer.ExpressionAnalyzer.isNumericType;
import static io.trino.sql.analyzer.ExpressionAnalyzer.isStringType;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractLocation;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.jsonpath.tree.ArithmeticUnary.Sign.PLUS;
import static io.trino.type.Json2016Type.JSON_2016;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class JsonPathAnalyzer
{
    // the type() method returns a textual description of type as determined by the SQL standard, of length lower or equal to 27
    private static final Type TYPE_METHOD_RESULT_TYPE = createVarcharType(27);

    private final Metadata metadata;
    private final Session session;
    private final ExpressionAnalyzer literalAnalyzer;
    private final Map<PathNodeRef<PathNode>, Type> types = new LinkedHashMap<>();
    private final Set<PathNodeRef<PathNode>> jsonParameters = new LinkedHashSet<>();

    public JsonPathAnalyzer(Metadata metadata, Session session, ExpressionAnalyzer literalAnalyzer)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.session = requireNonNull(session, "session is null");
        this.literalAnalyzer = requireNonNull(literalAnalyzer, "literalAnalyzer is null");
    }

    public JsonPathAnalysis analyzeJsonPath(StringLiteral path, Map<String, Type> parameterTypes)
    {
        Location pathStart = extractLocation(path)
                .map(location -> new Location(location.getLineNumber(), location.getColumnNumber()))
                .orElseThrow(() -> new IllegalStateException("missing NodeLocation in path"));
        PathNode root = new PathParser(pathStart).parseJsonPath(path.getValue());
        new Visitor(parameterTypes, path).process(root);
        return new JsonPathAnalysis((JsonPath) root, types, jsonParameters);
    }

    /**
     * This visitor determines and validates output types of PathNodes, whenever they can be deduced and represented as SQL types.
     * In some cases, the type of a PathNode can be determined without context. E.g., the `double()` method always returns DOUBLE.
     * In some other cases, the type depends on child nodes. E.g. the return type of the `abs()` method is the same as input type.
     * In some cases, the type cannot be represented as SQL type. E.g. the `keyValue()` method returns JSON objects.
     * Some PathNodes, including accessors, return objects whose types might or might not be representable as SQL types,
     * but that cannot be determined upfront.
     */
    private class Visitor
            extends JsonPathTreeVisitor<Type, Void>
    {
        private final Map<String, Type> parameterTypes;
        private final Node pathNode;

        public Visitor(Map<String, Type> parameterTypes, Node pathNode)
        {
            this.parameterTypes = ImmutableMap.copyOf(requireNonNull(parameterTypes, "parameterTypes is null"));
            this.pathNode = requireNonNull(pathNode, "pathNode is null");
        }

        @Override
        protected Type visitPathNode(PathNode node, Void context)
        {
            throw new UnsupportedOperationException("not supported JSON path node: " + node.getClass().getSimpleName());
        }

        @Override
        protected Type visitAbsMethod(AbsMethod node, Void context)
        {
            Type sourceType = process(node.getBase());
            if (sourceType != null) {
                Type resultType;
                try {
                    resultType = metadata.resolveFunction(session, QualifiedName.of("abs"), fromTypes(sourceType)).getSignature().getReturnType();
                }
                catch (TrinoException e) {
                    throw semanticException(INVALID_PATH, pathNode, e, "cannot perform JSON path abs() method with %s argument: %s", sourceType.getDisplayName(), e.getMessage());
                }
                types.put(PathNodeRef.of(node), resultType);
                return resultType;
            }

            return null;
        }

        @Override
        protected Type visitArithmeticBinary(ArithmeticBinary node, Void context)
        {
            Type leftType = process(node.getLeft());
            Type rightType = process(node.getRight());
            if (leftType != null && rightType != null) {
                BoundSignature signature;
                try {
                    signature = metadata.resolveOperator(session, OperatorType.valueOf(node.getOperator().name()), ImmutableList.of(leftType, rightType)).getSignature();
                }
                catch (OperatorNotFoundException e) {
                    throw semanticException(INVALID_PATH, pathNode, e, "invalid operand types (%s and %s) in JSON path arithmetic binary expression: %s", leftType.getDisplayName(), rightType.getDisplayName(), e.getMessage());
                }
                Type resultType = signature.getReturnType();
                types.put(PathNodeRef.of(node), resultType);
                return resultType;
            }

            return null;
        }

        @Override
        protected Type visitArithmeticUnary(ArithmeticUnary node, Void context)
        {
            Type sourceType = process(node.getBase());
            if (sourceType != null) {
                if (node.getSign() == PLUS) {
                    if (!isNumericType(sourceType)) {
                        throw semanticException(INVALID_PATH, pathNode, "Invalid operand type (%s) in JSON path arithmetic unary expression", sourceType.getDisplayName());
                    }
                    types.put(PathNodeRef.of(node), sourceType);
                    return sourceType;
                }
                Type resultType;
                try {
                    resultType = metadata.resolveOperator(session, NEGATION, ImmutableList.of(sourceType)).getSignature().getReturnType();
                }
                catch (OperatorNotFoundException e) {
                    throw semanticException(INVALID_PATH, pathNode, e, "invalid operand type (%s) in JSON path arithmetic unary expression: %s", sourceType.getDisplayName(), e.getMessage());
                }
                types.put(PathNodeRef.of(node), resultType);
                return resultType;
            }

            return null;
        }

        @Override
        protected Type visitArrayAccessor(ArrayAccessor node, Void context)
        {
            process(node.getBase());
            for (Subscript subscript : node.getSubscripts()) {
                process(subscript.getFrom());
                subscript.getTo().ifPresent(this::process);
            }

            return null;
        }

        @Override
        protected Type visitCeilingMethod(CeilingMethod node, Void context)
        {
            Type sourceType = process(node.getBase());
            if (sourceType != null) {
                Type resultType;
                try {
                    resultType = metadata.resolveFunction(session, QualifiedName.of("ceiling"), fromTypes(sourceType)).getSignature().getReturnType();
                }
                catch (TrinoException e) {
                    throw semanticException(INVALID_PATH, pathNode, e, "cannot perform JSON path ceiling() method with %s argument: %s", sourceType.getDisplayName(), e.getMessage());
                }
                types.put(PathNodeRef.of(node), resultType);
                return resultType;
            }

            return null;
        }

        @Override
        protected Type visitContextVariable(ContextVariable node, Void context)
        {
            return null;
        }

        @Override
        protected Type visitDatetimeMethod(DatetimeMethod node, Void context)
        {
            Type sourceType = process(node.getBase());
            if (sourceType != null && !isCharacterStringType(sourceType)) {
                throw semanticException(INVALID_PATH, pathNode, "JSON path datetime() method requires character string argument (found %s)", sourceType.getDisplayName());
            }
            // TODO process the format template, record the processed format, and deduce the returned type
            throw semanticException(NOT_SUPPORTED, pathNode, "datetime method in JSON path is not yet supported");
        }

        @Override
        protected Type visitDescendantMemberAccessor(DescendantMemberAccessor node, Void context)
        {
            process(node.getBase());
            return null;
        }

        @Override
        protected Type visitDoubleMethod(DoubleMethod node, Void context)
        {
            Type sourceType = process(node.getBase());
            if (sourceType != null) {
                if (!isStringType(sourceType) && !isNumericType(sourceType)) {
                    throw semanticException(INVALID_PATH, pathNode, "cannot perform JSON path double() method with %s argument", sourceType.getDisplayName());
                }
                try {
                    metadata.getCoercion(session, sourceType, DOUBLE);
                }
                catch (OperatorNotFoundException e) {
                    throw semanticException(INVALID_PATH, pathNode, e, "cannot perform JSON path double() method with %s argument: %s", sourceType.getDisplayName(), e.getMessage());
                }
            }

            types.put(PathNodeRef.of(node), DOUBLE);
            return DOUBLE;
        }

        @Override
        protected Type visitFilter(Filter node, Void context)
        {
            Type sourceType = process(node.getBase());
            Type predicateType = process(node.getPredicate());

            requireNonNull(predicateType, "missing type of predicate expression");
            checkState(predicateType.equals(BOOLEAN), "invalid type of predicate expression: " + predicateType.getDisplayName());

            if (sourceType != null) {
                types.put(PathNodeRef.of(node), sourceType);
                return sourceType;
            }

            return null;
        }

        @Override
        protected Type visitFloorMethod(FloorMethod node, Void context)
        {
            Type sourceType = process(node.getBase());
            if (sourceType != null) {
                Type resultType;
                try {
                    resultType = metadata.resolveFunction(session, QualifiedName.of("floor"), fromTypes(sourceType)).getSignature().getReturnType();
                }
                catch (TrinoException e) {
                    throw semanticException(INVALID_PATH, pathNode, e, "cannot perform JSON path floor() method with %s argument: %s", sourceType.getDisplayName(), e.getMessage());
                }
                types.put(PathNodeRef.of(node), resultType);
                return resultType;
            }

            return null;
        }

        @Override
        protected Type visitJsonNullLiteral(JsonNullLiteral node, Void context)
        {
            return null;
        }

        @Override
        protected Type visitJsonPath(JsonPath node, Void context)
        {
            Type type = process(node.getRoot());
            if (type != null) {
                types.put(PathNodeRef.of(node), type);
            }
            return type;
        }

        @Override
        protected Type visitKeyValueMethod(KeyValueMethod node, Void context)
        {
            process(node.getBase());
            return null;
        }

        @Override
        protected Type visitLastIndexVariable(LastIndexVariable node, Void context)
        {
            types.put(PathNodeRef.of(node), INTEGER);
            return INTEGER;
        }

        @Override
        protected Type visitMemberAccessor(MemberAccessor node, Void context)
        {
            process(node.getBase());
            return null;
        }

        @Override
        protected Type visitNamedVariable(NamedVariable node, Void context)
        {
            Type parameterType = parameterTypes.get(node.getName());
            if (parameterType == null) {
                // This condition might be caused by the unintuitive semantics:
                // identifiers in JSON path are case-sensitive, while non-delimited identifiers in SQL are upper-cased.
                // Hence, a function call like JSON_VALUE(x, 'lax $var.floor()` PASSING 2.5 AS var)
                // is an error, since the variable name is "var", and the passed parameter name is "VAR".
                // We try to identify such situation and produce an explanatory message.
                Optional<String> similarName = parameterTypes.keySet().stream()
                        .filter(name -> name.equalsIgnoreCase(node.getName()))
                        .findFirst();
                if (similarName.isPresent()) {
                    throw semanticException(INVALID_PATH, pathNode, format("no value passed for parameter %s. Try quoting \"%s\" in the PASSING clause to match case", node.getName(), node.getName()));
                }
                throw semanticException(INVALID_PATH, pathNode, "no value passed for parameter " + node.getName());
            }

            if (parameterType.equals(JSON_2016)) {
                jsonParameters.add(PathNodeRef.of(node));
                return null;
            }

            // in case of a non-JSON named variable, the type cannot be recorded and used as the result type of the node
            // this is because any incoming null value shall be transformed into a JSON null, which is out of the SQL type system.
            // however, for any incoming non-null value, the type will be preserved.
            return null;
        }

        @Override
        protected Type visitPredicateCurrentItemVariable(PredicateCurrentItemVariable node, Void context)
        {
            return null;
        }

        @Override
        protected Type visitSizeMethod(SizeMethod node, Void context)
        {
            process(node.getBase());
            types.put(PathNodeRef.of(node), INTEGER);
            return INTEGER;
        }

        @Override
        protected Type visitSqlValueLiteral(SqlValueLiteral node, Void context)
        {
            Type type = literalAnalyzer.analyze(node.getValue(), Scope.create());
            types.put(PathNodeRef.of(node), type);
            return type;
        }

        @Override
        protected Type visitTypeMethod(TypeMethod node, Void context)
        {
            process(node.getBase());
            Type type = TYPE_METHOD_RESULT_TYPE;
            types.put(PathNodeRef.of(node), type);
            return type;
        }

        // predicate

        @Override
        protected Type visitComparisonPredicate(ComparisonPredicate node, Void context)
        {
            process(node.getLeft());
            process(node.getRight());
            types.put(PathNodeRef.of(node), BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitConjunctionPredicate(ConjunctionPredicate node, Void context)
        {
            Type leftType = process(node.getLeft());
            requireNonNull(leftType, "missing type of predicate expression");
            checkState(leftType.equals(BOOLEAN), "invalid type of predicate expression: " + leftType.getDisplayName());

            Type rightType = process(node.getRight());
            requireNonNull(rightType, "missing type of predicate expression");
            checkState(rightType.equals(BOOLEAN), "invalid type of predicate expression: " + rightType.getDisplayName());

            types.put(PathNodeRef.of(node), BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitDisjunctionPredicate(DisjunctionPredicate node, Void context)
        {
            Type leftType = process(node.getLeft());
            requireNonNull(leftType, "missing type of predicate expression");
            checkState(leftType.equals(BOOLEAN), "invalid type of predicate expression: " + leftType.getDisplayName());

            Type rightType = process(node.getRight());
            requireNonNull(rightType, "missing type of predicate expression");
            checkState(rightType.equals(BOOLEAN), "invalid type of predicate expression: " + rightType.getDisplayName());

            types.put(PathNodeRef.of(node), BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitExistsPredicate(ExistsPredicate node, Void context)
        {
            process(node.getPath());
            types.put(PathNodeRef.of(node), BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitLikeRegexPredicate(LikeRegexPredicate node, Void context)
        {
            throw semanticException(NOT_SUPPORTED, pathNode, "like_regex predicate in JSON path is not yet supported");
            // TODO when like_regex is supported, this method should do the following:
            // process(node.getPath());
            // types.put(PathNodeRef.of(node), BOOLEAN);
            // return BOOLEAN;
        }

        @Override
        protected Type visitNegationPredicate(NegationPredicate node, Void context)
        {
            Type predicateType = process(node.getPredicate());
            requireNonNull(predicateType, "missing type of predicate expression");
            checkState(predicateType.equals(BOOLEAN), "invalid type of predicate expression: " + predicateType.getDisplayName());

            types.put(PathNodeRef.of(node), BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitStartsWithPredicate(StartsWithPredicate node, Void context)
        {
            process(node.getWhole());
            process(node.getInitial());
            types.put(PathNodeRef.of(node), BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitIsUnknownPredicate(IsUnknownPredicate node, Void context)
        {
            Type predicateType = process(node.getPredicate());
            requireNonNull(predicateType, "missing type of predicate expression");
            checkState(predicateType.equals(BOOLEAN), "invalid type of predicate expression: " + predicateType.getDisplayName());

            types.put(PathNodeRef.of(node), BOOLEAN);
            return BOOLEAN;
        }
    }

    public static class JsonPathAnalysis
    {
        private final JsonPath path;
        private final Map<PathNodeRef<PathNode>, Type> types;
        private final Set<PathNodeRef<PathNode>> jsonParameters;

        public JsonPathAnalysis(JsonPath path, Map<PathNodeRef<PathNode>, Type> types, Set<PathNodeRef<PathNode>> jsonParameters)
        {
            this.path = requireNonNull(path, "path is null");
            this.types = ImmutableMap.copyOf(requireNonNull(types, "types is null"));
            this.jsonParameters = ImmutableSet.copyOf(requireNonNull(jsonParameters, "jsonParameters is null"));
        }

        public JsonPath getPath()
        {
            return path;
        }

        public Type getType(PathNode pathNode)
        {
            return types.get(PathNodeRef.of(pathNode));
        }

        public Map<PathNodeRef<PathNode>, Type> getTypes()
        {
            return types;
        }

        public Set<PathNodeRef<PathNode>> getJsonParameters()
        {
            return jsonParameters;
        }
    }
}
