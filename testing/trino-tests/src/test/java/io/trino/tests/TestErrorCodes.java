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
package io.trino.tests;

import com.google.common.collect.ImmutableSet;
import com.sun.source.tree.AssignmentTree;
import com.sun.source.tree.BinaryTree;
import com.sun.source.tree.ClassTree;
import com.sun.source.tree.CompilationUnitTree;
import com.sun.source.tree.ExpressionStatementTree;
import com.sun.source.tree.ExpressionTree;
import com.sun.source.tree.LiteralTree;
import com.sun.source.tree.MethodTree;
import com.sun.source.tree.NewClassTree;
import com.sun.source.tree.ReturnTree;
import com.sun.source.tree.Tree;
import com.sun.source.tree.VariableTree;
import com.sun.source.util.JavacTask;
import com.sun.source.util.TreeScanner;
import org.junit.jupiter.api.Test;

import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

import static com.sun.source.tree.Tree.Kind.ENUM;
import static com.sun.source.tree.Tree.Kind.PLUS;
import static javax.lang.model.element.Modifier.FINAL;
import static javax.lang.model.element.Modifier.STATIC;
import static javax.tools.Diagnostic.Kind.ERROR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestErrorCodes
{
    // These libraries deliberately expose the same errors as the Hive connector.
    private static final Set<Set<String>> SHARED_ERROR_CODES = ImmutableSet.of(
            ImmutableSet.of(
                    "io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA",
                    "io.trino.hive.formats.HiveFormatsErrorCode.HIVE_INVALID_METADATA",
                    "io.trino.metastore.MetastoreErrorCode.HIVE_INVALID_METADATA"),
            ImmutableSet.of(
                    "io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT",
                    "io.trino.metastore.MetastoreErrorCode.HIVE_UNSUPPORTED_FORMAT"),
            ImmutableSet.of(
                    "io.trino.plugin.hive.HiveErrorCode.HIVE_UNSERIALIZABLE_JSON_VALUE",
                    "io.trino.hive.formats.HiveFormatsErrorCode.HIVE_UNSERIALIZABLE_JSON_VALUE"));

    @Test
    public void testUniqueErrorCodes()
            throws Exception
    {
        // Parse sources so every connector is checked without adding connector dependencies.
        // Only literal offsets and constructors that add a literal base are supported. Fail on
        // other forms instead of silently computing a value different from the runtime code.
        List<Path> sources = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(findRepositoryRoot())) {
            for (Path path : paths.filter(path -> path.toString().contains("/src/main/java/") && path.toString().endsWith(".java")).toList()) {
                if (Files.readString(path).contains("ErrorCodeSupplier")) {
                    sources.add(path);
                }
            }
        }
        assertThat(sources).isNotEmpty();

        Map<Integer, Set<String>> codes = new TreeMap<>();
        Map<String, String> sharedTypes = new TreeMap<>();
        var compiler = ToolProvider.getSystemJavaCompiler();
        assertThat(compiler).as("A JDK is required to parse error code suppliers").isNotNull();
        var diagnostics = new DiagnosticCollector<JavaFileObject>();
        try (var fileManager = compiler.getStandardFileManager(diagnostics, null, null)) {
            var task = (JavacTask) compiler.getTask(null, fileManager, diagnostics, List.of("-proc:none"), null, fileManager.getJavaFileObjectsFromPaths(sources));
            for (CompilationUnitTree unit : task.parse()) {
                new TreeScanner<Void, String>()
                {
                    @Override
                    public Void visitClass(ClassTree node, String enclosingName)
                    {
                        String className = enclosingName + "." + node.getSimpleName();
                        if (node.getImplementsClause().stream().anyMatch(type -> type.toString().equals("ErrorCodeSupplier") || type.toString().equals("io.trino.spi.ErrorCodeSupplier"))) {
                            readErrorCodes(node, className, codes, sharedTypes);
                        }
                        return super.visitClass(node, className);
                    }
                }.scan(unit, unit.getPackageName().toString());
            }
        }
        assertThat(diagnostics.getDiagnostics()).filteredOn(diagnostic -> diagnostic.getKind() == ERROR).isEmpty();
        assertThat(codes).isNotEmpty();
        codes.forEach((code, definitions) -> {
            if (definitions.size() > 1) {
                assertThat(SHARED_ERROR_CODES)
                        .as("Duplicate error code 0x%08x: %s", code, definitions)
                        .contains(definitions);
                assertThat(definitions.stream().map(sharedTypes::get).distinct())
                        .as("Shared error types for %s", definitions)
                        .singleElement().isNotNull();
            }
        });
    }

    private static void readErrorCodes(ClassTree supplier, String className, Map<Integer, Set<String>> codes, Map<String, String> sharedTypes)
    {
        assertThat(supplier.getKind()).as("Error code supplier %s", className).isEqualTo(ENUM);
        assertThat(supplier.getMembers().stream()
                .filter(VariableTree.class::isInstance)
                .map(VariableTree.class::cast)
                .filter(variable -> variable.getName().contentEquals("errorCode")))
                .singleElement()
                .satisfies(field -> assertThat(field.getModifiers().getFlags()).contains(FINAL).doesNotContain(STATIC));
        List<MethodTree> methods = supplier.getMembers().stream()
                .filter(MethodTree.class::isInstance)
                .map(MethodTree.class::cast)
                .toList();
        List<Integer> bases = methods.stream()
                .filter(method -> method.getReturnType() == null)
                .map(method -> readBase(method, className))
                .distinct()
                .toList();
        assertThat(bases).as("All constructors of %s must use the same error code base", className).hasSize(1);
        assertThat(methods.stream().filter(method -> method.getName().contentEquals("toErrorCode")))
                .singleElement()
                .satisfies(method -> {
                    assertThat(method.getBody().getStatements()).as("toErrorCode in %s", className).hasSize(1);
                    Tree statement = method.getBody().getStatements().getFirst();
                    if (!(statement instanceof ReturnTree returned) || !returned.getExpression().toString().equals("errorCode")) {
                        throw new AssertionError("Unsupported toErrorCode in " + className + ": " + statement);
                    }
                });

        List<VariableTree> constants = supplier.getMembers().stream()
                .filter(VariableTree.class::isInstance)
                .map(VariableTree.class::cast)
                .filter(variable -> variable.getType().toString().equals(supplier.getSimpleName().toString()))
                .toList();
        assertThat(constants).as("Error code constants in %s", className).isNotEmpty();
        for (VariableTree constant : constants) {
            String name = className + "." + constant.getName();
            if (!(constant.getInitializer() instanceof NewClassTree initializer) || initializer.getClassBody() != null) {
                throw new AssertionError("Unsupported error code constant " + name + ": " + constant);
            }
            long code = (long) bases.getFirst() + readInteger(initializer.getArguments().getFirst(), name);
            assertThat(code).as("Error code for %s", name).isBetween(0L, (long) Integer.MAX_VALUE);
            codes.computeIfAbsent((int) code, _ -> new TreeSet<>()).add(name);
            if (SHARED_ERROR_CODES.stream().anyMatch(aliases -> aliases.contains(name))) {
                String type = initializer.getArguments().get(1).toString();
                assertThat(type).as("Shared error type for %s", name).isIn("USER_ERROR", "EXTERNAL");
                sharedTypes.put(name, type);
            }
        }
    }

    private static int readBase(MethodTree constructor, String className)
    {
        assertThat(constructor.getParameters()).as("Constructor parameters in %s", className).isNotEmpty();
        VariableTree parameter = constructor.getParameters().getFirst();
        assertThat(parameter.getType().toString()).as("Error code parameter in %s", className).isEqualTo("int");
        assertThat(constructor.getBody().getStatements()).as("Constructor body in %s", className).hasSize(1);
        Tree statement = constructor.getBody().getStatements().getFirst();
        if (!(statement instanceof ExpressionStatementTree expression &&
                expression.getExpression() instanceof AssignmentTree assignment &&
                assignment.getVariable().toString().equals("errorCode") &&
                assignment.getExpression() instanceof NewClassTree allocation &&
                (allocation.getIdentifier().toString().equals("ErrorCode") || allocation.getIdentifier().toString().equals("io.trino.spi.ErrorCode")))) {
            throw new AssertionError("Unsupported error code constructor in " + className + ": " + constructor);
        }
        if (SHARED_ERROR_CODES.stream().flatMap(Set::stream).anyMatch(name -> name.startsWith(className + "."))) {
            // Matching types imply matching fatal flags only with the default ErrorCode constructor.
            assertThat(constructor.getParameters()).hasSize(2);
            assertThat(allocation.getArguments()).hasSize(3);
            assertThat(allocation.getArguments().get(1).toString()).isEqualTo("name()");
            assertThat(allocation.getArguments().get(2).toString()).isEqualTo(constructor.getParameters().get(1).getName().toString());
        }
        ExpressionTree code = allocation.getArguments().getFirst();
        if (code.toString().equals(parameter.getName().toString())) {
            return 0;
        }
        if (code instanceof BinaryTree binary && binary.getKind() == PLUS && binary.getLeftOperand().toString().equals(parameter.getName().toString())) {
            return readInteger(binary.getRightOperand(), className);
        }
        throw new AssertionError("Unsupported error code expression in " + className + ": " + code);
    }

    private static int readInteger(ExpressionTree expression, String name)
    {
        if (expression instanceof LiteralTree literal && literal.getValue() instanceof Integer value) {
            return value;
        }
        throw new AssertionError("Expected an integer literal in " + name + ": " + expression);
    }

    private static Path findRepositoryRoot()
    {
        Path workingDirectory = Path.of("").toAbsolutePath();
        for (Path path = workingDirectory; path != null; path = path.getParent()) {
            if (Files.exists(path.resolve(".git"))) {
                return path;
            }
        }
        throw new IllegalStateException("Failed to find repository root from " + workingDirectory);
    }
}
