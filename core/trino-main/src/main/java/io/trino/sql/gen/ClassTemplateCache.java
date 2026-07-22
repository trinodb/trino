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
package io.trino.sql.gen;

import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.bytecode.ClassDefinition;
import io.trino.cache.CacheStatsMBean;
import io.trino.cache.NonEvictableCache;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.DefaultTraversalVisitor;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionRewriter;
import io.trino.sql.ir.ExpressionTreeRewriter;
import jakarta.annotation.Nullable;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.sql.gen.BytecodeUtils.loadConstant;
import static io.trino.util.CompilerUtils.defineHiddenClass;
import static io.trino.util.CompilerUtils.defineHiddenClassFromBytes;
import static io.trino.util.CompilerUtils.generateHiddenClassBytes;
import static io.trino.util.CompilerUtils.isClassDumpEnabled;
import static java.util.Objects.requireNonNull;

/**
 * Caches generated class bytes keyed on the expression structure with constant values
 * stripped. All non-boolean constants live in the class data, so structurally identical
 * expressions with different literals share one compiled template: a cache hit assembles
 * fresh class data from the current literals and defines the cached bytes directly,
 * skipping expression compilation and bytecode serialization entirely.
 *
 * <p>Compilations whose bytecode or class data derives from constant values (rather than
 * holding the literal value objects themselves) must call
 * {@link CallSiteBinder#markValueDependent()} and are never templated.
 */
public final class ClassTemplateCache<T>
{
    private final Class<T> superType;
    @Nullable
    private final NonEvictableCache<TemplateKey, ClassTemplate> cache;
    @Nullable
    private final CacheStatsMBean cacheStats;

    private record TemplateKey(Expression normalized, List<Boolean> nonNullLiterals, List<?> extra) {}

    private record ClassTemplate(byte[] bytecode, List<SlotRecipe> recipe) {}

    private sealed interface SlotRecipe {}

    /**
     * A class data slot holding a structural value: a method handle, a type, or another
     * constant that is not a literal of the expression.
     */
    private record FixedSlot(Object value)
            implements SlotRecipe {}

    /**
     * A class data slot holding the value of the expression literals at the given ordinals.
     * Multiple ordinals appear when equal literals were deduplicated into one binding, in
     * which case a template use requires the literals at all ordinals to still be equal.
     */
    private record LiteralSlot(int[] ordinals)
            implements SlotRecipe {}

    /**
     * The class data slot holding the generated toString description. The description
     * describes the literal values, so every template use takes a description of the
     * expression at hand instead of replaying the one from the first compilation.
     */
    private record DescriptionSlot()
            implements SlotRecipe {}

    public ClassTemplateCache(Class<T> superType, int cacheSize)
    {
        this.superType = requireNonNull(superType, "superType is null");
        if (cacheSize > 0) {
            cache = buildNonEvictableCache(
                    CacheBuilder.newBuilder()
                            .recordStats()
                            .maximumSize(cacheSize));
            cacheStats = new CacheStatsMBean(cache);
        }
        else {
            cache = null;
            cacheStats = null;
        }
    }

    @Nullable
    public CacheStatsMBean getStats()
    {
        return cacheStats;
    }

    /**
     * Defines a class for the given expression, reusing a cached template when a class for
     * a structurally identical expression was already generated. The generator receives a
     * fresh binder and is only invoked on a template miss. A toString method returning a
     * description of the expression is added to every class, so the generator must not
     * declare its own.
     *
     * @param expression the expression the class is generated from; the literal value
     *         instances of this expression must be exactly the objects the generator binds
     * @param extraKey non-expression inputs that affect the generated bytecode
     */
    public Class<? extends T> defineClass(Expression expression, List<?> extraKey, Function<CallSiteBinder, ClassDefinition> generator)
    {
        Description description = new Description(superType, expression, extraKey);

        // dumped classes retain debug attributes and unique names, which templates skip
        if (cache == null || isClassDumpEnabled()) {
            CallSiteBinder callSiteBinder = new CallSiteBinder();
            ClassDefinition classDefinition = generator.apply(callSiteBinder);
            generateToString(classDefinition, callSiteBinder.bind(description, Object.class));
            return defineHiddenClass(classDefinition, superType, callSiteBinder.getClassData());
        }

        TemplateKey templateKey = templateKey(expression, extraKey);
        ClassTemplate template = cache.getIfPresent(templateKey);
        if (template != null) {
            Optional<List<Object>> classData = assembleClassData(template.recipe(), expression, description);
            if (classData.isPresent()) {
                return defineHiddenClassFromBytes(template.bytecode(), superType, classData.get());
            }
        }

        CallSiteBinder callSiteBinder = new CallSiteBinder();
        ClassDefinition classDefinition = generator.apply(callSiteBinder);
        // bound after the generator, so the description slot is identified by its binding id
        Binding descriptionBinding = callSiteBinder.bind(description, Object.class);
        generateToString(classDefinition, descriptionBinding);
        byte[] bytecode = generateHiddenClassBytes(classDefinition);
        List<Object> classData = callSiteBinder.getClassData();
        Class<? extends T> clazz = defineHiddenClassFromBytes(bytecode, superType, classData);
        if (!callSiteBinder.isValueDependent()) {
            cache.put(templateKey, new ClassTemplate(bytecode, buildRecipe(classData, expression, descriptionBinding.getBindingId())));
        }
        return clazz;
    }

    /**
     * The generated toString description: the super type, the expression with its literal
     * values, and any extra key. Instances of the generated hidden class describe
     * themselves with it in debuggers and logs.
     *
     * <p>The description is rendered on first use rather than on compilation. Rendering an
     * expression renders its constants, and rendering a constant materializes its value as
     * a single position block, so a compilation that describes itself eagerly pays for every
     * literal it holds. Nothing asks a generated instance to describe itself unless a
     * debugger or a log line does, so that cost is not paid at all in a running engine.
     */
    private static final class Description
    {
        private final Class<?> superType;
        private final Expression expression;
        private final List<?> extra;
        @Nullable
        private String description;

        private Description(Class<?> superType, Expression expression, List<?> extra)
        {
            this.superType = superType;
            this.expression = expression;
            this.extra = extra;
        }

        // races produce equal strings, so the result is not published through a volatile
        @Override
        public String toString()
        {
            String result = description;
            if (result == null) {
                String string = expression.toString();
                if (string.length() > 1000) {
                    string = string.substring(0, 1000) + "...";
                }
                result = superType.getSimpleName() + "{" + string + (extra.isEmpty() ? "" : ", " + extra) + "}";
                description = result;
            }
            return result;
        }
    }

    private static void generateToString(ClassDefinition classDefinition, Binding descriptionBinding)
    {
        classDefinition.declareMethod(a(PUBLIC), "toString", type(String.class))
                .getBody()
                .append(loadConstant(descriptionBinding))
                .invokeVirtual(Object.class, "toString", String.class)
                .retObject();
    }

    /**
     * The template key is the expression with the values of all parameterizable constants
     * stripped. Boolean constants stay in the key because they compile to bytecode
     * constants, and nullness stays in the key because null constants compile to a
     * different shape than bound constants.
     */
    private static TemplateKey templateKey(Expression expression, List<?> extra)
    {
        ImmutableList.Builder<Boolean> nonNullLiterals = ImmutableList.builder();
        new DefaultTraversalVisitor<Void>()
        {
            @Override
            protected Void visitConstant(Constant node, Void context)
            {
                if (node.type().getJavaType() != boolean.class) {
                    nonNullLiterals.add(node.value() != null);
                }
                return null;
            }
        }.process(expression, null);
        return new TemplateKey(stripConstants(expression), nonNullLiterals.build(), extra);
    }

    /**
     * Rewrites the values of all parameterizable constants to null, producing the
     * structure all expressions sharing a template have in common.
     */
    private static Expression stripConstants(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteConstant(Constant node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                if (node.type().getJavaType() == boolean.class || node.value() == null) {
                    return node;
                }
                return new Constant(node.type(), null);
            }
        }, expression);
    }

    private static List<Constant> parameterizableLiterals(Expression expression)
    {
        ImmutableList.Builder<Constant> literals = ImmutableList.builder();
        new DefaultTraversalVisitor<Void>()
        {
            @Override
            protected Void visitConstant(Constant node, Void context)
            {
                if (node.type().getJavaType() != boolean.class && node.value() != null) {
                    literals.add(node);
                }
                return null;
            }
        }.process(expression, null);
        return literals.build();
    }

    private static List<SlotRecipe> buildRecipe(List<Object> classData, Expression expression, long descriptionBindingId)
    {
        List<Constant> literals = parameterizableLiterals(expression);
        // literal values are matched by identity: this is exactly how the binder deduplicated
        // them, so a class data slot holding a literal value maps back to its ordinals
        Map<Object, List<Integer>> literalOrdinals = new IdentityHashMap<>();
        for (int ordinal = 0; ordinal < literals.size(); ordinal++) {
            literalOrdinals.computeIfAbsent(literals.get(ordinal).value(), _ -> new ArrayList<>()).add(ordinal);
        }

        ImmutableList.Builder<SlotRecipe> recipe = ImmutableList.builder();
        for (int slot = 0; slot < classData.size(); slot++) {
            if (slot == descriptionBindingId) {
                recipe.add(new DescriptionSlot());
                continue;
            }
            List<Integer> ordinals = literalOrdinals.get(classData.get(slot));
            if (ordinals != null) {
                recipe.add(new LiteralSlot(Ints.toArray(ordinals)));
            }
            else {
                recipe.add(new FixedSlot(classData.get(slot)));
            }
        }
        return recipe.build();
    }

    private static Optional<List<Object>> assembleClassData(List<SlotRecipe> recipe, Expression expression, Description description)
    {
        List<Constant> literals = parameterizableLiterals(expression);
        ImmutableList.Builder<Object> classData = ImmutableList.builder();
        for (SlotRecipe slot : recipe) {
            switch (slot) {
                case FixedSlot fixed -> classData.add(fixed.value());
                case DescriptionSlot _ -> classData.add(description);
                case LiteralSlot literal -> {
                    Object value = literals.get(literal.ordinals()[0]).value();
                    for (int ordinal : literal.ordinals()) {
                        // equal literals shared this slot in the template; if the literals
                        // differ now, the template does not fit and a fresh compilation is needed
                        if (!Objects.equals(literals.get(ordinal).value(), value)) {
                            return Optional.empty();
                        }
                    }
                    classData.add(value);
                }
            }
        }
        return Optional.of(classData.build());
    }
}
