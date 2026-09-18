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
package io.trino.spi.function;

import io.trino.spi.type.TypeTemplate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import static io.trino.spi.function.FunctionKind.AGGREGATE;
import static io.trino.spi.function.FunctionKind.SCALAR;
import static io.trino.spi.function.FunctionKind.TABLE;
import static io.trino.spi.function.FunctionKind.WINDOW;
import static java.util.Objects.requireNonNull;

public class FunctionMetadata
{
    // Copied from OperatorNameUtil
    private static final String OPERATOR_PREFIX = "$operator$";

    private final FunctionId functionId;
    private final Signature signature;
    private final String canonicalName;
    private final Set<String> names;
    private final FunctionNullability functionNullability;
    private final boolean hidden;
    private final boolean deterministic;
    private final Predicate<BoundSignature> neverFails;
    private final String description;
    private final FunctionKind kind;
    private final boolean deprecated;
    private final Optional<TypeTemplate> receiverType;
    private final boolean instanceMethod;

    private FunctionMetadata(
            FunctionId functionId,
            Signature signature,
            String canonicalName,
            Set<String> names,
            FunctionNullability functionNullability,
            boolean hidden,
            boolean deterministic,
            Predicate<BoundSignature> neverFails,
            String description,
            FunctionKind kind,
            boolean deprecated,
            Optional<TypeTemplate> receiverType,
            boolean instanceMethod)
    {
        this.functionId = requireNonNull(functionId, "functionId is null");
        this.signature = requireNonNull(signature, "signature is null");
        this.canonicalName = requireNonNull(canonicalName, "canonicalName is null");
        this.names = Set.copyOf(names);
        if (!names.contains(canonicalName)) {
            throw new IllegalArgumentException("names must contain the canonical name");
        }
        this.functionNullability = requireNonNull(functionNullability, "functionNullability is null");
        if (functionNullability.getArgumentNullable().size() != signature.getArgumentTypes().size()) {
            throw new IllegalArgumentException("signature and functionNullability must have same argument count");
        }

        this.hidden = hidden;
        this.deterministic = deterministic;
        this.neverFails = requireNonNull(neverFails, "neverFails is null");
        this.description = requireNonNull(description, "description is null");
        this.kind = requireNonNull(kind, "kind is null");
        this.deprecated = deprecated;
        this.receiverType = requireNonNull(receiverType, "receiverType is null");
        if (instanceMethod && receiverType.isEmpty()) {
            throw new IllegalArgumentException("instance method must have a receiver type");
        }
        this.instanceMethod = instanceMethod;
    }

    /**
     * Unique id of this function.
     */
    public FunctionId getFunctionId()
    {
        return functionId;
    }

    /**
     * Signature of a matching call site.
     */
    public Signature getSignature()
    {
        return signature;
    }

    /**
     * The canonical name of the function.
     */
    public String getCanonicalName()
    {
        return canonicalName;
    }

    /**
     * Canonical name and any aliases.
     */
    public Set<String> getNames()
    {
        return names;
    }

    public FunctionNullability getFunctionNullability()
    {
        return functionNullability;
    }

    public boolean isHidden()
    {
        return hidden;
    }

    public boolean isDeterministic()
    {
        return deterministic;
    }

    /**
     * Predicate that returns whether this function never fails for the
     * given bound signature (call site).
     */
    public Predicate<BoundSignature> getNeverFails()
    {
        return neverFails;
    }

    public String getDescription()
    {
        return description;
    }

    public FunctionKind getKind()
    {
        return kind;
    }

    public boolean isDeprecated()
    {
        return deprecated;
    }

    /**
     * The receiver type when this function is a method. For a static method
     * (invocable as {@code T::method(args)}) this is the named type. For an
     * instance method (invocable as {@code receiver.method(args)}) this is
     * the type of the {@code self} parameter (the first declared argument).
     * Empty for regular functions.
     */
    public Optional<TypeTemplate> getReceiverType()
    {
        return receiverType;
    }

    /**
     * Whether this is a method -- an instance or static method invoked through
     * {@code receiver.method(args)} or {@code type::method(args)} -- rather than a
     * plain function. Equivalent to {@link #getReceiverType()} being present.
     */
    public boolean isMethod()
    {
        return receiverType.isPresent();
    }

    /**
     * Whether this is an instance method (receiver passed as the first
     * argument) rather than a static method. Only meaningful when
     * {@link #getReceiverType()} is present.
     */
    public boolean isInstanceMethod()
    {
        return instanceMethod;
    }

    @Override
    public String toString()
    {
        return signature.toString();
    }

    public static Builder scalarBuilder(String canonicalName)
    {
        return builder(canonicalName, SCALAR);
    }

    public static Builder operatorBuilder(OperatorType operatorType)
    {
        String name = OPERATOR_PREFIX + requireNonNull(operatorType, "operatorType is null").name();
        return builder(name, SCALAR);
    }

    public static Builder aggregateBuilder(String canonicalName)
    {
        return builder(canonicalName, AGGREGATE);
    }

    public static Builder windowBuilder(String canonicalName)
    {
        return builder(canonicalName, WINDOW);
    }

    public static Builder tableBuilder(String canonicalName)
    {
        return builder(canonicalName, TABLE);
    }

    public static Builder builder(String canonicalName, FunctionKind functionKind)
    {
        return new Builder(canonicalName, functionKind);
    }

    public static final class Builder
    {
        private final String canonicalName;
        private final FunctionKind kind;
        private Signature signature;
        private final Set<String> names = new HashSet<>();
        private boolean nullable;
        private List<Boolean> argumentNullability;
        private boolean hidden;
        private boolean deterministic = true;
        private Predicate<BoundSignature> neverFails = _ -> false;
        private String description;
        private FunctionId functionId;
        private boolean deprecated;
        private Optional<TypeTemplate> receiverType = Optional.empty();
        private boolean instanceMethod;

        private Builder(String canonicalName, FunctionKind kind)
        {
            this.canonicalName = requireNonNull(canonicalName, "canonicalName is null");
            names.add(canonicalName);
            if (canonicalName.startsWith(OPERATOR_PREFIX)) {
                hidden = true;
                description = "";
            }
            this.kind = kind;
        }

        public Builder signature(Signature signature)
        {
            this.signature = signature;
            return this;
        }

        public Builder alias(String alias)
        {
            names.add(alias);
            return this;
        }

        public Builder nullable()
        {
            this.nullable = true;
            return this;
        }

        public Builder argumentNullability(boolean... argumentNullability)
        {
            requireNonNull(argumentNullability, "argumentNullability is null");
            List<Boolean> list = new ArrayList<>(argumentNullability.length);
            for (boolean nullability : argumentNullability) {
                list.add(nullability);
            }
            return argumentNullability(list);
        }

        public Builder argumentNullability(List<Boolean> argumentNullability)
        {
            this.argumentNullability = List.copyOf(requireNonNull(argumentNullability, "argumentNullability is null"));
            return this;
        }

        public Builder hidden()
        {
            this.hidden = true;
            if (description == null) {
                description = "";
            }
            return this;
        }

        public Builder nondeterministic()
        {
            this.deterministic = false;
            return this;
        }

        public Builder neverFails()
        {
            return neverFails(_ -> true);
        }

        public Builder neverFails(Predicate<BoundSignature> neverFails)
        {
            this.neverFails = requireNonNull(neverFails, "neverFails is null");
            return this;
        }

        /**
         * @deprecated Use {@link #description(String)} with an empty string.
         */
        @Deprecated
        public Builder noDescription()
        {
            this.description = "";
            return this;
        }

        public Builder description(String description)
        {
            requireNonNull(description, "description is null");
            this.description = description;
            return this;
        }

        public Builder functionId(FunctionId functionId)
        {
            this.functionId = functionId;
            return this;
        }

        public Builder deprecated()
        {
            this.deprecated = true;
            return this;
        }

        public Builder receiverType(TypeTemplate receiverType)
        {
            this.receiverType = Optional.of(requireNonNull(receiverType, "receiverType is null"));
            return this;
        }

        public Builder instanceMethod()
        {
            this.instanceMethod = true;
            return this;
        }

        public FunctionMetadata build()
        {
            FunctionId functionId = this.functionId;
            if (functionId == null) {
                functionId = FunctionId.toFunctionId(canonicalName, signature, receiverType);
            }
            if (argumentNullability == null) {
                argumentNullability = Collections.nCopies(signature.getArgumentTypes().size(), kind == WINDOW);
            }
            return new FunctionMetadata(
                    functionId,
                    signature,
                    canonicalName,
                    names,
                    new FunctionNullability(nullable, argumentNullability),
                    hidden,
                    deterministic,
                    neverFails,
                    description,
                    kind,
                    deprecated,
                    receiverType,
                    instanceMethod);
        }
    }
}
