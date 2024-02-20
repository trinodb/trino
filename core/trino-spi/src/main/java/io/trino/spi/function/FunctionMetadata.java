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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.DoNotCall;
import io.trino.spi.Experimental;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.trino.spi.function.FunctionKind.AGGREGATE;
import static io.trino.spi.function.FunctionKind.SCALAR;
import static io.trino.spi.function.FunctionKind.WINDOW;
import static java.util.Objects.requireNonNull;

@Experimental(eta = "2022-10-31")
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
    private final String description;
    private final FunctionKind kind;
    private final boolean deprecated;

    private FunctionMetadata(
            FunctionId functionId,
            Signature signature,
            String canonicalName,
            Set<String> names,
            FunctionNullability functionNullability,
            boolean hidden,
            boolean deterministic,
            String description,
            FunctionKind kind,
            boolean deprecated)
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
        this.description = requireNonNull(description, "description is null");
        this.kind = requireNonNull(kind, "kind is null");
        this.deprecated = deprecated;
    }

    /**
     * Unique id of this function.
     */
    @JsonProperty
    public FunctionId getFunctionId()
    {
        return functionId;
    }

    /**
     * Signature of a matching call site.
     */
    @JsonProperty
    public Signature getSignature()
    {
        return signature;
    }

    /**
     * The canonical name of the function.
     */
    @JsonProperty
    public String getCanonicalName()
    {
        return canonicalName;
    }

    /**
     * Canonical name and any aliases.
     */
    @JsonProperty
    public Set<String> getNames()
    {
        return names;
    }

    @JsonProperty
    public FunctionNullability getFunctionNullability()
    {
        return functionNullability;
    }

    @JsonProperty
    public boolean isHidden()
    {
        return hidden;
    }

    @JsonProperty
    public boolean isDeterministic()
    {
        return deterministic;
    }

    @JsonProperty
    public String getDescription()
    {
        return description;
    }

    @JsonProperty
    public FunctionKind getKind()
    {
        return kind;
    }

    @JsonProperty
    public boolean isDeprecated()
    {
        return deprecated;
    }

    @JsonCreator
    @DoNotCall // For JSON deserialization only
    public static FunctionMetadata fromJson(
            @JsonProperty FunctionId functionId,
            @JsonProperty Signature signature,
            @JsonProperty String canonicalName,
            @JsonProperty Set<String> names,
            @JsonProperty FunctionNullability functionNullability,
            @JsonProperty boolean hidden,
            @JsonProperty boolean deterministic,
            @JsonProperty String description,
            @JsonProperty FunctionKind kind,
            @JsonProperty boolean deprecated)
    {
        return new FunctionMetadata(
                functionId,
                signature,
                canonicalName,
                names,
                functionNullability,
                hidden,
                deterministic,
                description,
                kind,
                deprecated);
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
        private String description;
        private FunctionId functionId;
        private boolean deprecated;

        private Builder(String canonicalName, FunctionKind kind)
        {
            this.canonicalName = requireNonNull(canonicalName, "canonicalName is null");
            names.add(canonicalName);
            if (canonicalName.startsWith(OPERATOR_PREFIX)) {
                hidden = true;
                description = "";
            }
            this.kind = kind;
            if (kind == AGGREGATE || kind == WINDOW) {
                nullable = true;
            }
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

        public Builder noDescription()
        {
            this.description = "";
            return this;
        }

        public Builder description(String description)
        {
            requireNonNull(description, "description is null");
            if (description.isBlank()) {
                throw new IllegalArgumentException("description is blank");
            }
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

        public FunctionMetadata build()
        {
            FunctionId functionId = this.functionId;
            if (functionId == null) {
                functionId = FunctionId.toFunctionId(canonicalName, signature);
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
                    description,
                    kind,
                    deprecated);
        }
    }
}
