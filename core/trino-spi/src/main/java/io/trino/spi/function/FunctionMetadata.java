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

import io.trino.spi.Experimental;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.trino.spi.function.FunctionKind.AGGREGATE;
import static io.trino.spi.function.FunctionKind.SCALAR;
import static io.trino.spi.function.FunctionKind.WINDOW;
import static java.util.Objects.requireNonNull;

@Experimental(eta = "2022-10-31")
public class FunctionMetadata
{
    private final FunctionId functionId;
    private final Signature signature;
    private final String canonicalName;
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
     * For aliased functions, each alias must have a different alias.
     */
    public FunctionId getFunctionId()
    {
        return functionId;
    }

    /**
     * Signature of a matching call site.
     * For aliased functions, the signature must use the alias name.
     */
    public Signature getSignature()
    {
        return signature;
    }

    /**
     * For aliased functions, the canonical name of the function.
     */
    public String getCanonicalName()
    {
        return canonicalName;
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

    @Override
    public String toString()
    {
        return signature.toString();
    }

    public static Builder scalarBuilder()
    {
        return builder(SCALAR);
    }

    public static Builder aggregateBuilder()
    {
        return builder(AGGREGATE);
    }

    public static Builder windowBuilder()
    {
        return builder(WINDOW);
    }

    public static Builder builder(FunctionKind functionKind)
    {
        return new Builder(functionKind);
    }

    public static final class Builder
    {
        private final FunctionKind kind;
        private Signature signature;
        private String canonicalName;
        private boolean nullable;
        private List<Boolean> argumentNullability;
        private boolean hidden;
        private boolean deterministic = true;
        private String description;
        private FunctionId functionId;
        private boolean deprecated;

        private Builder(FunctionKind kind)
        {
            this.kind = kind;
            if (kind == AGGREGATE || kind == WINDOW) {
                nullable = true;
            }
        }

        public Builder signature(Signature signature)
        {
            this.signature = signature;
            if (signature.isOperator()) {
                hidden = true;
                description = "";
            }
            return this;
        }

        public Builder canonicalName(String canonicalName)
        {
            this.canonicalName = canonicalName;
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
                functionId = FunctionId.toFunctionId(signature);
            }
            if (canonicalName == null) {
                canonicalName = signature.getName();
            }
            if (argumentNullability == null) {
                argumentNullability = Collections.nCopies(signature.getArgumentTypes().size(), kind == WINDOW);
            }
            return new FunctionMetadata(
                    functionId,
                    signature,
                    canonicalName,
                    new FunctionNullability(nullable, argumentNullability),
                    hidden,
                    deterministic,
                    description,
                    kind,
                    deprecated);
        }
    }
}
