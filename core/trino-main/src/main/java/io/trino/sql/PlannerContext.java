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
package io.trino.sql;

import com.google.inject.Inject;
import io.opentelemetry.api.trace.Tracer;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.FunctionResolver;
import io.trino.metadata.LanguageFunctionManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction.ResolvedFunctionDecoder;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;

import static java.util.Objects.requireNonNull;

/**
 * A carrier of core, global, non-derived services for planner and analyzer.
 * This is used to ease the addition of new services in the future without
 * having to modify large portions the planner and analyzer just to pass around
 * the service.
 */
public class PlannerContext
{
    // Be careful when adding services here. This context is used
    // throughout the analyzer and planner, so it is easy to create
    // circular dependencies, just create a junk drawer of services.
    private final Metadata metadata;
    private final TypeOperators typeOperators;
    private final BlockEncodingSerde blockEncodingSerde;
    private final TypeManager typeManager;
    private final FunctionManager functionManager;
    private final LanguageFunctionManager languageFunctionManager;
    private final Tracer tracer;
    private final ResolvedFunctionDecoder functionDecoder;

    @Inject
    public PlannerContext(Metadata metadata,
            TypeOperators typeOperators,
            BlockEncodingSerde blockEncodingSerde,
            TypeManager typeManager,
            FunctionManager functionManager,
            LanguageFunctionManager languageFunctionManager,
            Tracer tracer)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.languageFunctionManager = requireNonNull(languageFunctionManager, "languageFunctionManager is null");
        // the function decoder contains caches that are critical for planner performance so this must be shared
        this.functionDecoder = new ResolvedFunctionDecoder(typeManager::getType);
        this.tracer = requireNonNull(tracer, "tracer is null");
    }

    public Metadata getMetadata()
    {
        return metadata;
    }

    public TypeOperators getTypeOperators()
    {
        return typeOperators;
    }

    public BlockEncodingSerde getBlockEncodingSerde()
    {
        return blockEncodingSerde;
    }

    public TypeManager getTypeManager()
    {
        return typeManager;
    }

    public FunctionManager getFunctionManager()
    {
        return functionManager;
    }

    public ResolvedFunctionDecoder getFunctionDecoder()
    {
        return functionDecoder;
    }

    public FunctionResolver getFunctionResolver()
    {
        return getFunctionResolver(WarningCollector.NOOP);
    }

    public FunctionResolver getFunctionResolver(WarningCollector warningCollector)
    {
        return new FunctionResolver(metadata, typeManager, languageFunctionManager, functionDecoder, warningCollector);
    }

    public LanguageFunctionManager getLanguageFunctionManager()
    {
        return languageFunctionManager;
    }

    public Tracer getTracer()
    {
        return tracer;
    }
}
