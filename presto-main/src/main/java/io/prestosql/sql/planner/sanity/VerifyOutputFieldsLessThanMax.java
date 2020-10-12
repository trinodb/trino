package io.prestosql.sql.planner.sanity;

import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeOperators;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.PlanNode;

import java.util.List;

import static io.prestosql.SystemSessionProperties.getQueryMaxFieldsOutput;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;

public class VerifyOutputFieldsLessThanMax
        implements PlanSanityChecker.Checker
{
    @Override
    public void validate(PlanNode planNode, Session session, Metadata metadata, TypeOperators typeOperators, TypeAnalyzer typeAnalyzer, TypeProvider types, WarningCollector warningCollector)
    {
        sanityCheckOutputFieldsCount(planNode, session, types);
    }

    private static void sanityCheckOutputFieldsCount(PlanNode root, Session session, TypeProvider typeProvider)
    {
        List<Symbol> outputSymbols = root.getOutputSymbols();
        long count = 0;
        for (Symbol outputSymbol : outputSymbols) {
            Type type = typeProvider.get(outputSymbol);
            count += countOutputFields(type);
        }
        int maxCount = getQueryMaxFieldsOutput(session);
        if (count > maxCount) {
            throw new PrestoException(NOT_SUPPORTED, format("Maximum number of output fields of %s has been exceeded with %s", maxCount, count));
        }
    }

    private static long countOutputFields(Type type)
    {
        List<Type> typeParameters = type.getTypeParameters();
        if (typeParameters.isEmpty()) {
            return 1;
        }
        int count = 0;
        for (Type subtype : typeParameters) {
            count += countOutputFields(subtype);
        }
        return count;
    }
}
