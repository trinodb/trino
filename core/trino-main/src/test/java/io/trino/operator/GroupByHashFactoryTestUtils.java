package io.trino.operator;

import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;
import io.trino.type.BlockTypeOperators;

public final class GroupByHashFactoryTestUtils
{
    private GroupByHashFactoryTestUtils() {}

    public static GroupByHashFactory createGroupByHashFactory()
    {
        return createGroupByHashFactory(new TypeOperators());
    }

    public static GroupByHashFactory createGroupByHashFactory(TypeOperators typeOperators)
    {
        return createGroupByHashFactory(typeOperators, new BlockTypeOperators(typeOperators));
    }

    public static GroupByHashFactory createGroupByHashFactory(TypeOperators typeOperators, BlockTypeOperators blockTypeOperators)
    {
        return createGroupByHashFactory(typeOperators, blockTypeOperators);
    }

    public static GroupByHashFactory createGroupByHashFactory(JoinCompiler joinCompiler, BlockTypeOperators blockTypeOperators)
    {
        return new GroupByHashFactory(joinCompiler, blockTypeOperators);
    }
}
