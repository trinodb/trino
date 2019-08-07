package io.prestosql.plugin.bigo.udf;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import static io.airlift.slice.SliceUtf8.countCodePoints;

public class BigoStringFunctions {

    @Description("returns index of first occurrence of a substring (or 0 if not found)")
    @ScalarFunction("instr")
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.BIGINT)
    public static long inStr(@SqlType("varchar(x)") Slice string, @SqlType("varchar(y)") Slice substring)
    {
        if (substring.length() == 0) {
            return 1;
        }

        int index = string.indexOf(substring);
        if (index < 0) {
            return 0;
        }
        return countCodePoints(string, 0, index) + 1;
    }
}
