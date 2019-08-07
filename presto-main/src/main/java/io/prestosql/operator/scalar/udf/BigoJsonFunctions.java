package io.prestosql.operator.scalar.udf;

import io.airlift.slice.Slice;
import io.prestosql.operator.scalar.JsonExtract;
import io.prestosql.operator.scalar.JsonPath;
import io.prestosql.spi.function.*;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.type.JsonPathType;

public class BigoJsonFunctions {
    @ScalarFunction("get_json_object")
    @LiteralParameters("x")
    @SqlNullable
    @SqlType(StandardTypes.JSON)
    public static Slice varcharGetJsonObject(@SqlType("varchar(x)") Slice json, @SqlType(JsonPathType.NAME) JsonPath jsonPath)
    {
        return JsonExtract.extract(json, jsonPath.getObjectExtractor());
    }
}
