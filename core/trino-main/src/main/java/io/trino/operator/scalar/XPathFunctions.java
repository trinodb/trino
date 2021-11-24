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

package io.trino.operator.scalar;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.w3c.dom.NodeList;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.VarcharType.VARCHAR;

/**
 * implementation of xml string operation function, migrate from hive function
 * https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/udf/xml/
 */
public class XPathFunctions
{
    private static final XPathUtils xpathUtil = XPathUtils.getInstance();

    private XPathFunctions() {}

    @SqlNullable
    @Description("Returns a string array of values within xml nodes that match the xpath expression")
    @ScalarFunction("xpath")
    @LiteralParameters({"x", "y"})
    @SqlType("array(varchar(x))")
    public static Block xpath(@SqlType("varchar(x)") Slice xml, @SqlType("varchar(y)") Slice path)
    {
        BlockBuilder parts = VARCHAR.createBlockBuilder(null, 1, xml.length());
        NodeList nodeList = xpathUtil.evalNodeList(xml.toStringUtf8(), path.toStringUtf8());

        if (nodeList == null || nodeList.getLength() == 0) {
            return parts.build();
        }

        for (int i = 0; i < nodeList.getLength(); i++) {
            String value = nodeList.item(i).getNodeValue();
            if (value != null) {
                VARCHAR.writeSlice(parts, utf8Slice(value));
            }
        }
        return parts.build();
    }

    @SqlNullable
    @Description("Evaluates a boolean xpath expression")
    @ScalarFunction("xpath_boolean")
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean xpathBoolean(@SqlType("varchar(x)") Slice xml, @SqlType("varchar(y)") Slice path)
    {
        return xpathUtil.evalBoolean(xml.toStringUtf8(), path.toStringUtf8());
    }

    @SqlNullable
    @Description("Returns a double value that matches the xpath expression")
    @ScalarFunction(value = "xpath_double", alias = {"xpath_number", "xpath_float"})
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.DOUBLE)
    public static Double xpathDouble(@SqlType("varchar(x)") Slice xml, @SqlType("varchar(y)") Slice path)
    {
        return xpathUtil.evalNumber(xml.toStringUtf8(), path.toStringUtf8());
    }

    @SqlNullable
    @Description("Returns a long value that matches the xpath expression")
    @ScalarFunction(value = "xpath_long", alias = "xpath_int")
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.BIGINT)
    public static Long xpathLong(@SqlType("varchar(x)") Slice xml, @SqlType("varchar(y)") Slice path)
    {
        Double result = xpathUtil.evalNumber(xml.toStringUtf8(), path.toStringUtf8());
        return result == null ? null : result.longValue();
    }

    @SqlNullable
    @Description("Returns a string value that matches the xpath expression")
    @ScalarFunction(value = "xpath_string", alias = "xpath_str")
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.VARCHAR)
    public static Slice xpathString(@SqlType("varchar(x)") Slice xml, @SqlType("varchar(y)") Slice path)
    {
        String result = xpathUtil.evalString(xml.toStringUtf8(), path.toStringUtf8());
        return result == null ? null : utf8Slice(result);
    }
}
