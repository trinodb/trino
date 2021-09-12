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
import io.trino.util.UDFXPathUtil;
import org.w3c.dom.NodeList;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.VarcharType.VARCHAR;

public final class XPathFunctions
{
    private XPathFunctions() {}

    @SqlNullable
    @Description("Returns the text contents of the first xml node that matches the xpath expression")
    @ScalarFunction("xpath_string")
    @LiteralParameters({"x", "y"})
    @SqlType("varchar(x)")
    public static Slice xpathString(@SqlType("varchar(x)") Slice xml, @SqlType("varchar(y)") Slice path)
    {
        UDFXPathUtil xpath = new UDFXPathUtil();
        String s = xpath.evalString(xml.toStringUtf8(), path.toStringUtf8());
        if (s == null) {
            return null;
        }
        return utf8Slice(s);
    }

    @SqlNullable
    @Description("Returns a string array of values within xml nodes that match the xpath expression")
    @ScalarFunction("xpath")
    @LiteralParameters({"x", "y"})
    @SqlType("array(varchar)")
    public static Block xpath(@SqlType("varchar(y)") Slice xml, @SqlType("varchar(x)") Slice path)
    {
        List<String> initList = eval(xml.toStringUtf8(), path.toStringUtf8());

        BlockBuilder builder = VARCHAR.createBlockBuilder(null, initList.size());
        for (String value : initList) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                VARCHAR.writeSlice(builder, utf8Slice(value));
            }
        }
        return builder.build();
    }

    @SqlNullable
    @Description("Returns true if the XPath expression evaluates to true, or if a matching node is found")
    @ScalarFunction("xpath_boolean")
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean xpath_boolean(@SqlType("varchar(y)") Slice xml, @SqlType("varchar(x)") Slice path)
    {
        UDFXPathUtil xpath = new UDFXPathUtil();
        return xpath.evalBoolean(xml.toStringUtf8(), path.toStringUtf8());
    }

    @SqlNullable
    @Description("Return a double value, or the value 0.0 if no match is found, or NaN for non-numeric match")
    @ScalarFunction("xpath_double")
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.DOUBLE)
    public static Double xpath_double(@SqlType("varchar(y)") Slice xml, @SqlType("varchar(x)") Slice path)
    {
        UDFXPathUtil xpath = new UDFXPathUtil();
        return xpath.evalNumber(xml.toStringUtf8(), path.toStringUtf8());
    }

    private static List<String> eval(String xml, String path)
    {
        UDFXPathUtil xpath = new UDFXPathUtil();
        NodeList nodeList = xpath.evalNodeList(xml, path);
        if (nodeList == null) {
            return Collections.<String>emptyList();
        }

        return Stream.iterate(0, i -> i < nodeList.getLength(), i -> i + 1)
                .map(nodeList::item)
                .filter(field -> field.getNodeValue() != null)
                .map(field -> field.getNodeValue())
                .collect(Collectors.toList());
    }
}
