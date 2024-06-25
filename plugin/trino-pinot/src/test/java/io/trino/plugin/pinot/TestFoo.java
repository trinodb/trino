package io.trino.plugin.pinot;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.exc.InvalidDefinitionException;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.plugin.pinot.query.ptf.context.SerializableExpressionContext;
import io.trino.plugin.pinot.query.ptf.context.SerializableLiteralContext;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.LiteralContext;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestFoo
{
    @Test
    public void test()
    {
        String query = "select * from foo limit 3 -- foo";
        SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(query);
        SqlNode sqlNode = sqlNodeAndOptions.getSqlNode();
        SqlOrderBy orderByNode = null;
        SqlSelect selectNode;
        if (sqlNode instanceof SqlOrderBy) {
            // Store order-by info into the select sql node
            orderByNode = (SqlOrderBy) sqlNode;
            selectNode = (SqlSelect) orderByNode.query;
            selectNode.setOrderBy(orderByNode.orderList);
            selectNode.setFetch(orderByNode.fetch);
            selectNode.setOffset(orderByNode.offset);
        } else {
            selectNode = (SqlSelect) sqlNode;
        }


        SqlNode limitNode = orderByNode.fetch;
        SqlParserPos pos = limitNode.getParserPosition();
        System.out.println(pos.getColumnNum() + ":" + pos.getLineNum()+ "-" + pos.getEndColumnNum() + ":" + pos.getEndLineNum());

        int limit = ((SqlNumericLiteral) limitNode).intValue(false);
        assertEquals(3, limit);
        System.out.println(query.substring(pos.getColumnNum()));
    }

    @Test
    public void testLiteral()
    {
        PinotDataType[] values = PinotDataType.values();
        for (PinotDataType value : values) {
            System.out.println(value.name());
        }
    }

    @Test
    public void testJoin()
    {
        String query = "set multistage=true; select * from foo join bar";
        SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(query);
        PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sqlNodeAndOptions);
        pinotQuery.setQueryOptions(Map.of("useMultistageEngine", "true"));
        // Query context does not account for joins! do not use it
        QueryContext queryContext = QueryContextConverterUtils.getQueryContext(pinotQuery);
        System.out.println(queryContext);
    }

    @Test
    public void testSerde()
    {
        String query = "set multistage=true; select * from foo join bar";
        SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(query);
        PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sqlNodeAndOptions);
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        ObjectMapper objectMapper = objectMapperProvider.get();
        objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        //objectMapper.disable()
        JsonCodecFactory factory = new JsonCodecFactory(() -> objectMapper);
        JsonCodec<PinotQuery> codec = factory.jsonCodec(PinotQuery.class);
        System.out.println(codec.toJson(pinotQuery));
    }

    @Test
    public void testLiteralContext()
    {
        Literal literal = Literal.intArrayValue(List.of(3,4,5));
        LiteralContext literalContext = new LiteralContext(literal);
        assertThat(FieldSpec.DataType.INT).isEqualTo(literalContext.getType());
        LiteralContext literalContext2 = new LiteralContext(literalContext.getType(), literalContext.getValue());
        assertThat(FieldSpec.DataType.INT).isEqualTo(literalContext2.getType());
    }

    @Test
    public void testJsonLiteralContextSerialization()
    {
        JsonCodecFactory jsonCodecFactory = new JsonCodecFactory(new ObjectMapperProvider());
        JsonCodec<LiteralContext> literalContextJsonCodec = jsonCodecFactory.jsonCodec(LiteralContext.class);
        assertThatCode(() -> {
                String json = literalContextJsonCodec.toJson(new LiteralContext(FieldSpec.DataType.INT, null));
        LiteralContext literalContext = literalContextJsonCodec.fromJson(json);
        assertThat(literalContext.getType()).isEqualTo(FieldSpec.DataType.INT);
        assertThat(literalContext.getValue()).isNull();
        }).hasCauseInstanceOf(InvalidDefinitionException.class);

        JsonCodec<SerializableLiteralContext> serializableLiteralContextJsonCodec = jsonCodecFactory.jsonCodec(SerializableLiteralContext.class);
        String json = serializableLiteralContextJsonCodec.toJson(new SerializableLiteralContext(new LiteralContext(FieldSpec.DataType.INT, null)));
        SerializableLiteralContext serializableLiteralContext = serializableLiteralContextJsonCodec.fromJson(json);
        assertThat(serializableLiteralContext.dataType()).isEqualTo(FieldSpec.DataType.INT);
        assertThat(serializableLiteralContext.value()).isEqualTo(Optional.empty());
        assertThat(serializableLiteralContext.value().orElse(null)).isNull();
    }

    @Test
    public void testExpressionContextJsonSerialization()
    {
        SerializableExpressionContext serializableExpressionContext = new SerializableExpressionContext(ExpressionContext.Type.IDENTIFIER, Optional.of("identifier"), Optional.empty(), Optional.empty());
        JsonCodec<SerializableExpressionContext> jsonCodec = JsonCodec.jsonCodec(SerializableExpressionContext.class);
        String json = jsonCodec.toJson(serializableExpressionContext);
        SerializableExpressionContext serializableExpressionContext1 = jsonCodec.fromJson(json);
        assertThat(serializableExpressionContext1.type()).isEqualTo(ExpressionContext.Type.IDENTIFIER);
        assertThat(serializableExpressionContext1.identifier()).isEqualTo(Optional.of("identifier"));
    }

    @Test
    public void testLiterals()
    {
        Literal literal = new Literal();
        literal.setIntValue(0);
        literal.setNullValue(true);
        assertThatThrownBy(literal::getIntValue).isInstanceOf(RuntimeException.class).hasMessage("Cannot get field 'intValue' because union is currently set to nullValue");
        assertThat(literal.isSetNullValue()).isEqualTo(true);
    }

    @Test
    public void testShadow()
    {
        Optional<List<Integer>> list = Optional.of(List.of(3,4,5));
        Optional<List<Integer>> list2 = Optional.of(List.of(6,7,8));
        // doesn't work!
        //list2.stream().map(list2 -> list2 - 1).collect(Collectors.toUnmodifiableList())
        //list2.map(list2 -> list2.stream().map(element -> element + 10))
        // Appearws to work with members though
    }
}
