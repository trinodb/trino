package tech.ydb.trino;

import com.google.inject.Inject;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;

import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.String.format;

public class YdbQueryBuilder extends DefaultQueryBuilder {
    @Inject
    public YdbQueryBuilder(RemoteQueryModifier queryModifier) {
        super(queryModifier);
    }

    @Override
    protected String formatJoinCondition(JdbcClient client, String leftAlias, String rightAlias, JdbcJoinCondition condition) {
        return format("%s %s %s",
                formatJoinKey(client, leftAlias, condition.getLeftColumn()),
                condition.getOperator().getValue(),
                formatJoinKey(client, rightAlias, condition.getRightColumn()));
    }

    private String formatJoinKey(JdbcClient client, String alias, JdbcColumnHandle column) {
        String reference = alias + "." + client.quoted(column.getColumnName());
        if (column.getJdbcTypeHandle().jdbcTypeName().filter("Uint64"::equalsIgnoreCase).isPresent()) {
            return "CAST(" + reference + " AS Decimal(20, 0))";
        }
        if (column.getColumnType().equals(REAL) || column.getColumnType().equals(DOUBLE)) {
            // Match Trino's JOIN behavior for NaN and signed zero.
            String type = column.getColumnType().equals(REAL) ? "Float" : "Double";
            return format("NANVL(IF(COALESCE(%1$s = 0, false), CAST(0 AS %2$s), %1$s), CAST(NULL AS %2$s))",
                    reference, type);
        }
        return reference;
    }
}
