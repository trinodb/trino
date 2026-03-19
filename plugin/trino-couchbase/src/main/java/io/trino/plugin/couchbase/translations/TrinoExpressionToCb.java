package io.trino.plugin.couchbase.translations;

import io.trino.plugin.couchbase.CouchbaseColumnHandle;
import io.trino.plugin.couchbase.ParametrizedString;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;

import java.util.*;

public class TrinoExpressionToCb {
    public static List<ParametrizedString> convert(List<ConnectorExpression> expressions, Map<String, ColumnHandle> assignments) {
        return expressions.stream().map(e -> convert(e, assignments)).toList();
    }
    public static ParametrizedString convert(ConnectorExpression expression, Map<String, ColumnHandle> assignments) {
        if (expression instanceof Call call) {
            return processCall(call, assignments);
        } else if (expression instanceof Constant constant) {
            // todo: support type casts
            return ParametrizedString.from("?", Arrays.asList(constant.getValue()));
        } else if (expression instanceof Variable variable) {
            final String name = variable.getName();
            ColumnHandle assignment = assignments.get(name);
            if (assignment == null) {
                return ParametrizedString.from(String.format("`%s`", variable.getName()));
            } else if (assignment instanceof CouchbaseColumnHandle column) {
                return ParametrizedString.from(String.format("`%s`", column.name()));
            } else {
                throw new IllegalArgumentException("Unsupported column type: " + assignment);
            }
        }
        throw new IllegalArgumentException("Unsupported expression type: " + expression.getClass());
    }

    private static ParametrizedString processCall(Call call, Map<String, ColumnHandle> assignments) {
        final String fname = call.getFunctionName().getName();
        if ("$in".equals(fname)) {
            ParametrizedString source = convert(call.getArguments().get(0), assignments);
            ParametrizedString list = convert(call.getArguments().get(1), assignments);
            return ParametrizedString.join(
                    Arrays.asList(source, list), " IN ", "", ""
            );
        } else if ("$array".equals(fname)) {
            return ParametrizedString.join(
                    convert(call.getArguments(), assignments), ", ", "[", "]"
            );
        } else if ("$cast".equals(fname)) {
            final String castFn = String.format("%s(", getCastFn(call.getType()));
            return ParametrizedString.join(
                    convert(call.getArguments(), assignments),
                    String.format(", %s", castFn),
                    castFn,
                    ")"
            );
        } else if ("$equal".equals(fname)) {
            return ParametrizedString.join(
                    convert(call.getArguments(), assignments), " = ", "", ""
            );
        } else if ("$modulus".equals(fname) || "mod".equals(fname)) {
            return ParametrizedString.join(
                    convert(call.getArguments(), assignments),
                    " % ", "", ""
            );
        } else if ("$add".equals(fname)) {
            return ParametrizedString.join(
                    convert(call.getArguments(), assignments),
                    " + ", "", ""
            );
        } else if ("$negate".equals(fname)) {
            return ParametrizedString.join(
                    convert(call.getArguments(), assignments),
                    " + ", "-(", ")"
            );
        } else if ("$less_than".equals(fname)) {
            return ParametrizedString.join(
                    convert(call.getArguments(), assignments),
                    " < ", "", ""
            );
        } else if ("$not".equals(fname)) {
            return ParametrizedString.join(
                    convert(call.getArguments(), assignments),
                    ") OR (", "NOT ((", "))"
            );
        } else if ("$not_equal".equals(fname)) {
            return ParametrizedString.join(
                    convert(call.getArguments(), assignments),
                    " != ", "", ""
            );
        } else {
            throw new IllegalArgumentException("Unsupported function: " + call.getFunctionName());
        }
    }

    private static String getCastFn(Type type) {
        if (type.getJavaType() == Int128.class
        || type instanceof DoubleType) {
            return "to_number";
        } else {
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }
}
