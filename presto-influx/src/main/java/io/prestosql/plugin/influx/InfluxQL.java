package io.prestosql.plugin.influx;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.Slice;

public class InfluxQL {

    private final StringBuilder influxQL;

    public InfluxQL() {
        influxQL = new StringBuilder();
    }

    @JsonCreator
    public InfluxQL(@JsonProperty("q") String prefix) {
        influxQL = new StringBuilder(prefix);
    }

    public InfluxQL append(InfluxQL fragment) {
        influxQL.append(fragment);
        return this;
    }

    public InfluxQL append(String s) {
        influxQL.append(s);
        return this;
    }

    public InfluxQL append(char ch) {
        influxQL.append(ch);
        return this;
    }

    public InfluxQL add(InfluxColumn column) {
        addIdentifier(column.getInfluxName());
        return this;
    }

    public InfluxQL add(Object value) {
        assert !(value instanceof InfluxColumn): value;
        if (value == null) {
            influxQL.append("null");
        } else if (value instanceof Slice) {
            quote(((Slice) value).toStringUtf8(), '\'');
        } else if (value instanceof Number || value instanceof Boolean) {
            influxQL.append(value);
        } else {
            quote(value.toString(), '\'');
        }
        return this;
    }

    public InfluxQL addIdentifier(String identifier) {
        boolean safe = true;
        for (int i = 0; i < identifier.length() && safe; i++) {
            char ch = identifier.charAt(i);
            safe = (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (i > 0 && ch >= '0' && ch <= '9') || ch == '_';
        }
        if (safe) {
            influxQL.append(identifier);
        } else {
            quote(identifier, '"');
        }
        return this;
    }

    public void quote(String value, char delimiter) {
        append(delimiter);
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch < ' ' || ch > 127) {
                InfluxError.BAD_VALUE.fail("illegal value", value);
            }
            if (ch == delimiter || ch == '\\') {
                append('\\');
            }
            append(ch);
        }
        append(delimiter);
    }

    public boolean isEmpty() {
        return influxQL.length() == 0;
    }

    @JsonProperty("q")
    @Override
    public String toString() {
        return influxQL.toString();
    }

}
