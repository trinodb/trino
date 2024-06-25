package io.trino.plugin.pinot.query.ptf;

import org.apache.pinot.common.request.Literal;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record SerializableLiteral(Literal._Fields setField, Optional<Object> value)
{
    public SerializableLiteral
    {
        requireNonNull(setField, "setField is null");
        requireNonNull(value, "value is null");
    }

    public SerializableLiteral(Literal literal)
    {
        this(literal.getSetField(), Optional.ofNullable(literal.getFieldValue()));
    }

    public Literal toLiteral()
    {
        return new Literal(setField, value.orElse(null));
    }
}
