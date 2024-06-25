package io.trino.plugin.pinot.query.ptf;

import org.apache.pinot.common.request.Join;
import org.apache.pinot.common.request.JoinType;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record SerializableJoin(JoinType joinType, Optional<SerializableDataSource> left, Optional<SerializableDataSource> right, Optional<SerializableExpression> condition)
{
    public SerializableJoin
    {
        requireNonNull(joinType, "joinType is null");
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");
        requireNonNull(condition, "condition is null");
    }

    public SerializableJoin(Join join)
    {
        this(join.getType(),
                Optional.ofNullable(join.getLeft())
                        .map(SerializableDataSource::new),
                Optional.ofNullable(join.getRight())
                        .map(SerializableDataSource::new),
                Optional.ofNullable(join.getCondition())
                        .map(SerializableExpression::new));
    }

    public Join toJoin()
    {
        Join join = new Join();
        join.setType(joinType);
        left.map(SerializableDataSource::toDataSource).ifPresent(join::setLeft);
        right.map(SerializableDataSource::toDataSource).ifPresent(join::setRight);
        condition.map(SerializableExpression::toExpression).ifPresent(join::setCondition);
        return join;
    }
}
