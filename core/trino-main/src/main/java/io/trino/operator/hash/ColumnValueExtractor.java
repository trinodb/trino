package io.trino.operator.hash;

import io.trino.spi.block.Block;
import io.trino.spi.type.FixedWidthType;
import io.trino.spi.type.Type;

import static io.trino.spi.type.BigintType.BIGINT;

public interface ColumnValueExtractor
{
    static boolean isSupported(Type type)
    {
        return BIGINT.equals(type);
    }

    void putValue(GroupByHashTableAccess row, int rowPosition, int valueIndex, Block block, int position);

    int getSummarySize();

    static ColumnValueExtractor columnValueExtractor(Type type)
    {
        if (BIGINT.equals(type)) {
            return new BigIntValueExtractor();
        }
        throw new RuntimeException("unsupported type " + type);
    }

    class BigIntValueExtractor
            extends FixedWidthTypeValueExtractor
            implements ColumnValueExtractor
    {
        public BigIntValueExtractor()
        {
            super(BIGINT);
        }

        @Override
        public void putValue(GroupByHashTableAccess row, int rowPosition, int valueIndex, Block block, int position)
        {
            row.putLongValue(rowPosition, valueIndex, BIGINT.getLong(block, position));
        }
    }

    abstract class FixedWidthTypeValueExtractor
            implements ColumnValueExtractor
    {
        private static final int MAX_SUMMARY_SIZE = 16;
        private final FixedWidthType type;

        protected FixedWidthTypeValueExtractor(FixedWidthType type) {this.type = type;}

        @Override
        public int getSummarySize()
        {
            return Math.min(type.getFixedSize(), MAX_SUMMARY_SIZE);
        }
    }
}
