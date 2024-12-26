package io.trino.arrow;

import io.trino.spi.block.Block;

public interface ArrowColumnWriter
{
    void write(Block block);
}
