package io.trino.arrow;

import io.trino.spi.block.Block;

//interface for writing to a trino block to an arrow vector
//implementations are responsible for allocating their own memory and safely writing to it
public interface ArrowColumnWriter
{
    //write the block to the underlying vector. this should be called exactly once per writer
    void write(Block block);

}
