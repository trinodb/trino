package io.trino.parquet.reader;

import com.google.common.io.Resources;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.*;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTestUtils.createParquetReader;
import static org.assertj.core.api.Assertions.assertThat;

public class TestNestedColumnReaderListInnerList {

    @Test
    public void testNestedColumnListWithInnerList()
            throws IOException, URISyntaxException
    {
        // Parquet schema
        //    message schema {
        //        optional group my_list_r1 (LIST) {
        //            repeated int32 element;
        //        }
        //        optional group my_list_r2 (LIST) {
        //            repeated group element {
        //                required binary str (STRING);
        //                required int32 num;
        //            }
        //        }
        //        optional group my_list_r3 (LIST) {
        //            repeated group array (LIST) {
        //                repeated int32 array;
        //            }
        //        }
        //        optional group my_list_r4_1 (LIST) {
        //            repeated group array {
        //                required binary str (STRING);
        //            }
        //        }
        //        optional group my_list_r4_2 (LIST) {
        //            repeated group my_list_tuple {
        //                required binary str (STRING);
        //            }
        //        }
        //        optional group my_list_r5 (LIST) {
        //            repeated group element {
        //                optional binary str (STRING);
        //            }
        //        }
        //    }

        ParquetDataSource dataSource = new FileParquetDataSource(
                new File(Resources.getResource("list_nested_column_data.parquet").toURI()),
                ParquetReaderOptions.defaultOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());

        List<String> columnNames = List.of(
                "my_list_r1",  // array(integer)
                "my_list_r2",  // array(row(str varchar, num integer))
                "my_list_r3",  // array(array(integer))
                "my_list_r4_1",  // array(varchar)
                "my_list_r4_2",  // array(varchar)
                "my_list_r5");   // array(varchar)

        List<Type> types = List.of(
                new ArrayType(IntegerType.INTEGER),
                new ArrayType(RowType.rowType(
                        new RowType.Field(Optional.of("str"), VarcharType.createUnboundedVarcharType()),
                        new RowType.Field(Optional.of("num"), IntegerType.INTEGER)
                )),
                new ArrayType(new ArrayType(IntegerType.INTEGER)),
                new ArrayType(VarcharType.createUnboundedVarcharType()),
                new ArrayType(VarcharType.createUnboundedVarcharType()),
                new ArrayType(VarcharType.createUnboundedVarcharType())
        );

        ParquetReader reader = createParquetReader(dataSource, parquetMetadata, newSimpleAggregatedMemoryContext(), types, columnNames);
        SourcePage page = reader.nextPage();
        assertThat(page).isNotNull();
        assertThat(page.getChannelCount()).isEqualTo(6);

        List<?> listR1 = (List<?>) types.get(0).getObjectValue(page.getBlock(0), 0);
        assertThat(listR1.size()).isEqualTo(2);
        List<?> listR2 = (List<?>) types.get(1).getObjectValue(page.getBlock(1), 0);
        assertThat(listR2.size()).isEqualTo(2);

        List<?> listR3 = (List<?>) types.get(2).getObjectValue(page.getBlock(2), 0);
        assertThat(listR3.size()).isEqualTo(1);

        List<?> listR41 = (List<?>) types.get(3).getObjectValue(page.getBlock(3), 0);
        assertThat(listR41.size()).isEqualTo(1);

        List<?> listR42 = (List<?>) types.get(4).getObjectValue(page.getBlock(4), 0);
        assertThat(listR42.size()).isEqualTo(1);

        List<?> listR5 = (List<?>) types.get(5).getObjectValue(page.getBlock(5), 0);
        assertThat(listR5.size()).isEqualTo(2);
    }
}
