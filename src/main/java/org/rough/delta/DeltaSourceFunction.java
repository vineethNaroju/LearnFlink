package org.rough.delta;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import java.util.concurrent.ThreadLocalRandom;

public class DeltaSourceFunction extends RichParallelSourceFunction<RowData> {

    private  volatile boolean running = true;


    DataFormatConverters.DataFormatConverter<RowData, Row> converter
            = DataFormatConverters.getConverterForDataType(
                    TypeConversions.fromLogicalToDataType(DeltaConfig.rowType));

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {

        ThreadLocalRandom random = ThreadLocalRandom.current();

        while(running) {

            RowData rowData = converter.toInternal(
                    Row.of(random.nextInt(0, 100))
            );

            ctx.collect(rowData);

            Thread.sleep(1_000);
        }
    }

    @Override
    public void cancel() {
        running = true;
    }
}
// add flink-table-runtime in pom.xml