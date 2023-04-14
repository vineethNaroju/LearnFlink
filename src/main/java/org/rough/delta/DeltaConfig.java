package org.rough.delta;

import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;

public final class DeltaConfig {

    public static final RowType rowType
            = new RowType(
                    Collections.singletonList(
                            new RowType.RowField("val", new IntType())
                    )
    );

}
