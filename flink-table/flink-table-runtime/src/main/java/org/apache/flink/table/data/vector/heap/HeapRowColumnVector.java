package org.apache.flink.table.data.vector.heap;

import org.apache.flink.table.data.ColumnarRowData;
import org.apache.flink.table.data.vector.RowColumnVector;
import org.apache.flink.table.data.vector.VectorizedColumnBatch;
import org.apache.flink.table.data.vector.writable.WritableColumnVector;

/** This class represents a nullable heap row column vector. */
public class HeapRowColumnVector extends AbstractHeapVector
        implements WritableColumnVector, RowColumnVector {

    public WritableColumnVector[] fields;

    public HeapRowColumnVector(int len, WritableColumnVector... fields) {
        super(len);
        this.fields = fields;
    }

    @Override
    public ColumnarRowData getRow(int i) {
        ColumnarRowData columnarRowData = new ColumnarRowData(new VectorizedColumnBatch(fields));
        columnarRowData.setRowId(i);
        return columnarRowData;
    }
}
