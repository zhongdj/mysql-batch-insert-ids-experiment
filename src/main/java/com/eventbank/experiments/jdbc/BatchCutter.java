package com.eventbank.experiments.jdbc;

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;

public class BatchCutter<T> implements Iterable<List<T>> {

    private static final int DEFAULT_BATCH_SIZE = 200;

    private final int batchSize;
    private final List<List<T>> batchList;

    public BatchCutter(final List<T> data) {
        this(DEFAULT_BATCH_SIZE, data);
    }

    public BatchCutter(final int batchSize, final List<T> data) {
        this.batchSize = batchSize;
        if ( null == data ) {
            throw new NullPointerException("data cannot be null.");
        }
        batchList = makeBatchList(data, batchSize);
    }

    private List<List<T>> makeBatchList(final List<T> data, final int batchSize) {
        List<List<T>> batchList;
        if ( data.size() <= this.batchSize ) {
            batchList = Lists.newArrayList();
            batchList.add(Lists.newArrayList(data));
        } else {
            int initialArraySize = calculateBatchListSize(batchSize, data);
            batchList = Lists.newArrayListWithCapacity(initialArraySize);
            List<T> accumulator = Lists.newArrayList(data);
            while (!accumulator.isEmpty()) {
                if ( batchSize >= accumulator.size() ) {
                    batchList.add(accumulator);
                    accumulator = Lists.newArrayList();
                } else {
                    batchList.add(accumulator.subList(0, batchSize));
                    accumulator = accumulator.subList(batchSize, accumulator.size());
                }
            }
        }
        return batchList;
    }

    private int calculateBatchListSize(final int batchSize, final List<T> data) {
        return data.size() % batchSize == 0 ? data.size() / this.batchSize : (data.size() / batchSize) + 1;
    }

    public Iterator<List<T>> iterator() {
        return batchList.iterator();
    }

}
