package com.eventbank.experiments.jdbc;

public class AutoIncrementedData {

    public volatile long autoIncrementalNumer;
    public final int seq;
    public final String workerId;

    public AutoIncrementedData(final long autoIncrementalNumer, final int seq, final String workerId) {
        this.autoIncrementalNumer = autoIncrementalNumer;
        this.seq = seq;
        this.workerId = workerId;
    }

    public AutoIncrementedData(final int seq, final String workerId) {
        this.seq = seq;
        this.workerId = workerId;
    }

    public String getCode() {
        return seq + "@" + workerId;
    }

    public void setId(long id) {
        this.autoIncrementalNumer = id;
    }

    @Override
    public String toString() {
        return "AutoIncrementedData [autoIncrementalNumer=" + autoIncrementalNumer + ", seq=" + seq + ", workerId="
                + workerId + "]";
    }

}
