package com.eventbank.experiments.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;

public class ValidationWorker extends JdbcWorker {

    private BlockingQueue<ValidationResult> resultQueue;
    private static final String sql = "SELECT COUNT(*) FROM tblIncrementalTest WHERE id = ? AND code = ?";
    private BlockingQueue<AutoIncrementedData> loadQueue;

    public ValidationWorker(final BlockingQueue<AutoIncrementedData> loadQueue,
            final BlockingQueue<ValidationResult> resultQueue, final String user, final String pass, final String url) {
        super(user, pass, url);
        this.resultQueue = resultQueue;
        this.loadQueue = loadQueue;
    }

    public void validate() {
        Thread t = new Thread(new Runnable() {
            public void run() {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        final AutoIncrementedData load = loadQueue.take();
                        doValidate(load.autoIncrementalNumer, load.seq, load.workerId);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        t.start();
    }

    protected void doValidate(final long incrementalId, final int generationSeq, final String workerName) {
        PreparedStatement ps = null;
        ResultSet resultSet = null;
        try {
            ps = this.connection.prepareStatement(sql);
            ps.setLong(1, incrementalId);
            ps.setString(2, generationSeq + "@" + workerName);
            resultSet = ps.executeQuery();
            this.connection.commit();
            if ( resultSet.next() ) {
                final int count = resultSet.getInt(1);

                final AutoIncrementedData data = new AutoIncrementedData(incrementalId, generationSeq, workerName);
                if ( count <= 0 ) {
                    resultQueue.put(new ValidationResult(data, false));
                } else {
                    resultQueue.put(new ValidationResult(data, true));
                }
            } else {
                throw new IllegalStateException();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if ( null != resultSet ) {
                try {
                    resultSet.close();
                } catch (SQLException ignored) {
                    ignored.printStackTrace();
                }
            }
            if ( null != ps ) {
                try {
                    ps.close();
                } catch (SQLException ignored) {
                }
            }
        }
    }
}
