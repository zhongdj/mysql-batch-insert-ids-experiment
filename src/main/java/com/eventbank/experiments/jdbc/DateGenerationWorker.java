package com.eventbank.experiments.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.Lists;

public class DateGenerationWorker extends JdbcWorker implements Runnable {

    private BlockingQueue<AutoIncrementedData> loadQueue = new LinkedBlockingQueue<AutoIncrementedData>();
    private BlockingQueue<ValidationResult> resultQueue;
    private final String id;

    public DateGenerationWorker(BlockingQueue<ValidationResult> resultQueue, String id, String user, String pass,
            String url) {
        super(user, pass, url);
        this.id = id;
        this.resultQueue = resultQueue;
    }

    private int fromSeq = 1;
    private int toSeq = 10000000;

    public void run() {

        ValidationWorker worker = new ValidationWorker(loadQueue, resultQueue, user, pass, url);
        worker.validate();
        worker = new ValidationWorker(loadQueue, resultQueue, user, pass, url);
        worker.validate();

        final ArrayList<AutoIncrementedData> dataList = makeDateList();
        final BatchCutter<AutoIncrementedData> cutter = new BatchCutter<AutoIncrementedData>(1000, dataList);
        final Iterator<List<AutoIncrementedData>> iterator = cutter.iterator();
        while (iterator.hasNext()) {
            insert(iterator.next());
        }

    }

    protected ArrayList<AutoIncrementedData> makeDateList() {
        final ArrayList<AutoIncrementedData> dataList = Lists.newArrayListWithCapacity(toSeq - fromSeq + 1);
        for ( int i = fromSeq; i <= toSeq; i++ ) {
            dataList.add(new AutoIncrementedData(i, id));
        }
        return dataList;
    }

    private void insert(final List<AutoIncrementedData> aBatch) {

        Statement stat = null;
        ResultSet resultSet = null;
        try {
            stat = this.connection.createStatement();
            // for ( AutoIncrementedData autoIncremetedData : aBatch ) {
            // stat.addBatch(createSql(autoIncremetedData));
            // }
            // int[] results = stat.executeBatch();
            final String sql = madeInsertFromBatch(aBatch);
            stat.execute(sql);
            this.connection.commit();

            final long firstId = queryFirstId();

            if ( 0 >= firstId ) {
                throw new IllegalStateException();
            }
            long id = firstId;
            for ( final AutoIncrementedData data : aBatch ) {
                data.setId(id++);
                loadQueue.put(data);
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
            if ( null != stat ) {
                try {
                    stat.close();
                } catch (SQLException ignored) {
                }
            }
        }

    }

    private String madeInsertFromBatch(List<AutoIncrementedData> aBatch) {
        StringBuilder builder = new StringBuilder("INSERT INTO tblIncrementalTest (code) VALUES ");
        for ( AutoIncrementedData autoIncrementedData : aBatch ) {
            builder.append("('").append(autoIncrementedData.getCode()).append("')").append(",");
        }
        builder.deleteCharAt(builder.length() - 1);

        return builder.toString();
    }

    protected long queryFirstId() throws SQLException {
        PreparedStatement stat = null;
        ResultSet idRs = null;
        try {
            stat = this.connection.prepareStatement("SELECT LAST_INSERT_ID()");
            idRs = stat.executeQuery();
            final long firstId;
            if ( idRs.next() ) {
                firstId = idRs.getLong(1);
            } else {
                firstId = 0;
            }
            return firstId;
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        } finally {
            if ( null != idRs ) {
                try {
                    idRs.close();
                } catch (SQLException ignored) {
                    ignored.printStackTrace();
                }
            }
            if ( null != stat ) {
                try {
                    stat.close();
                } catch (SQLException ignored) {
                }
            }
        }
    }

    private String createSql(AutoIncrementedData autoIncremetedData) {
        return "INSERT INTO tblIncrementalTest (code) VALUES ('" + autoIncremetedData.getCode() + "')";
    }
}
