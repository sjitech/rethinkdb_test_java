package util;

import com.rethinkdb.net.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.rethinkdb.RethinkDB.r;

public class Database {

    public final String dbName;
    public final Connection c;
    final Logger logger;

    public int batchRows = 1000;
    public boolean throwErrorIfInsertSamePrimaryKey = true;

    public Database(String dbName) {
        this.dbName = dbName;
        c = Connection.build().db(dbName).connect();
        logger = LoggerFactory.getLogger(Database.class);

        logger.debug("create database {}", dbName);
        r.branch(r.dbList().contains(dbName)
                , r.expr(true)
                , r.dbCreate(dbName)
        ).do_(unused -> r.db(dbName).wait_())
                .run(c);
    }

    public void dropTable(String tableName) {
        logger.debug("dropTable table {}", tableName);
        r.branch(r.tableList().contains(tableName)
                , r.tableDrop(tableName)
                , r.expr(true)
        )
                .run(c);
    }

    public void createTable(String tableName, String primaryKey) {
        logger.debug("create table {}", tableName);
        r.branch(r.tableList().contains(tableName)
                        .and(r.not(r.table(tableName).info().g("primary_key").eq(primaryKey)))
                , r.tableDrop(tableName)
                , r.expr(true)
        ).do_(unused ->
                r.branch(r.tableList().contains(tableName)
                        , r.expr(true)
                        , r.tableCreate(tableName).optArg("primary_key", primaryKey)
                )
        ).do_(unused -> r.table(tableName).wait_())
                .run(c);
    }

    public void recreateTable(String tableName, String primaryKey) {
        dropTable(tableName);
        createTable(tableName, primaryKey);
    }

    public void createIndex(String tableName, String indexColumn) {
        logger.debug("create index {} for {} of table {}", indexColumn, indexColumn, tableName);
        r.branch(r.table(tableName).indexList().contains(indexColumn)
                , r.expr(true)
                , r.table(tableName).indexCreate(indexColumn)
        ).do_(unused -> r.table(tableName).indexWait(indexColumn))
                .run(c);
    }

    public void createIndex(String tableName, String indexName, String[] columnNames) {
        logger.debug("create index {} for {} of table {}", indexName, columnNames, tableName);
        r.branch(r.table(tableName).indexList().contains(indexName)
                , r.expr(true)
                , r.table(tableName).indexCreate(indexName,
                        row -> Arrays.stream(columnNames).map(columnName -> row.g(columnName)).toArray())
        ).do_(unused -> r.table(tableName).indexWait(indexName))
                .run(c);
    }

    public int bulkInsert(String tableName, List rowAry) {
        logger.debug("insert {} rows to {}", rowAry.size(), tableName);
        Map<String, Object> res = r.table(tableName).insert(rowAry).run(c);
        int insertedCount = ((Long) res.get("inserted")).intValue();
        if (throwErrorIfInsertSamePrimaryKey && insertedCount < rowAry.size()) {
            throw new RuntimeException(res.get("first_error").toString());
        }
        logger.debug("done");
        return insertedCount;
    }

    public interface RowProvider {
        /**
         * return an Object or Object[] or List
         * return null means skip, false means abort
         *
         * @param iRow absolute index of result row in total rows returned.
         *             Usually is same as callbackIndex.
         *             If return array or null then it will be different with callbackIndex.
         * @return single or array of row data object (javaBean/HashMap).
         */
        Object getDataOfRow(long iRow);
    }

    public interface RowProvider2 {
        /**
         * return an Object or Object[] or List
         * return null means skip, false means abort
         *
         * @param iRow      absolute index of result row in total rows returned.
         *                  Usually is same as callbackIndex.
         *                  If return array or null then it will be different with callbackIndex.
         * @param iCallback sequence of calling RowProvider
         * @return single or array of row data object (javaBean/HashMap).
         */
        Object getDataOfRow(long iRow, long iCallback);
    }

    /**
     * Bulk insert
     *
     * @param tableName                 table name
     * @param callingCountOfRowProvider the count of calling RowProvider
     * @param rowProvider               a callback to return single or array of row data. Null means skip, false means abort
     * @return inserted row count
     */
    public long bulkInsert(String tableName, long callingCountOfRowProvider, RowProvider2 rowProvider) {
        logger.debug("bulkInsert to table {}", tableName);
        logger.debug(" prepare rows");

        long insertedCount = 0;
        long iRow = 0;
        Object[] ary1 = new Object[1];

        int batchRows = this.batchRows;
        ArrayList<Object> rowAry = new ArrayList<>(batchRows);

        for (long i = 0; i < callingCountOfRowProvider; i++) {

            Object row = ary1[0] = rowProvider.getDataOfRow(iRow, i);

            if (row == null) continue;
            if (row.equals(false)) break;

            List _ary = (row instanceof List) ? (List) row
                    : Arrays.asList((row instanceof Object[]) ? (Object[]) row : ary1);

            for (Object _row : _ary) {

                rowAry.add(_row);
                iRow++;

                if (rowAry.size() == batchRows) {
                    bulkInsert(tableName, rowAry);

                    insertedCount += batchRows;
                    rowAry.clear();
                    logger.debug(" prepare rows");
                }
            }
        }

        if (rowAry.size() > 0) {
            bulkInsert(tableName, rowAry);
            insertedCount += rowAry.size();
        }
        logger.debug("bulkInsert done: {} rows inserted to {}", insertedCount, tableName);
        return insertedCount;
    }

    public long bulkInsert(String tableName, long callingCountOfRowProvider, RowProvider rowProvider) {
        return bulkInsert(tableName, callingCountOfRowProvider, ((iRow, iCallback) -> rowProvider.getDataOfRow(iRow)));
    }

    public void deleteAllData(String tableName) {
        logger.debug("delete all data from {}", tableName);
        Map<String, Object> res = r.table(tableName).delete().run(c);
        logger.debug("done: {} rows deleted", res.get("deleted"));
    }

    public long getTableRowCount(String tableName) {
        logger.debug("get row count of {}", tableName);
        long rowCount = r.table(tableName).count().run(c);
        logger.debug("done: {} rows", rowCount);
        return rowCount;
    }
}
