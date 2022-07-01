module com.tugalsan.api.sql.col {
    requires java.sql;
    requires com.tugalsan.api.executable;
    requires com.tugalsan.api.list;
    requires com.tugalsan.api.unsafe;
    requires com.tugalsan.api.log;
    requires com.tugalsan.api.string;
    requires com.tugalsan.api.sql.update;
    requires com.tugalsan.api.sql.sanitize;
    requires com.tugalsan.api.sql.conn;
    requires com.tugalsan.api.sql.db;
    requires com.tugalsan.api.sql.resultset;
    requires com.tugalsan.api.sql.col.typed;
    exports com.tugalsan.api.sql.col.server;
}
