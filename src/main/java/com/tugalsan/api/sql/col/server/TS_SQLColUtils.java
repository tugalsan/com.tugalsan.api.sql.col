package com.tugalsan.api.sql.col.server;

import com.tugalsan.api.function.client.maythrowexceptions.checked.TGS_FuncMTCUtils;
import java.sql.*;
import java.util.*;
import com.tugalsan.api.list.client.*;
import com.tugalsan.api.sql.col.typed.client.*;
import com.tugalsan.api.sql.conn.server.*;
import com.tugalsan.api.sql.db.server.*;
import com.tugalsan.api.sql.resultset.server.*;
import com.tugalsan.api.sql.sanitize.server.*;
import com.tugalsan.api.sql.update.server.*;
import com.tugalsan.api.string.client.*;


public class TS_SQLColUtils {

//    final private static TS_Log d = TS_Log.of(TS_SQLColUtils.class);

    public static boolean setPrimaryKey(TS_SQLConnAnchor anchor, CharSequence tableName, CharSequence primaryColumnName) {
        TS_SQLSanitizeUtils.sanitize(tableName);
        TS_SQLSanitizeUtils.sanitize(primaryColumnName);
        var sql = TGS_StringUtils.cmn().concat("ALTER TABLE ", tableName, " ADD PRIMARY KEY ('", primaryColumnName, "')");
        return TS_SQLUpdateStmtUtils.update(anchor, sql).affectedRowCount == 1;
    }

    public static boolean delPrimaryKey(TS_SQLConnAnchor anchor, CharSequence tableName, String primaryColumnName) {
        TS_SQLSanitizeUtils.sanitize(tableName);
        TS_SQLSanitizeUtils.sanitize(primaryColumnName);
        var sql = TGS_StringUtils.cmn().concat("ALTER TABLE ", tableName, " DROP PRIMARY KEY ('", primaryColumnName, "')");
        return TS_SQLUpdateStmtUtils.update(anchor, sql).affectedRowCount == 1;
    }

    public static List<String> names(TS_SQLConnAnchor anchor, CharSequence tableName) {
        return TS_SQLConnColUtils.names(anchor, tableName);
    }

    public static boolean add(TS_SQLConnAnchor anchor, CharSequence tableName, TGS_SQLColTyped newColumnType) {
        var columnName = newColumnType.toString();
        var creationType = TS_SQLConnColUtils.creationType(anchor, newColumnType);
        TS_SQLSanitizeUtils.sanitize(tableName);
        TS_SQLSanitizeUtils.sanitize(columnName);
        TS_SQLSanitizeUtils.sanitize(creationType);
        var sql = TGS_StringUtils.cmn().concat("ALTER TABLE ", tableName, " ADD ", columnName, " ", creationType);
        return TS_SQLUpdateStmtUtils.update(anchor, sql).affectedRowCount == 1;
    }

    public static boolean delete(TS_SQLConnAnchor anchor, CharSequence tableName, CharSequence columnName) {
        TS_SQLSanitizeUtils.sanitize(tableName);
        TS_SQLSanitizeUtils.sanitize(columnName);
        var sql = TGS_StringUtils.cmn().concat("ALTER TABLE ", tableName, " DROP ", columnName);
        return TS_SQLUpdateStmtUtils.update(anchor, sql).affectedRowCount == 1;
    }

    public static boolean rename(TS_SQLConnAnchor anchor, CharSequence tableName, CharSequence oldColumnName, CharSequence newColumnName) {
        var ct = new TGS_SQLColTyped(oldColumnName.toString());
        var creationType = TS_SQLConnColUtils.creationType(anchor, ct);
        TS_SQLSanitizeUtils.sanitize(tableName);
        TS_SQLSanitizeUtils.sanitize(oldColumnName);
        TS_SQLSanitizeUtils.sanitize(newColumnName);
        TS_SQLSanitizeUtils.sanitize(creationType);
        var sql = TGS_StringUtils.cmn().concat("ALTER TABLE ", tableName, " CHANGE ", oldColumnName, " ", newColumnName, " ", creationType);
        return TS_SQLUpdateStmtUtils.update(anchor, sql).affectedRowCount == 1;
    }

    @Deprecated
    public static boolean move(TS_SQLConnAnchor anchor, CharSequence tableName, CharSequence column_to_move, CharSequence column_to_move_after) {
        TS_SQLSanitizeUtils.sanitize(tableName);
        TS_SQLSanitizeUtils.sanitize(column_to_move);
        TS_SQLSanitizeUtils.sanitize(column_to_move_after);
        var sql = TGS_StringUtils.cmn().concat("ALTER TABLE ", tableName, " MODIFY ", column_to_move, " tinyint(1) DEFAULT '0' AFTER ", column_to_move_after);
        return TS_SQLUpdateStmtUtils.update(anchor, sql).affectedRowCount == 1;
    }

    public static List<TS_SQLColTypeInfo> types(TS_SQLConnAnchor anchor, CharSequence tableName) {
        TS_SQLSanitizeUtils.sanitize(tableName);
        List<TS_SQLColTypeInfo> columnInfos = TGS_ListUtils.of();
        TS_SQLDBUtils.meta(anchor, meta -> {
            TGS_FuncMTCUtils.run(() -> {
                try ( var rss = meta.getColumns(null, null, tableName.toString(), null);) {
                    var rs = new TS_SQLResultSet(rss);
                    rs.walkRows(null, ri -> {
                        TGS_FuncMTCUtils.run(() -> {
                            var colInfo = new TS_SQLColTypeInfo();
                            colInfo.COLUMN_NAME = rs.str.get("COLUMN_NAME");
                            colInfo.TYPE_NAME = rs.str.get("TYPE_NAME");
                            colInfo.COLUMN_SIZE = rs.resultSet.getInt("COLUMN_SIZE");
                            colInfo.NULLABLE = rs.resultSet.getInt("NULLABLE") == DatabaseMetaData.columnNullable;
                            colInfo.ORDINAL_POSITION = rs.resultSet.getInt("ORDINAL_POSITION");
                            columnInfos.add(colInfo);
                        });
                    });
                }
            });
        });
        return columnInfos;
    }
}
