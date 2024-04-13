package com.tugalsan.api.sql.col.server;

import java.sql.*;
import java.util.*;
import com.tugalsan.api.list.client.*;
import com.tugalsan.api.log.server.TS_Log;
import com.tugalsan.api.sql.col.typed.client.*;
import com.tugalsan.api.sql.conn.server.*;
import com.tugalsan.api.sql.db.server.*;
import com.tugalsan.api.sql.resultset.server.*;
import com.tugalsan.api.sql.sanitize.server.*;
import com.tugalsan.api.sql.update.server.*;
import com.tugalsan.api.string.client.*;
import com.tugalsan.api.union.client.TGS_UnionExcuse;
import com.tugalsan.api.union.client.TGS_UnionExcuseVoid;

public class TS_SQLColUtils {

    final private static TS_Log d = TS_Log.of(TS_SQLColUtils.class);

    public static TGS_UnionExcuseVoid setPrimaryKey(TS_SQLConnAnchor anchor, CharSequence tableName, CharSequence primaryColumnName) {
        TS_SQLSanitizeUtils.sanitize(tableName);
        TS_SQLSanitizeUtils.sanitize(primaryColumnName);
        var sql = TGS_StringUtils.concat("ALTER TABLE ", tableName, " ADD PRIMARY KEY ('", primaryColumnName, "')");
        var u_update = TS_SQLUpdateStmtUtils.update(anchor, sql);
        if (u_update.isExcuse()) {
            return u_update.toExcuseVoid();
        }
        if (u_update.value().affectedRowCount() != 1L) {
            return TGS_UnionExcuseVoid.ofExcuse(d.className, "setPrimaryKey", "no row affected");
        }
        return TGS_UnionExcuseVoid.ofVoid();
    }

    public static TGS_UnionExcuseVoid delPrimaryKey(TS_SQLConnAnchor anchor, CharSequence tableName, String primaryColumnName) {
        TS_SQLSanitizeUtils.sanitize(tableName);
        TS_SQLSanitizeUtils.sanitize(primaryColumnName);
        var sql = TGS_StringUtils.concat("ALTER TABLE ", tableName, " DROP PRIMARY KEY ('", primaryColumnName, "')");
        var u_update = TS_SQLUpdateStmtUtils.update(anchor, sql);
        if (u_update.isExcuse()) {
            return u_update.toExcuseVoid();
        }
        if (u_update.value().affectedRowCount() != 1L) {
            return TGS_UnionExcuseVoid.ofExcuse(d.className, "delPrimaryKey", "no row affected");
        }
        return TGS_UnionExcuseVoid.ofVoid();
    }

    public static TGS_UnionExcuse<List<String>> names(TS_SQLConnAnchor anchor, CharSequence tableName) {
        return TS_SQLConnColUtils.names(anchor, tableName);
    }

    public static TGS_UnionExcuseVoid add(TS_SQLConnAnchor anchor, CharSequence tableName, TGS_SQLColTyped newColumnType) {
        var columnName = newColumnType.toString();
        var u_creationType = TS_SQLConnColUtils.creationType(anchor, newColumnType);
        if (u_creationType.isExcuse()) {
            return u_creationType.toExcuseVoid();
        }
        TS_SQLSanitizeUtils.sanitize(tableName);
        TS_SQLSanitizeUtils.sanitize(columnName);
        TS_SQLSanitizeUtils.sanitize(u_creationType.value());
        var sql = TGS_StringUtils.concat("ALTER TABLE ", tableName, " ADD ", columnName, " ", u_creationType.value());
        var u_update = TS_SQLUpdateStmtUtils.update(anchor, sql);
        if (u_update.isExcuse()) {
            return u_update.toExcuseVoid();
        }
        if (u_update.value().affectedRowCount() != 1L) {
            return TGS_UnionExcuseVoid.ofExcuse(d.className, "add", "no row affected");
        }
        return TGS_UnionExcuseVoid.ofVoid();
    }

    public static TGS_UnionExcuseVoid delete(TS_SQLConnAnchor anchor, CharSequence tableName, CharSequence columnName) {
        TS_SQLSanitizeUtils.sanitize(tableName);
        TS_SQLSanitizeUtils.sanitize(columnName);
        var sql = TGS_StringUtils.concat("ALTER TABLE ", tableName, " DROP ", columnName);
        var u_update = TS_SQLUpdateStmtUtils.update(anchor, sql);
        if (u_update.isExcuse()) {
            return u_update.toExcuseVoid();
        }
        if (u_update.value().affectedRowCount() != 1L) {
            return TGS_UnionExcuseVoid.ofExcuse(d.className, "delete", "no row affected");
        }
        return TGS_UnionExcuseVoid.ofVoid();
    }

    public static TGS_UnionExcuseVoid rename(TS_SQLConnAnchor anchor, CharSequence tableName, CharSequence oldColumnName, CharSequence newColumnName) {
        var ct = new TGS_SQLColTyped(oldColumnName.toString());
        var u_creationType = TS_SQLConnColUtils.creationType(anchor, ct);
        if (u_creationType.isExcuse()) {
            return u_creationType.toExcuseVoid();
        }
        TS_SQLSanitizeUtils.sanitize(tableName);
        TS_SQLSanitizeUtils.sanitize(oldColumnName);
        TS_SQLSanitizeUtils.sanitize(newColumnName);
        TS_SQLSanitizeUtils.sanitize(u_creationType);
        var sql = TGS_StringUtils.concat("ALTER TABLE ", tableName, " CHANGE ", oldColumnName, " ", newColumnName, " ", u_creationType.value());
        var u_update = TS_SQLUpdateStmtUtils.update(anchor, sql);
        if (u_update.isExcuse()) {
            return u_update.toExcuseVoid();
        }
        if (u_update.value().affectedRowCount() != 1L) {
            return TGS_UnionExcuseVoid.ofExcuse(d.className, "rename", "no row affected");
        }
        return TGS_UnionExcuseVoid.ofVoid();
    }

    @Deprecated
    public static TGS_UnionExcuseVoid move(TS_SQLConnAnchor anchor, CharSequence tableName, CharSequence column_to_move, CharSequence column_to_move_after) {
        TS_SQLSanitizeUtils.sanitize(tableName);
        TS_SQLSanitizeUtils.sanitize(column_to_move);
        TS_SQLSanitizeUtils.sanitize(column_to_move_after);
        var sql = TGS_StringUtils.concat("ALTER TABLE ", tableName, " MODIFY ", column_to_move, " tinyint(1) DEFAULT '0' AFTER ", column_to_move_after);
        var u_update = TS_SQLUpdateStmtUtils.update(anchor, sql);
        if (u_update.isExcuse()) {
            return u_update.toExcuseVoid();
        }
        if (u_update.value().affectedRowCount() != 1L) {
            return TGS_UnionExcuseVoid.ofExcuse(d.className, "rename", "no row affected");
        }
        return TGS_UnionExcuseVoid.ofVoid();
    }

    public static TGS_UnionExcuse<List<TS_SQLColTypeInfo>> types(TS_SQLConnAnchor anchor, CharSequence tableName) {
        TS_SQLSanitizeUtils.sanitize(tableName);
        List<TS_SQLColTypeInfo> columnInfos = TGS_ListUtils.of();
        var wrap = new Object() {
            TGS_UnionExcuse<String> u_COLUMN_NAME;
            TGS_UnionExcuse<String> u_TYPE_NAME;
            TGS_UnionExcuseVoid u_process_walk_process;
            TGS_UnionExcuseVoid u_walk;
            TGS_UnionExcuseVoid u_meta_process;
            TGS_UnionExcuseVoid u_meta;
        };
        wrap.u_meta = TS_SQLDBUtils.meta(anchor, meta -> {
            try (var rss = meta.getColumns(null, null, tableName.toString(), null);) {
                var rs = new TS_SQLResultSet(rss);
                wrap.u_walk = rs.walkRows(null, ri -> {
                    try {
                        var colInfo = new TS_SQLColTypeInfo();

                        wrap.u_COLUMN_NAME = rs.str.get("COLUMN_NAME");
                        if (wrap.u_COLUMN_NAME.isExcuse()) {
                            return;
                        }
                        colInfo.COLUMN_NAME = wrap.u_COLUMN_NAME.value();

                        wrap.u_TYPE_NAME = rs.str.get("TYPE_NAME");
                        if (wrap.u_TYPE_NAME.isExcuse()) {
                            return;
                        }
                        colInfo.TYPE_NAME = wrap.u_TYPE_NAME.value();

                        colInfo.COLUMN_SIZE = rs.resultSet.getInt("COLUMN_SIZE");

                        colInfo.NULLABLE = rs.resultSet.getInt("NULLABLE") == DatabaseMetaData.columnNullable;

                        colInfo.ORDINAL_POSITION = rs.resultSet.getInt("ORDINAL_POSITION");

                        columnInfos.add(colInfo);

                        wrap.u_process_walk_process = TGS_UnionExcuseVoid.ofVoid();
                    } catch (SQLException ex) {
                        wrap.u_process_walk_process = TGS_UnionExcuseVoid.ofExcuse(ex);
                    }
                });
                wrap.u_meta_process = TGS_UnionExcuseVoid.ofVoid();
            } catch (SQLException ex) {
                wrap.u_meta_process = TGS_UnionExcuseVoid.ofExcuse(ex);
            }
        });
        if (wrap.u_COLUMN_NAME != null && wrap.u_COLUMN_NAME.isExcuse()) {
            return wrap.u_COLUMN_NAME.toExcuse();
        }
        if (wrap.u_TYPE_NAME != null && wrap.u_TYPE_NAME.isExcuse()) {
            return wrap.u_TYPE_NAME.toExcuse();
        }
        if (wrap.u_process_walk_process != null && wrap.u_process_walk_process.isExcuse()) {
            return wrap.u_process_walk_process.toExcuse();
        }
        if (wrap.u_walk != null && wrap.u_walk.isExcuse()) {
            return wrap.u_walk.toExcuse();
        }
        if (wrap.u_meta_process != null && wrap.u_meta_process.isExcuse()) {
            return wrap.u_meta_process.toExcuse();
        }
        if (wrap.u_meta != null && wrap.u_meta.isExcuse()) {
            return wrap.u_meta.toExcuse();
        }
        return TGS_UnionExcuse.of(columnInfos);
    }
}
