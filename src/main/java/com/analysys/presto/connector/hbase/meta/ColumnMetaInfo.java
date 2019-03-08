package com.analysys.presto.connector.hbase.meta;

/**
 * Column meta info
 *
 * @author wupeng
 * @date 2019/01/29
 */
public class ColumnMetaInfo {
    private String family = null;
    private String columnName = null;
    private String comment = null;
    private String type = null;
    private boolean isRowKey = false;

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isIsRowKey() {
        return isRowKey;
    }

    public void setIsRowKey(boolean isRowKey) {
        this.isRowKey = isRowKey;
    }

    @Override
    public String toString() {
        return "ColumnMetaInfo{" +
                "family='" + family + '\'' +
                ", columnName='" + columnName + '\'' +
                ", comment='" + comment + '\'' +
                ", type='" + type + '\'' +
                // ", ordinalPosition=" + ordinalPosition +
                ", isIsRowKey=" + isRowKey +
                '}';
    }
}