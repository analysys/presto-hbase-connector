package com.analysys.presto.connector.hbase.meta;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;

import java.util.Objects;

/**
 * HBase connector column metadata
 *
 * @author wupeng
 * @date 2019/01/29
 */
public class HBaseColumnMetadata extends ColumnMetadata {

    private String family = null;

    public HBaseColumnMetadata(String family, String name, Type type) {
        super(name, type);
        this.family = family;
    }

    public HBaseColumnMetadata(String family, String name, Type type, String comment, boolean hidden) {
        super(name, type, comment, null, hidden);
        this.family = family;
    }

    public HBaseColumnMetadata(String name, Type type, String comment, String extraInfo, boolean hidden) {
        super(name, type, comment, extraInfo, hidden);
    }

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    @Override
    public int hashCode() {
        return Objects.hash(family, getName(), getType(), getComment(), getExtraInfo(), isHidden());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        HBaseColumnMetadata other = (HBaseColumnMetadata) obj;
        return Objects.equals(getName(), other.getName()) &&
                Objects.equals(this.getType(), other.getType()) &&
                Objects.equals(this.getComment(), other.getComment()) &&
                Objects.equals(this.getExtraInfo(), other.getExtraInfo()) &&
                Objects.equals(this.isHidden(), other.isHidden()) &&
                Objects.equals(this.family, other.family);
    }
}