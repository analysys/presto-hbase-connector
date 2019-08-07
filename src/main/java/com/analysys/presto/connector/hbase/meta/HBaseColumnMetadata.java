/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.analysys.presto.connector.hbase.meta;

import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.Type;

import java.util.Objects;

/**
 * HBase connector column metadata
 *
 * @author wupeng
 * @date 2019/01/29
 */
public class HBaseColumnMetadata extends ColumnMetadata {

    private boolean rowKey;
    private String family;

    public HBaseColumnMetadata(String family, String name, Type type, boolean rowKey) {
        super(name, type);
        this.family = family;
        this.rowKey = rowKey;
    }

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    public boolean isRowKey() {
        return rowKey;
    }

    public void setRowKey(boolean rowKey) {
        this.rowKey = rowKey;
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