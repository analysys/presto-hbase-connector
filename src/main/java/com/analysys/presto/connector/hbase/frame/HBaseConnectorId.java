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
package com.analysys.presto.connector.hbase.frame;

import java.util.Objects;

/**
 * HBase connector id
 *
 * @author wupeng
 * @date 2019/01/29
 */
public final class HBaseConnectorId {

    private final String id;

    public HBaseConnectorId(String id) {
        this.id = Objects.requireNonNull(id, "id is null");
    }

    public String getId() {
        return this.id;
    }

    @Override
    public String toString() {
        return this.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(new Object[]{this.id});
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj != null && this.getClass() == obj.getClass()) {
            HBaseConnectorId other = (HBaseConnectorId) obj;
            return Objects.equals(this.id, other.id);
        } else {
            return false;
        }
    }
}
