/*
 * dw-jdbc
 * Copyright 2017 data.world, Inc.

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the
 * License.
 *
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * This product includes software developed at data.world, Inc.(http://www.data.world/).
 */
package world.data.jdbc.internal.types;

import org.junit.Test;

import java.sql.JDBCType;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

public class TypeMapTest {
    @Test
    public void testAllJdbcMapped() {
        // This test should fail when new JDBC types are added.  It forces us to either (a) add mappings for
        // the new types to RDF datatypes or (b) explicitly ignore the new type via 'unmap()'
        Collection<Integer> mappedTypes = TypeMap.INSTANCE.getMappedJdbcTypes();
        for (JDBCType jdbcType : JDBCType.values()) {
            assertThat(mappedTypes).as(jdbcType.name()).contains(jdbcType.getVendorTypeNumber());
        }
    }
}
