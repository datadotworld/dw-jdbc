/*
* dw-jdbc
* Copyright 2016 data.world, Inc.

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
package world.data.jdbc.testing;

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class MoreAssertions {

    public static void assertSQLException(ThrowingCallable callable) {
        assertThatThrownBy(callable).isInstanceOf(SQLException.class);
    }

    public static void assertSQLFeatureNotSupported(ThrowingCallable callable) {
        assertThatThrownBy(callable).isInstanceOf(SQLFeatureNotSupportedException.class);
    }
}
