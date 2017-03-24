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
package world.data.jdbc.util;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class Conditions {

    public static void check(boolean flag, String message) throws SQLException {
        if (!flag) {
            throw new SQLException(message);
        }
    }

    public static void checkSupported(boolean flag) throws SQLException {
        if (!flag) {
            throw new SQLFeatureNotSupportedException();
        }
    }

    public static void checkSupported(boolean flag, String message) throws SQLException {
        if (!flag) {
            throw new SQLFeatureNotSupportedException(message);
        }
    }
}
