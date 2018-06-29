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
package world.data.jdbc.internal.util;

import lombok.experimental.UtilityClass;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

@UtilityClass
public final class Conditions {

    public static void check(boolean flag, String message) throws SQLException {
        if (!flag) {
            throw new SQLException(message);
        }
    }

    public static void check(boolean flag, String format, Object... args) throws SQLException {
        if (!flag) {
            throw new SQLException(String.format(format, args));
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

    public static void checkConnectionTransactionIsolation(int level) throws SQLException {
        if (level != Connection.TRANSACTION_NONE &&
                level != Connection.TRANSACTION_READ_UNCOMMITTED &&
                level != Connection.TRANSACTION_READ_COMMITTED &&
                level != Connection.TRANSACTION_REPEATABLE_READ &&
                level != Connection.TRANSACTION_SERIALIZABLE) {
            throw new SQLException(String.format("%d is not a valid transaction isolation level", level));
        }
    }

    public static void checkStatementGeneratedKeys(int generatedKeys) throws SQLException {
        if (generatedKeys != Statement.NO_GENERATED_KEYS &&
                generatedKeys != Statement.RETURN_GENERATED_KEYS) {
            throw new SQLException(String.format("%d is not a valid generated keys value", generatedKeys));
        }
    }

    public static void checkResultSetConcurrency(int concurrency) throws SQLException {
        if (concurrency != ResultSet.CONCUR_READ_ONLY &&
                concurrency != ResultSet.CONCUR_UPDATABLE) {
            throw new SQLException(String.format("%d is not a valid result set concurrency", concurrency));
        }
    }

    public static void checkResultSetDirection(int direction) throws SQLException {
        if (direction != ResultSet.FETCH_FORWARD &&
                direction != ResultSet.FETCH_REVERSE &&
                direction != ResultSet.FETCH_UNKNOWN) {
            throw new SQLException(String.format("%d is not a valid result set fetch direction", direction));
        }
    }

    public static void checkResultSetHoldability(int holdability) throws SQLException {
        if (holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT &&
                holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw new SQLException(String.format("%d is not a valid result set holdability", holdability));
        }
    }

    public static void checkResultSetType(int type) throws SQLException {
        if (type != ResultSet.TYPE_FORWARD_ONLY &&
                type != ResultSet.TYPE_SCROLL_INSENSITIVE &&
                type != ResultSet.TYPE_SCROLL_SENSITIVE) {
            throw new SQLException(String.format("%d is not a valid result set type", type));
        }
    }
}
