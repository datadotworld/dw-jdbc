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
package world.data.jdbc.internal.results;

import java.sql.SQLException;

import static world.data.jdbc.internal.util.Conditions.check;

/**
 * Helper for implementing {@code ResultSet} objects where {@code getType() == TYPE_FORWARD_ONLY}.
 */
interface ForwardOnlyResultSet extends java.sql.ResultSet {

    @Override
    default int getType() throws SQLException {
        check(!isClosed(), "Result Set is closed");
        return TYPE_FORWARD_ONLY;
    }

    @Override
    default int getFetchDirection() throws SQLException {
        check(!isClosed(), "Result Set is closed");
        return FETCH_FORWARD;
    }

    @Override
    default void setFetchDirection(int direction) throws SQLException {
        check(!isClosed(), "Result Set is closed");
        check(direction == FETCH_FORWARD, "Result Set is forward-only");
    }

    @Override
    default int getRow() throws SQLException {
        check(!isClosed(), "Result Set is closed");
        throw new SQLException("Result Set is forward-only");  // support is optional
    }

    @Override
    default boolean isBeforeFirst() throws SQLException {
        check(!isClosed(), "Result Set is closed");
        throw new SQLException("Result Set is forward-only");  // support is optional
    }

    @Override
    default boolean isAfterLast() throws SQLException {
        check(!isClosed(), "Result Set is closed");
        throw new SQLException("Result Set is forward-only");  // support is optional
    }

    @Override
    default boolean isFirst() throws SQLException {
        check(!isClosed(), "Result Set is closed");
        throw new SQLException("Result Set is forward-only");  // support is optional
    }

    @Override
    default boolean isLast() throws SQLException {
        check(!isClosed(), "Result Set is closed");
        throw new SQLException("Result Set is forward-only");  // support is optional
    }

    @Override
    default void beforeFirst() throws SQLException {
        check(!isClosed(), "Result Set is closed");
        throw new SQLException("Result Set is forward-only");
    }

    @Override
    default void afterLast() throws SQLException {
        check(!isClosed(), "Result Set is closed");
        throw new SQLException("Result Set is forward-only");
    }

    @Override
    default boolean first() throws SQLException {
        check(!isClosed(), "Result Set is closed");
        throw new SQLException("Result Set is forward-only");
    }

    @Override
    default boolean last() throws SQLException {
        check(!isClosed(), "Result Set is closed");
        throw new SQLException("Result Set is forward-only");
    }

    @Override
    default boolean absolute(int row) throws SQLException {
        check(!isClosed(), "Result Set is closed");
        throw new SQLException("Result Set is forward-only");
    }

    @Override
    default boolean relative(int rows) throws SQLException {
        check(!isClosed(), "Result Set is closed");
        throw new SQLException("Result Set is forward-only");
    }

    @Override
    default boolean previous() throws SQLException {
        check(!isClosed(), "Result Set is closed");
        throw new SQLException("Result Set is forward-only");
    }

    @Override
    default void refreshRow() throws SQLException {
        check(!isClosed(), "Result Set is closed");
        throw new SQLException("Result Set is forward-only");
    }
}
