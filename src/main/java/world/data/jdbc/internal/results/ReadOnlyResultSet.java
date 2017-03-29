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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;

import static world.data.jdbc.internal.util.Conditions.check;

/**
 * Helper for implementing {@code ResultSet} objects where {@code getConcurrency() == CONCUR_READ_ONLY}.
 */
interface ReadOnlyResultSet extends java.sql.ResultSet {

    @Override
    default int getConcurrency() throws SQLException {
        check(!isClosed(), "Result Set is closed");
        return CONCUR_READ_ONLY;
    }

    @Override
    default void insertRow() throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateRow() throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void deleteRow() throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default boolean rowDeleted() throws SQLException {
        check(!isClosed(), "Result Set is closed");
        return false;
    }

    @Override
    default boolean rowInserted() throws SQLException {
        check(!isClosed(), "Result Set is closed");
        return false;
    }

    @Override
    default boolean rowUpdated() throws SQLException {
        check(!isClosed(), "Result Set is closed");
        return false;
    }

    @Override
    default void cancelRowUpdates() throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void moveToCurrentRow() throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void moveToInsertRow() throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateNull(int columnIndex) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLException("Result set is read-only");
    }

    @Override
    default void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLException("Result set is read-only");
    }
}
