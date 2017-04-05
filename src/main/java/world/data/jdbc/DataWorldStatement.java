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
package world.data.jdbc;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Extends {@link Statement} with data.world-specific extensions.
 */
public interface DataWorldStatement extends Statement {

    /**
     * Gets the JDBC compatibility level that is in use, see
     * {@link JdbcCompatibility} for explanations
     * <p>
     * By default this is set at the connection level and inherited, however you
     * may call {@link #setJdbcCompatibilityLevel(JdbcCompatibility)} to set the compatibility
     * level for this statement. This allows you to change the compatibility
     * level on a per-query basis if so desired.
     * </p>
     *
     * @return Compatibility level
     */
    JdbcCompatibility getJdbcCompatibilityLevel() throws SQLException;

    /**
     * Sets the JDBC compatibility level that is in use, see
     * {@link JdbcCompatibility} for explanations.
     * <p>
     * By default this is set at the connection level and inherited, however you
     * may call the {@code setJdbcCompatibilityLevel} method to set the compatibility
     * level for this statement. This allows you to change the compatibility
     * level on a per-query basis if so desired.
     * </p>
     * <p>
     * Changing the level may not effect existing open objects, behaviour in
     * this case will be implementation specific.
     * </p>
     *
     * @param compatibilityLevel Compatibility level
     */
    void setJdbcCompatibilityLevel(JdbcCompatibility compatibilityLevel) throws SQLException;

    @Override
    DataWorldConnection getConnection() throws SQLException;
}
