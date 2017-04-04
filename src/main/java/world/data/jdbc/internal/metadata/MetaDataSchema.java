/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package world.data.jdbc.internal.metadata;

import lombok.experimental.UtilityClass;
import world.data.jdbc.internal.results.ResultSetImpl;
import world.data.jdbc.model.Iri;
import world.data.jdbc.model.LiteralFactory;
import world.data.jdbc.model.Node;
import world.data.jdbc.vocab.Xsd;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import static java.sql.ResultSetMetaData.columnNoNulls;
import static java.sql.ResultSetMetaData.columnNullable;

/**
 * Helper class containing constants pertaining to the columns returned by various methods of a
 * {@link DatabaseMetaData} implementation
 */
@UtilityClass
class MetaDataSchema {

    /**
     * Columns for the {@link DatabaseMetaData#getAttributes(String, String, String, String)} method.
     */
    static final ColumnInfo[] ATTRIBUTE_COLUMNS;

    /**
     * Columns for the {@link DatabaseMetaData#getBestRowIdentifier(String, String, String, int, boolean)} method.
     */
    static final ColumnInfo[] BEST_ROW_IDENTIFIER_COLUMNS;

    /**
     * Columns for the  {@link DatabaseMetaData#getCatalogs()} method.
     */
    static final ColumnInfo[] CATALOG_COLUMNS;

    /**
     * Columns for the {@link DatabaseMetaData#getClientInfoProperties()} method.
     */
    static final ColumnInfo[] CLIENT_INFO_PROPERTY_COLUMNS;

    /**
     * Columns for the {@link DatabaseMetaData#getColumns(String, String, String, String)} method.
     */
    static final ColumnInfo[] COLUMN_COLUMNS;

    /**
     * Columns for the {@link DatabaseMetaData#getColumnPrivileges(String, String, String, String)} method.
     */
    static final ColumnInfo[] COLUMN_PRIVILEGE_COLUMNS;

    /**
     * Columns for the {@link DatabaseMetaData#getCrossReference(String, String, String, String, String, String)} method.
     */
    static final ColumnInfo[] CROSS_REFERENCE_COLUMNS;

    /**
     * Columns for the {@link DatabaseMetaData#getExportedKeys(String, String, String)} method.
     */
    static final ColumnInfo[] EXPORTED_KEY_COLUMNS;

    /**
     * Columns for the {@link DatabaseMetaData#getFunctionColumns(String, String, String, String)} method.
     */
    static final ColumnInfo[] FUNCTION_COLUMN_COLUMNS;

    /**
     * Columns for the {@link DatabaseMetaData#getFunctions(String, String, String)} method.
     */
    static final ColumnInfo[] FUNCTION_COLUMNS;

    /**
     * Columns for the {@link DatabaseMetaData#getImportedKeys(String, String, String)} method.
     */
    static final ColumnInfo[] IMPORTED_KEY_COLUMNS;

    /**
     * Columns for the {@link DatabaseMetaData#getIndexInfo(String, String, String, boolean, boolean)} method.
     */
    static final ColumnInfo[] INDEX_INFO_COLUMNS;

    /**
     * Columns for the {@link DatabaseMetaData#getPrimaryKeys(String, String, String)} method.
     */
    static final ColumnInfo[] PRIMARY_KEY_COLUMNS;

    /**
     * Columns for the {@link DatabaseMetaData#getProcedureColumns(String, String, String, String)} method.
     */
    static final ColumnInfo[] PROCEDURE_COLUMN_COLUMNS;

    /**
     * Columns for the {@link DatabaseMetaData#getProcedures(String, String, String)} method.
     */
    static final ColumnInfo[] PROCEDURE_COLUMNS;

    /**
     * Columns for the {@link DatabaseMetaData#getPseudoColumns(String, String, String, String)} method.
     */
    static final ColumnInfo[] PSUEDO_COLUMN_COLUMNS;

    /**
     * Columns for the  {@link DatabaseMetaData#getSchemas()} method.
     */
    static final ColumnInfo[] SCHEMA_COLUMNS;

    /**
     * Columns for the {@link DatabaseMetaData#getSuperTables(String, String, String)} method.
     */
    static final ColumnInfo[] SUPER_TABLE_COLUMNS;

    /**
     * Columns for the {@link DatabaseMetaData#getSuperTypes(String, String, String)} method.
     */
    static final ColumnInfo[] SUPER_TYPE_COLUMNS;

    /**
     * Columns for the {@link DatabaseMetaData#getTablePrivileges(String, String, String)} method.
     */
    static final ColumnInfo[] TABLE_PRIVILEGE_COLUMNS;

    /**
     * Columns for the  {@link DatabaseMetaData#getTableTypes()} method.
     */
    static final ColumnInfo[] TABLE_TYPE_COLUMNS;

    /**
     * Columns for the {@link DatabaseMetaData#getTables(String, String, String, String[])} method.
     */
    static final ColumnInfo[] TABLE_COLUMNS;

    /**
     * Columns for the  {@link DatabaseMetaData#getTypeInfo()} method.
     */
    static final ColumnInfo[] TYPE_INFO_COLUMNS;

    /**
     * Columns for the {@link DatabaseMetaData#getUDTs(String, String, String, int[])} method.
     */
    static final ColumnInfo[] UDT_COLUMNS;

    /**
     * Columns for the {@link DatabaseMetaData#getVersionColumns(String, String, String)} method.
     */
    static final ColumnInfo[] VERSION_COLUMNS;

    static {
        // Define all the columns we are going to use since some of these
        // appear in multiple schema
        ColumnInfo empty = ColumnFactory.builder("", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo typeCat = ColumnFactory.builder("TYPE_CATA", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo typeSchema = ColumnFactory.builder("TYPE_SCHEM", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo typeName = ColumnFactory.builder("TYPE_NAME", Xsd.STRING).nullable(columnNoNulls).build();
        ColumnInfo attrName = ColumnFactory.builder("ATTR_NAME", Xsd.STRING).nullable(columnNoNulls).build();
        ColumnInfo dataType = ColumnFactory.builder("DATA_TYPE", Xsd.INT).nullable(columnNoNulls).build();
        ColumnInfo attrTypeName = ColumnFactory.builder("ATTR_TYPE_NAME", Xsd.STRING).nullable(columnNoNulls).build();
        ColumnInfo attrSize = ColumnFactory.builder("ATTR_SIZE", Xsd.INT).nullable(columnNoNulls).build();
        ColumnInfo decimalDigits = ColumnFactory.builder("DECIMAL_DIGITS", Xsd.INT).nullable(columnNoNulls).build();
        ColumnInfo numPrecRadix = ColumnFactory.builder("NUM_PREC_RADIX", Xsd.INT).nullable(columnNoNulls).build();
        ColumnInfo nullable = ColumnFactory.builder("NULLABLE", Xsd.INT).nullable(columnNoNulls).build();
        ColumnInfo shortNullable = ColumnFactory.builder("NULLABLE", Xsd.SHORT).nullable(columnNoNulls).build();
        ColumnInfo remarks = ColumnFactory.builder("REMARKS", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo attrDef = ColumnFactory.builder("ATTR_DEF", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo sqlDataType = ColumnFactory.builder("SQL_DATA_TYPE", Xsd.INT).nullable(columnNoNulls).build();
        ColumnInfo sqlDateTimeSub = ColumnFactory.builder("SQL_DATETIME_SUB", Xsd.INT).nullable(columnNoNulls).build();
        ColumnInfo charOctetLength = ColumnFactory.builder("CHAR_OCTET_LENGTH", Xsd.INT).nullable(columnNoNulls).build();
        ColumnInfo ordinalPosition = ColumnFactory.builder("ORDINAL_POSITION", Xsd.INT).nullable(columnNoNulls).build();
        ColumnInfo isNullable = ColumnFactory.builder("IS_NULLABLE", Xsd.STRING).nullable(columnNoNulls).build();
        ColumnInfo scope = ColumnFactory.builder("SCOPE", Xsd.SHORT).nullable(columnNoNulls).build();
        ColumnInfo scopeCatalog = ColumnFactory.builder("SCOPE_CATALOG", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo scopeSchema = ColumnFactory.builder("SCOPE_SCHEMA", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo scopeTable = ColumnFactory.builder("SCOPE_TABLE", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo sourceDataType = ColumnFactory.builder("SOURCE_DATA_TYPE", Xsd.SHORT).nullable(columnNullable).build();
        ColumnInfo columnName = ColumnFactory.builder("COLUMN_NAME", Xsd.STRING).nullable(columnNoNulls).build();
        ColumnInfo columnSize = ColumnFactory.builder("COLUMN_SIZE", Xsd.INT).nullable(columnNoNulls).build();
        ColumnInfo columnDef = ColumnFactory.builder("COLUMN_DEF", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo bufferLength = ColumnFactory.builder("BUFFER_LENGTH", Xsd.INT).nullable(columnNoNulls).build();
        ColumnInfo psuedoColumn = ColumnFactory.builder("PSUEDO_COLUMN", Xsd.SHORT).nullable(columnNoNulls).build();
        ColumnInfo tableCat = ColumnFactory.builder("TABLE_CAT", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo tableCatalog = ColumnFactory.builder("TABLE_CATALOG", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo tableSchema = ColumnFactory.builder("TABLE_SCHEM", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo tableName = ColumnFactory.builder("TABLE_NAME", Xsd.STRING).nullable(columnNoNulls).build();
        ColumnInfo name = ColumnFactory.builder("NAME", Xsd.STRING).nullable(columnNoNulls).build();
        ColumnInfo maxLen = ColumnFactory.builder("MAX_LEN", Xsd.INT).nullable(columnNoNulls).build();
        ColumnInfo defaultValue = ColumnFactory.builder("DEFAULT_VALUE", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo description = ColumnFactory.builder("DESCRIPTION", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo isAutoIncrement = ColumnFactory.builder("IS_AUTOINCREMENT", Xsd.STRING).nullable(columnNoNulls).build();
        ColumnInfo className = ColumnFactory.builder("CLASS_NAME", Xsd.STRING).nullable(columnNoNulls).build();
        ColumnInfo baseType = ColumnFactory.builder("BASE_TYPE", Xsd.SHORT).nullable(columnNullable).build();
        ColumnInfo grantor = ColumnFactory.builder("GRANTOR", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo grantee = ColumnFactory.builder("GRANTEE", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo privilege = ColumnFactory.builder("PRIVILEGE", Xsd.STRING).nullable(columnNoNulls).build();
        ColumnInfo isGrantable = ColumnFactory.builder("IS_GRANTABLE", Xsd.STRING).nullable(columnNoNulls).build();
        ColumnInfo pkTableCat = ColumnFactory.builder("PKTABLE_CAT", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo pkTableSchema = ColumnFactory.builder("PKTABLE_SCHEM", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo pkTableName = ColumnFactory.builder("PKTABLE_NAME", Xsd.STRING).nullable(columnNoNulls).build();
        ColumnInfo pkColumnName = ColumnFactory.builder("PKCOLUMN_NAME", Xsd.STRING).nullable(columnNoNulls).build();
        ColumnInfo fkTableCat = ColumnFactory.builder("FKTABLE_CAT", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo fkTableSchema = ColumnFactory.builder("FKTABLE_SCHEM", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo fkTableName = ColumnFactory.builder("FKTABLE_NAME", Xsd.STRING).nullable(columnNoNulls).build();
        ColumnInfo fkColumnName = ColumnFactory.builder("FKCOLUMN_NAME", Xsd.STRING).nullable(columnNoNulls).build();
        ColumnInfo keySeq = ColumnFactory.builder("KEY_SEQ", Xsd.SHORT).nullable(columnNoNulls).build();
        ColumnInfo updateRule = ColumnFactory.builder("UPDATE_RULE", Xsd.SHORT).nullable(columnNoNulls).build();
        ColumnInfo deleteRule = ColumnFactory.builder("DELETE_RULE", Xsd.SHORT).nullable(columnNoNulls).build();
        ColumnInfo fkName = ColumnFactory.builder("FK_NAME", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo pkName = ColumnFactory.builder("PK_NAME", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo deferrability = ColumnFactory.builder("DEFERRABILITY", Xsd.SHORT).nullable(columnNoNulls).build();
        ColumnInfo functionCat = ColumnFactory.builder("FUNCTION_CAT", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo functionSchema = ColumnFactory.builder("FUNCTION_SCHEM", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo functionName = ColumnFactory.builder("FUNCTION_NAME", Xsd.STRING).nullable(columnNoNulls).build();
        ColumnInfo columnType = ColumnFactory.builder("COLUMN_TYPE", Xsd.SHORT).nullable(columnNoNulls).build();
        ColumnInfo precision = ColumnFactory.builder("PRECISION", Xsd.INT).nullable(columnNoNulls).build();
        ColumnInfo length = ColumnFactory.builder("LENGTH", Xsd.INT).nullable(columnNoNulls).build();
        ColumnInfo scale = ColumnFactory.builder("SCALE", Xsd.SHORT).nullable(columnNoNulls).build();
        ColumnInfo radix = ColumnFactory.builder("RADIX", Xsd.SHORT).nullable(columnNoNulls).build();
        ColumnInfo specificName = ColumnFactory.builder("SPECIFIC_NAME", Xsd.STRING).nullable(columnNoNulls).build();
        ColumnInfo functionType = ColumnFactory.builder("FUNCTION_TYPE", Xsd.SHORT).nullable(columnNoNulls).build();
        ColumnInfo nonUnique = ColumnFactory.builder("NON_UNIQUE", Xsd.BOOLEAN).nullable(columnNoNulls).build();
        ColumnInfo indexQualifier = ColumnFactory.builder("INDEX_QUALIFIER", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo indexName = ColumnFactory.builder("INDEX_NAME", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo type = ColumnFactory.builder("TYPE", Xsd.SHORT).nullable(columnNoNulls).build();
        ColumnInfo ascOrDesc = ColumnFactory.builder("ASC_OR_DESC", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo cardinality = ColumnFactory.builder("CARDINALITY", Xsd.INT).nullable(columnNoNulls).build();
        ColumnInfo pages = ColumnFactory.builder("PAGES", Xsd.INT).nullable(columnNoNulls).build();
        ColumnInfo filterCondition = ColumnFactory.builder("FILTER_CONDITION", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo procedureCat = ColumnFactory.builder("PROCEDURE_CAT", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo procedureSchema = ColumnFactory.builder("PROCEDURE_SCHEM", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo procedureName = ColumnFactory.builder("PROCEDURE_NAME", Xsd.STRING).nullable(columnNoNulls).build();
        ColumnInfo procedureType = ColumnFactory.builder("PROCEDURE_TYPE", Xsd.SHORT).nullable(columnNoNulls).build();
        ColumnInfo superTableName = ColumnFactory.builder("SUPERTABLE_NAME", Xsd.STRING).nullable(columnNoNulls).build();
        ColumnInfo superTypeCat = ColumnFactory.builder("SUPERTYPE_CAT", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo superTypeSchema = ColumnFactory.builder("SUPERTYPE_SCHEM", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo superTypeName = ColumnFactory.builder("SUPERTYPE_NAME", Xsd.STRING).nullable(columnNoNulls).build();
        ColumnInfo litPrefix = ColumnFactory.builder("LITERAL_PREFIX", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo litSuffix = ColumnFactory.builder("LITERAL_SUFFIX", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo createParams = ColumnFactory.builder("CREATE_PARAMS", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo caseSensitive = ColumnFactory.builder("CASE_SENSITIVE", Xsd.BOOLEAN).nullable(columnNoNulls).build();
        ColumnInfo searchable = ColumnFactory.builder("SEARCHABLE", Xsd.SHORT).nullable(columnNoNulls).build();
        ColumnInfo unsignedAttr = ColumnFactory.builder("UNSIGNED_ATTRIBUTE", Xsd.BOOLEAN).nullable(columnNoNulls).build();
        ColumnInfo fixedPrecScale = ColumnFactory.builder("FIXED_PREC_SCALE", Xsd.BOOLEAN).nullable(columnNoNulls).build();
        ColumnInfo autoIncrement = ColumnFactory.builder("AUTO_INCREMENT", Xsd.BOOLEAN).nullable(columnNoNulls).build();
        ColumnInfo localTypeName = ColumnFactory.builder("LOCAL_TYPE_NAME", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo minScale = ColumnFactory.builder("MINIMUM_SCALE", Xsd.SHORT).nullable(columnNoNulls).build();
        ColumnInfo maxScale = ColumnFactory.builder("MAXIMUM_SCALE", Xsd.SHORT).nullable(columnNullable).build();
        ColumnInfo tableType = ColumnFactory.builder("TABLE_TYPE", Xsd.STRING).nullable(columnNoNulls).build();
        ColumnInfo selfRefColName = ColumnFactory.builder("SELF_REFERENCING_COL_NAME", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo refGeneration = ColumnFactory.builder("REF_GENERATION", Xsd.STRING).nullable(columnNullable).build();
        ColumnInfo columnUsage = ColumnFactory.builder("COLUMN_USAGE", Xsd.STRING).nullable(columnNoNulls).build();

        ATTRIBUTE_COLUMNS = new ColumnInfo[]{
                // TYPE_CAT String => type catalog (may be null)
                typeCat,
                // TYPE_SCHEM String => type schema (may be null)
                typeSchema,
                // TYPE_NAME String => type name
                typeName,
                // ATTR_NAME String => attribute name
                attrName,
                // DATA_TYPE int => attribute type SQL type from
                // java.sql.Types
                dataType,
                // ATTR_TYPE_NAME String => Data source dependent type name.
                // For a UDT, the type name is fully qualified. For a REF,
                // the type name is fully qualified and represents the
                // target type of the reference type.
                attrTypeName,
                // ATTR_SIZE int => column size. For char or date types this
                // is the maximum number of characters; for numeric or
                // decimal types this is precision.
                attrSize,
                // DECIMAL_DIGITS int => the number of fractional digits.
                // Null is returned for data types where DECIMAL_DIGITS is
                // not applicable.
                decimalDigits,
                // NUM_PREC_RADIX int => Radix (typically either 10 or 2)
                numPrecRadix,
                // NULLABLE int => whether NULL is allowed
                // attributeNoNulls - might not allow NULL values
                // attributeNullable - definitely allows NULL values
                // attributeNullableUnknown - nullability unknown
                nullable,
                // REMARKS String => comment describing column (may be null)
                remarks,
                // ATTR_DEF String => default value (may be null)
                attrDef,
                // SQL_DATA_TYPE int => unused
                sqlDataType,
                // SQL_DATETIME_SUB int => unused
                sqlDateTimeSub,
                // CHAR_OCTET_LENGTH int => for char types the maximum
                // number of bytes in the column
                charOctetLength,
                // ORDINAL_POSITION int => index of the attribute in the UDT
                // (starting at 1)
                ordinalPosition,
                // IS_NULLABLE String => ISO rules are used to determine the
                // nullability for a attribute.
                // YES --- if the attribute can include NULLs
                // NO --- if the attribute cannot include NULLs
                // empty string --- if the nullability for the attribute is
                // unknown
                isNullable,
                // SCOPE_CATALOG String => catalog of table that is the
                // scope of a
                // reference attribute (null if DATA_TYPE isn't REF)
                scopeCatalog,
                // SCOPE_SCHEMA String => schema of table that is the scope
                // of a reference attribute (null if DATA_TYPE isn't REF)
                scopeSchema,
                // SCOPE_TABLE String => table name that is the scope of a
                // referenceattribute (null if the DATA_TYPE isn't REF)
                scopeTable,
                // SOURCE_DATA_TYPE short => source type of a distinct type
                // or user-generated Ref type,SQL type from java.sql.Types
                // (null if DATA_TYPE isn't DISTINCT or user-generated REF)
                sourceDataType
        };

        BEST_ROW_IDENTIFIER_COLUMNS = new ColumnInfo[]{
                // SCOPE short => actual scope of result
                // bestRowTemporary - very temporary, while using row
                // bestRowTransaction - valid for remainder of current
                // transaction
                // bestRowSession - valid for remainder of current session
                scope,
                // COLUMN_NAME String => column name
                columnName,
                // DATA_TYPE int => SQL data type from java.sql.Types
                dataType,
                // TYPE_NAME String => Data source dependent type name, for
                // a UDT the type name is fully qualified
                typeName,
                // COLUMN_SIZE int => precision
                columnSize,
                // BUFFER_LENGTH int => not used
                bufferLength,
                // DECIMAL_DIGITS short => scale - Null is returned for data
                // types where DECIMAL_DIGITS is not applicable.
                decimalDigits,
                // PSEUDO_COLUMN short => is this a pseudo column like an
                // Oracle ROWID
                // bestRowUnknown - may or may not be pseudo column
                // bestRowNotPseudo - is NOT a pseudo column
                // bestRowPseudo - is a pseudo column
                psuedoColumn
        };

        CATALOG_COLUMNS = new ColumnInfo[]{
                // TABLE_CAT String => catalog name
                tableCat
        };

        CLIENT_INFO_PROPERTY_COLUMNS = new ColumnInfo[]{
                // NAME String=> The name of the client info property
                name,
                // MAX_LEN int=> The maximum length of the value for the
                // property
                maxLen,
                // DEFAULT_VALUE String=> The default value of the property
                defaultValue,
                // DESCRIPTION String=> A description of the property. This
                // will typically contain information as to where this
                // property is stored in the database.
                description
        };

        COLUMN_COLUMNS = new ColumnInfo[]{
                // TABLE_CAT String => table catalog (may be null)
                tableCat,
                // TABLE_SCHEM String => table schema (may be null)
                tableSchema,
                // TABLE_NAME String => table name
                tableName,
                // COLUMN_NAME String => column name
                columnName,
                // DATA_TYPE int => SQL type from java.sql.Types
                dataType,
                // TYPE_NAME String => Data source dependent type name, for
                // a UDT
                // the type name is fully qualified
                typeName,
                // COLUMN_SIZE int => column size.
                columnSize,
                // BUFFER_LENGTH is not used.
                bufferLength,
                // DECIMAL_DIGITS int => the number of fractional digits.
                // Null is
                // returned for data types where DECIMAL_DIGITS is not
                // applicable.
                decimalDigits,
                // NUM_PREC_RADIX int => Radix (typically either 10 or 2)
                numPrecRadix,
                // NULLABLE int => is NULL allowed.
                // columnNoNulls - might not allow NULL values
                // columnNullable - definitely allows NULL values
                // columnNullableUnknown - nullability unknown
                nullable,
                // REMARKS String => comment describing column (may be
                // null),
                remarks,
                // COLUMN_DEF String => default value for the column, which
                // should
                // be interpreted as a string when the value is enclosed in
                // single
                // quotes (may be null)
                columnDef,
                // SQL_DATA_TYPE int => unused
                sqlDataType,
                // SQL_DATETIME_SUB int => unused
                sqlDateTimeSub,
                // CHAR_OCTET_LENGTH int => for char types the maximum
                // number of
                // bytes in the column
                charOctetLength,
                // ORDINAL_POSITION int => index of column in table
                // (starting at 1)
                ordinalPosition,
                // IS_NULLABLE String => ISO rules are used to determine the
                // nullability for a column.
                // YES --- if the parameter can include NULLs
                // NO --- if the parameter cannot include NULLs
                // empty string --- if the nullability for the parameter is
                // unknown
                isNullable,
                // SCOPE_CATLOG String => catalog of table that is the scope
                // of a
                // reference attribute (null if DATA_TYPE isn't REF)
                scopeCatalog,
                // SCOPE_SCHEMA String => schema of table that is the scope
                // of a
                // reference attribute (null if the DATA_TYPE isn't REF)
                scopeSchema,
                // SCOPE_TABLE String => table name that this the scope of a
                // reference attribure (null if the DATA_TYPE isn't REF)
                scopeTable,
                // SOURCE_DATA_TYPE short => source type of a distinct type
                // or
                // user-generated Ref type, SQL type from java.sql.Types
                // (null if
                // DATA_TYPE isn't DISTINCT or user-generated REF)
                sourceDataType,
                // IS_AUTOINCREMENT String => Indicates whether this column
                // is auto
                // incremented
                // YES --- if the column is auto incremented
                // NO --- if the column is not auto incremented
                // empty string --- if it cannot be determined whether the
                // column is
                // auto incremented parameter is unknown
                isAutoIncrement
        };

        COLUMN_PRIVILEGE_COLUMNS = new ColumnInfo[]{
                // TABLE_CAT String => table catalog (may be null)
                tableCat,
                // TABLE_SCHEM String => table schema (may be null)
                tableSchema,
                // TABLE_NAME String => table name
                tableName,
                // COLUMN_NAME String => column name
                columnName,
                // GRANTOR String => grantor of access (may be null)
                grantor,
                // GRANTEE String => grantee of access
                grantee,
                // PRIVILEGE String => name of access (SELECT, INSERT,
                // UPDATE, REFRENCES, ...)
                privilege,
                // IS_GRANTABLE String => "YES" if grantee is permitted to
                // grant to others; "NO" if not; null if unknown
                isGrantable
        };

        CROSS_REFERENCE_COLUMNS = new ColumnInfo[]{
                // PKTABLE_CAT String => parent key table catalog (may be
                // null)
                pkTableCat,
                // PKTABLE_SCHEM String => parent key table schema (may be
                // null)
                pkTableSchema,
                // PKTABLE_NAME String => parent key table name
                pkTableName,
                // PKCOLUMN_NAME String => parent key column name
                pkColumnName,
                // FKTABLE_CAT String => foreign key table catalog (may be
                // null)
                // being exported (may be null)
                fkTableCat,
                // FKTABLE_SCHEM String => foreign key table schema (may be
                // null) being exported (may be null)
                fkTableSchema,
                // FKTABLE_NAME String => foreign key table name being
                // exported
                fkTableName,
                // FKCOLUMN_NAME String => foreign key column name being
                // exported
                fkColumnName,
                // KEY_SEQ short => sequence number within foreign key( a
                // value of 1 represents the first column of the foreign
                // key, a value of 2 would represent the second column
                // within the foreign key).
                keySeq,
                // UPDATE_RULE short => What happens to foreign key when
                // parent key is updated:
                // importedNoAction - do not allow update of parent key if
                // it has been imported
                // importedKeyCascade - change imported key to agree with
                // parent key update
                // importedKeySetNull - change imported key to NULL if its
                // parent key has been updated
                // importedKeySetDefault - change imported key to default
                // values if its parent key has been updated
                // importedKeyRestrict - same as importedKeyNoAction (for
                // ODBC 2.x compatibility)
                updateRule,
                // DELETE_RULE short => What happens to the foreign key when
                // parent key is deleted.
                // importedKeyNoAction - do not allow delete of parent key
                // if it has been imported
                // importedKeyCascade - delete rows that import a deleted
                // key
                // importedKeySetNull - change imported key to NULL if its
                // primary key has been deleted
                // importedKeyRestrict - same as importedKeyNoAction (for
                // ODBC 2.x compatibility)
                // importedKeySetDefault - change imported key to default if
                // its parent key has been deleted
                deleteRule,
                // FK_NAME String => foreign key name (may be null)
                fkName,
                // PK_NAME String => parent key name (may be null)
                pkName,
                // DEFERRABILITY short => can the evaluation of foreign key
                // constraints be deferred until commit
                // importedKeyInitiallyDeferred - see SQL92 for definition
                // importedKeyInitiallyImmediate - see SQL92 for definition
                // importedKeyNotDeferrable - see SQL92 for definition
                deferrability
        };

        EXPORTED_KEY_COLUMNS = new ColumnInfo[]{
                // PKTABLE_CAT String => primary key table catalog (may be
                // null)
                pkTableCat,
                // PKTABLE_SCHEM String => primary key table schema (may be
                // null)
                pkTableSchema,
                // PKTABLE_NAME String => primary key table name
                pkTableName,
                // PKCOLUMN_NAME String => primary key column name
                pkColumnName,
                // FKTABLE_CAT String => foreign key table catalog (may be
                // null) being exported (may be null)
                fkTableCat,
                // FKTABLE_SCHEM String => foreign key table schema (may be
                // null) being exported (may be null)
                fkTableSchema,
                // FKTABLE_NAME String => foreign key table name being
                // exported
                fkTableName,
                // FKCOLUMN_NAME String => foreign key column name being
                // exported
                fkColumnName,
                // KEY_SEQ short => sequence number within foreign key( a
                // value of 1 represents the first column of the foreign
                // key, a value of 2 would represent the second column
                // within the foreign key).
                keySeq,
                // UPDATE_RULE short => What happens to foreign key when
                // primary is updated:
                // importedNoAction - do not allow update of primary key if
                // it has been imported
                // importedKeyCascade - change imported key to agree with
                // primary key update
                // importedKeySetNull - change imported key to NULL if its
                // primary key has been updated
                // importedKeySetDefault - change imported key to default
                // values if its primary key has been updated
                // importedKeyRestrict - same as importedKeyNoAction (for
                // ODBC 2.x compatibility)
                updateRule,
                // DELETE_RULE short => What happens to the foreign key when
                // primary is deleted.
                // importedKeyNoAction - do not allow delete of primary key
                // if it has been imported
                // importedKeyCascade - delete rows that import a deleted
                // key
                // importedKeySetNull - change imported key to NULL if its
                // primary key has been deleted
                // importedKeyRestrict - same as importedKeyNoAction (for
                // ODBC 2.x compatibility)
                // importedKeySetDefault - change imported key to default if
                // its primary key has been deleted
                deleteRule,
                // FK_NAME String => foreign key name (may be null)
                fkName,
                // PK_NAME String => primary key name (may be null)
                pkName,
                // DEFERRABILITY short => can the evaluation of foreign key
                // constraints be deferred until commit
                // importedKeyInitiallyDeferred - see SQL92 for definition
                // importedKeyInitiallyImmediate - see SQL92 for definition
                // importedKeyNotDeferrable - see SQL92 for definition
                deferrability
        };

        FUNCTION_COLUMN_COLUMNS = new ColumnInfo[]{
                // FUNCTION_CAT String => function catalog (may be null)
                functionCat,
                // FUNCTION_SCHEM String => function schema (may be null)
                functionSchema,
                // FUNCTION_NAME String => function name. This is the name
                // used to invoke the function
                functionName,
                // COLUMN_NAME String => column/parameter name
                columnName,
                // COLUMN_TYPE Short => kind of column/parameter:
                // functionColumnUnknown - nobody knows
                // functionColumnIn - IN parameter
                // functionColumnInOut - INOUT parameter
                // functionColumnOut - OUT parameter
                // functionColumnReturn - function return value
                // functionColumnResult - Indicates that the parameter or
                // column is a column in the ResultSet
                columnType,
                // DATA_TYPE int => SQL type from java.sql.Types
                dataType,
                // TYPE_NAME String => SQL type name, for a UDT type the
                // type name is fully qualified
                typeName,
                // PRECISION int => precision
                precision,
                // LENGTH int => length in bytes of data
                length,
                // SCALE short => scale - null is returned for data types
                // where SCALE is not applicable.
                scale,
                // RADIX short => radix
                radix,
                // NULLABLE short => can it contain NULL.
                // functionNoNulls - does not allow NULL values
                // functionNullable - allows NULL values
                // functionNullableUnknown - nullability unknown
                shortNullable,
                // REMARKS String => comment describing column/parameter
                remarks,
                // CHAR_OCTET_LENGTH int => the maximum length of binary and
                // character based parameters or columns. For any other
                // datatype the returned value is a NULL
                charOctetLength,
                // ORDINAL_POSITION int => the ordinal position, starting
                // from 1, for the input and output parameters. A value of 0
                // is returned if this row describes the function's return
                // value. For result set columns, it is the ordinal position
                // of the column in the result set starting from 1.
                ordinalPosition,
                // IS_NULLABLE String => ISO rules are used to determine the
                // nullability for a parameter or column.
                // YES --- if the parameter or column can include NULLs
                // NO --- if the parameter or column cannot include NULLs
                // empty string --- if the nullability for the parameter or
                // column is unknown
                isNullable,
                // SPECIFIC_NAME String => the name which uniquely
                // identifies this function within its schema. This is a
                // user specified, or DBMS generated, name that may be
                // different then the FUNCTION_NAME for example with
                // overload functions
                specificName
        };

        FUNCTION_COLUMNS = new ColumnInfo[]{
                // FUNCTION_CAT String => function catalog (may be null)
                functionCat,
                // FUNCTION_SCHEM String => function schema (may be null)
                functionSchema,
                // FUNCTION_NAME String => function name. This is the name
                // used to invoke the function
                functionName,
                // REMARKS String => explanatory comment on the function
                remarks,
                // FUNCTION_TYPE short => kind of function:
                // functionResultUnknown - Cannot determine if a return
                // value or table will be returned
                // functionNoTable- Does not return a table
                // functionReturnsTable - Returns a table
                functionType,
                // SPECIFIC_NAME String => the name which uniquely
                // identifies this function within its schema. This is a
                // user specified, or DBMS generated, name that may be
                // different then the FUNCTION_NAME for example with
                // overload functions
                specificName
        };

        IMPORTED_KEY_COLUMNS = new ColumnInfo[]{
                // PKTABLE_CAT String => primary key table catalog being
                // imported (may be null)
                pkTableCat,
                // PKTABLE_SCHEM String => primary key table schema being
                // imported (may be null)
                pkTableSchema,
                // PKTABLE_NAME String => primary key table name being
                // imported
                pkTableName,
                // PKCOLUMN_NAME String => primary key column name being
                // imported
                pkColumnName,
                // FKTABLE_CAT String => foreign key table catalog (may be
                // null)
                fkTableCat,
                // FKTABLE_SCHEM String => foreign key table schema (may be
                // null)
                fkTableSchema,
                // FKTABLE_NAME String => foreign key table name
                fkTableName,
                // FKCOLUMN_NAME String => foreign key column name
                fkColumnName,
                // KEY_SEQ short => sequence number within a foreign key( a
                // value of 1 represents the first column of the foreign
                // key, a value of 2 would represent the second column
                // within the foreign key).
                keySeq,
                // UPDATE_RULE short => What happens to a foreign key when
                // the primary key is updated:
                // importedNoAction - do not allow update of primary key if
                // it has been imported
                // importedKeyCascade - change imported key to agree with
                // primary key update
                // importedKeySetNull - change imported key to NULL if its
                // primary key has been updated
                // importedKeySetDefault - change imported key to default
                // values if its primary key has been updated
                // importedKeyRestrict - same as importedKeyNoAction (for
                // ODBC 2.x compatibility)
                updateRule,
                // DELETE_RULE short => What happens to the foreign key when
                // primary is deleted.
                // importedKeyNoAction - do not allow delete of primary key
                // if it has been imported
                // importedKeyCascade - delete rows that import a deleted
                // key
                // importedKeySetNull - change imported key to NULL if its
                // primary key has been deleted
                // importedKeyRestrict - same as importedKeyNoAction (for
                // ODBC 2.x compatibility)
                // importedKeySetDefault - change imported key to default if
                // its primary key has been deleted
                deleteRule,
                // FK_NAME String => foreign key name (may be null)
                fkName,
                // PK_NAME String => primary key name (may be null)
                pkName,
                // DEFERRABILITY short => can the evaluation of foreign key
                // constraints be deferred until commit
                // importedKeyInitiallyDeferred - see SQL92 for definition
                // importedKeyInitiallyImmediate - see SQL92 for definition
                // importedKeyNotDeferrable - see SQL92 for definition
                deferrability
        };

        INDEX_INFO_COLUMNS = new ColumnInfo[]{
                // TABLE_CAT String => table catalog (may be null)
                tableCat,
                // TABLE_SCHEM String => table schema (may be null)
                tableSchema,
                // TABLE_NAME String => table name
                tableName,
                // NON_UNIQUE boolean => Can index values be non-unique.
                // false when TYPE is tableIndexStatistic
                nonUnique,
                // INDEX_QUALIFIER String => index catalog (may be null);
                // null when TYPE is tableIndexStatistic
                indexQualifier,
                // INDEX_NAME String => index name; null when TYPE is
                // tableIndexStatistic
                indexName,
                // TYPE short => index type:
                // tableIndexStatistic - this identifies table statistics
                // that are returned in conjuction with a table's index
                // descriptions
                // tableIndexClustered - this is a clustered index
                // tableIndexHashed - this is a hashed index
                // tableIndexOther - this is some other style of index
                type,
                // ORDINAL_POSITION short => column sequence number within
                // index; zero when TYPE is tableIndexStatistic
                ordinalPosition,
                // COLUMN_NAME String => column name; null when TYPE is
                // tableIndexStatistic
                columnName,
                // ASC_OR_DESC String => column sort sequence, "A" =>
                // ascending, "D" => descending, may be null if sort
                // sequence is not supported; null when TYPE is
                // tableIndexStatistic
                ascOrDesc,
                // CARDINALITY int => When TYPE is tableIndexStatistic, then
                // this is the number of rows in the table; otherwise, it is
                // the number of unique values in the index.
                cardinality,
                // PAGES int => When TYPE is tableIndexStatisic then this is
                // the number of pages used for the table, otherwise it is
                // the number of pages used for the current index.
                pages,
                // FILTER_CONDITION String => Filter condition, if any. (may
                // be null)
                filterCondition
        };

        PRIMARY_KEY_COLUMNS = new ColumnInfo[]{
                // TABLE_CAT String => table catalog (may be null)
                tableCat,
                // TABLE_SCHEM String => table schema (may be null)
                tableSchema,
                // TABLE_NAME String => table name
                tableName,
                // COLUMN_NAME String => column name
                columnName,
                // KEY_SEQ short => sequence number within primary key( a
                // value of 1 represents the first column of the primary
                // key, a value of 2 would represent the second column
                // within the primary key).
                keySeq,
                // PK_NAME String => primary key name (may be null)
                pkName
        };

        PROCEDURE_COLUMN_COLUMNS = new ColumnInfo[]{
                // PROCEDURE_CAT String => procedure catalog (may be null)
                procedureCat,
                // PROCEDURE_SCHEM String => procedure schema (may be null)
                procedureSchema,
                // PROCEDURE_NAME String => procedure name
                procedureName,
                // COLUMN_NAME String => column/parameter name
                columnName,
                // COLUMN_TYPE Short => kind of column/parameter:
                // procedureColumnUnknown - nobody knows
                // procedureColumnIn - IN parameter
                // procedureColumnInOut - INOUT parameter
                // procedureColumnOut - OUT parameter
                // procedureColumnReturn - procedure return value
                // procedureColumnResult - result column in ResultSet
                columnType,
                // DATA_TYPE int => SQL type from java.sql.Types
                dataType,
                // TYPE_NAME String => SQL type name, for a UDT type the
                // type name is fully qualified
                typeName,
                // PRECISION int => precision
                precision,
                // LENGTH int => length in bytes of data
                length,
                // SCALE short => scale - null is returned for data types
                // where SCALE is not applicable.
                scale,
                // RADIX short => radix
                radix,
                // NULLABLE short => can it contain NULL.
                // procedureNoNulls - does not allow NULL values
                // procedureNullable - allows NULL values
                // procedureNullableUnknown - nullability unknown
                shortNullable,
                // REMARKS String => comment describing parameter/column
                remarks,
                // COLUMN_DEF String => default value for the column, which
                // should be interpreted as a string when the value is
                // enclosed in single quotes (may be null)
                // The string NULL (not enclosed in quotes) - if NULL was
                // specified as the default value
                // TRUNCATE (not enclosed in quotes) - if the specified
                // default value cannot be represented without truncation
                // NULL - if a default value was not specified
                columnDef,
                // SQL_DATA_TYPE int => reserved for future use
                sqlDataType,
                // SQL_DATETIME_SUB int => reserved for future use
                sqlDateTimeSub,
                // CHAR_OCTET_LENGTH int => the maximum length of binary and
                // character based columns. For any other datatype the
                // returned value is a NULL
                charOctetLength,
                // ORDINAL_POSITION int => the ordinal position, starting
                // from 1, for the input and output parameters for a
                // procedure. A value of 0 is returned if this row describes
                // the procedure's return value. For result set columns, it
                // is the ordinal position of the column in the result set
                // starting from 1. If there are multiple result sets, the
                // column ordinal positions are implementation defined.
                ordinalPosition,
                // IS_NULLABLE String => ISO rules are used to determine the
                // nullability for a column.
                // YES --- if the parameter can include NULLs
                // NO --- if the parameter cannot include NULLs
                // empty string --- if the nullability for the parameter is
                // unknown
                isNullable,
                // SPECIFIC_NAME String => the name which uniquely
                // identifies this procedure within its schema.
                specificName
        };

        PROCEDURE_COLUMNS = new ColumnInfo[]{
                // PROCEDURE_CAT String => procedure catalog (may be null)
                procedureCat,
                // PROCEDURE_SCHEM String => procedure schema (may be null)
                procedureSchema,
                // PROCEDURE_NAME String => procedure name
                procedureName,
                // reserved for future use
                empty,
                // reserved for future use
                empty,
                // reserved for future use
                empty,
                // REMARKS String => explanatory comment on the procedure
                remarks,
                // PROCEDURE_TYPE short => kind of procedure:
                // procedureResultUnknown - Cannot determine if a return
                // value will be returned
                // procedureNoResult - Does not return a return value
                // procedureReturnsResult - Returns a return value
                procedureType,
                // SPECIFIC_NAME String => The name which uniquely
                // identifies this procedure within its schema.
                specificName
        };

        PSUEDO_COLUMN_COLUMNS = new ColumnInfo[]{
                // TABLE_CAT String => table catalog (may be null)
                tableCat,
                // TABLE_SCHEM String => table schema (may be null)
                tableSchema,
                // TABLE_NAME String => table name
                tableName,
                // COLUMN_NAME String => column name
                columnName,
                // DATA_TYPE int => SQL type from java.sql.Types
                dataType,
                // COLUMN_SIZE int => column size.
                columnSize,
                // DECIMAL_DIGITS int => the number of fractional digits.
                // Null is returned for data types where DECIMAL_DIGITS is
                // not applicable.
                decimalDigits,
                // NUM_PREC_RADIX int => Radix (typically either 10 or 2)
                numPrecRadix,
                // COLUMN_USAGE String => The allowed usage for the column.
                // The value returned will correspond to the enum name
                // returned by PseudoColumnUsage.name()
                columnUsage,
                // REMARKS String => comment describing column (may be null)
                remarks,
                // CHAR_OCTET_LENGTH int => for char types the maximum
                // number of bytes in the column
                charOctetLength,
                // IS_NULLABLE String => ISO rules are used to determine the
                // nullability for a column.
                // YES --- if the column can include NULLs
                // NO --- if the column cannot include NULLs
                // empty string --- if the nullability for the column is
                // unknown
                isNullable
        };

        // NB - For some reason JDBC suddenly uses TABLE_CATALOG instead of
        // TABLE_CAT here?
        SCHEMA_COLUMNS = new ColumnInfo[]{
                // TABLE_SCHEM String => schema name
                tableSchema,
                // TABLE_CATALOG String => catalog name (may be null)
                tableCatalog
        };

        SUPER_TABLE_COLUMNS = new ColumnInfo[]{
                // TABLE_CAT String => the type's catalog (may be null)
                tableCat,
                // TABLE_SCHEM String => type's schema (may be null)
                tableSchema,
                // TABLE_NAME String => type name
                tableName,
                // SUPERTABLE_NAME String => the direct super type's name
                superTableName
        };

        SUPER_TYPE_COLUMNS = new ColumnInfo[]{
                // TYPE_CAT String => the UDT's catalog (may be null)
                typeCat,
                // TYPE_SCHEM String => UDT's schema (may be null)
                typeSchema,
                // TYPE_NAME String => type name of the UDT
                typeName,
                // SUPERTYPE_CAT String => the direct super type's catalog
                // (may be null)
                superTypeCat,
                // SUPERTYPE_SCHEM String => the direct super type's schema
                // (may be null)
                superTypeSchema,
                // SUPERTYPE_NAME String => the direct super type's name
                superTypeName
        };

        TABLE_PRIVILEGE_COLUMNS = new ColumnInfo[]{
                // TABLE_CAT String => table catalog (may be null)
                tableCat,
                // TABLE_SCHEM String => table schema (may be null)
                tableSchema,
                // TABLE_NAME String => table name
                tableName,
                // GRANTOR String => grantor of access (may be null)
                grantor,
                // GRANTEE String => grantee of access
                grantee,
                // PRIVILEGE String => name of access (SELECT, INSERT,
                // UPDATE, REFRENCES, ...)
                privilege,
                // IS_GRANTABLE String => "YES" if grantee is permitted to
                // grant to others; "NO" if not; null if unknown
                isGrantable
        };

        TABLE_TYPE_COLUMNS = new ColumnInfo[]{
                // TABLE_TYPE String => table type. Typical types are "TABLE",
                // "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY",
                // "ALIAS", "SYNONYM".
                tableType
        };

        TABLE_COLUMNS = new ColumnInfo[]{
                // TABLE_CAT String => table catalog (may be null)
                tableCat,
                // TABLE_SCHEM String => table schema (may be null)
                tableSchema,
                // TABLE_NAME String => table name
                tableName,
                // TABLE_TYPE String => table type. Typical types are
                // "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY",
                // "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
                tableType,
                // REMARKS String => explanatory comment on the table
                remarks,
                // TYPE_CAT String => the types catalog (may be null)
                typeCat,
                // TYPE_SCHEM String => the types schema (may be null)
                typeSchema,
                // TYPE_NAME String => type name (may be null)
                typeName,
                // SELF_REFERENCING_COL_NAME String => name of the
                // designated "identifier" column of a typed table (may be
                // null)
                selfRefColName,
                // REF_GENERATION String => specifies how values in
                // SELF_REFERENCING_COL_NAME are created. Values are
                // "SYSTEM", "USER", "DERIVED". (may be null)
                refGeneration
        };

        TYPE_INFO_COLUMNS = new ColumnInfo[]{
                // TYPE_NAME String => Type name
                typeName,
                // DATA_TYPE int => SQL data type from java.sql.Types
                dataType,
                // PRECISION int => maximum precision
                precision,
                // LITERAL_PREFIX String => prefix used to quote a literal
                // (may be null)
                litPrefix,
                // LITERAL_SUFFIX String => suffix used to quote a literal
                // (may be null)
                litSuffix,
                // CREATE_PARAMS String => parameters used in creating the
                // type (may be null)
                createParams,
                // NULLABLE short => can you use NULL for this type.
                // typeNoNulls - does not allow NULL values
                // typeNullable - allows NULL values
                // typeNullableUnknown - nullability unknown
                shortNullable,
                // CASE_SENSITIVE boolean=> is it case sensitive.
                caseSensitive,
                // SEARCHABLE short => can you use "WHERE" based on this
                // type:
                // typePredNone - No support
                // typePredChar - Only supported with WHERE .. LIKE
                // typePredBasic - Supported except for WHERE .. LIKE
                // typeSearchable - Supported for all WHERE ..
                searchable,
                // UNSIGNED_ATTRIBUTE boolean => is it unsigned.
                unsignedAttr,
                // FIXED_PREC_SCALE boolean => can it be a money value.
                fixedPrecScale,
                // AUTO_INCREMENT boolean => can it be used for an
                // auto-increment value.
                autoIncrement,
                // LOCAL_TYPE_NAME String => localized version of type name
                // (may be null)
                localTypeName,
                // MINIMUM_SCALE short => minimum scale supported
                minScale,
                // MAXIMUM_SCALE short => maximum scale supported
                maxScale,
                // SQL_DATA_TYPE int => unused
                sqlDataType,
                // SQL_DATETIME_SUB int => unused
                sqlDateTimeSub,
                // NUM_PREC_RADIX int => usually 2 or 10
                numPrecRadix
        };

        UDT_COLUMNS = new ColumnInfo[]{
                // TYPE_CAT String => the type's catalog (may be null)
                typeCat,
                // TYPE_SCHEM String => type's schema (may be null)
                typeSchema,
                // TYPE_NAME String => type name
                typeName,
                // CLASS_NAME String => Java class name
                className,
                // DATA_TYPE int => type value defined in java.sql.Types.
                // One of JAVA_OBJECT, STRUCT, or DISTINCT
                dataType,
                // REMARKS String => explanatory comment on the type
                remarks,
                // BASE_TYPE short => type code of the source type of a
                // DISTINCT type or the type that implements the
                // user-generated reference type of the
                // SELF_REFERENCING_COLUMN of a structured type as defined
                // in java.sql.Types (null if DATA_TYPE is not DISTINCT or
                // not STRUCT with REFERENCE_GENERATION = USER_DEFINED)
                baseType
        };

        VERSION_COLUMNS = new ColumnInfo[]{
                // SCOPE short => is not used
                scope,
                // COLUMN_NAME String => column name
                columnName,
                // DATA_TYPE int => SQL data type from java.sql.Types
                dataType,
                // TYPE_NAME String => Data source-dependent type name
                typeName,
                // COLUMN_SIZE int => precision
                columnSize,
                // BUFFER_LENGTH int => length of column value in bytes
                bufferLength,
                // DECIMAL_DIGITS short => scale - Null is returned for data
                // types where DECIMAL_DIGITS is not applicable.
                decimalDigits,
                // PSEUDO_COLUMN short => whether this is pseudo column like
                // an Oracle ROWID
                // versionColumnUnknown - may or may not be pseudo column
                // versionColumnNotPseudo - is NOT a pseudo column
                // versionColumnPseudo - is a pseudo column
                psuedoColumn
        };
    }

    static ResultSet newResultSet(ColumnInfo[] columns, Object[]... rows) throws SQLException {
        return newResultSet(columns, Arrays.asList(rows));
    }

    static ResultSet newResultSet(ColumnInfo[] columns, List<Object[]> rows) throws SQLException {
        Node[][] nodes = new Node[rows.size()][columns.length];
        for (int i = 0; i < nodes.length; i++) {
            Object[] row = rows.get(i);
            for (int j = 0; j < columns.length; j++) {
                nodes[i][j] = toNode(row[j], columns[j].getType());
            }
        }
        return new ResultSetImpl(null, new ResultSetMetaDataImpl(columns), Arrays.asList(nodes));
    }

    private static Node toNode(Object obj, int type) {
        if (obj == null) {
            return null;
        }
        switch (type) {
            case Types.NVARCHAR:
                String string = obj instanceof Iri ? ((Iri) obj).getIri() : (String) obj;
                return LiteralFactory.createString(string);
            case Types.BOOLEAN:
                return LiteralFactory.createBoolean((Boolean) obj);
            case Types.SMALLINT:
                return LiteralFactory.createShort(((Number) obj).shortValue());
            case Types.INTEGER:
                return LiteralFactory.createInt(((Number) obj).intValue());
            default:
                throw new UnsupportedOperationException("Unexpected metadata value type: " + type);
        }
    }
}
