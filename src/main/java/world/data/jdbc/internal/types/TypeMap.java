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

import world.data.jdbc.model.Iri;
import world.data.jdbc.model.Node;
import world.data.jdbc.vocab.Rdfs;
import world.data.jdbc.vocab.Xsd;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.sql.JDBCType;
import java.time.Duration;
import java.time.Month;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class TypeMap {
    private static final TypeMapping STRING = TypeMapping.simple(Xsd.STRING, JDBCType.NVARCHAR, String.class, Integer.MAX_VALUE);

    public static final Iri DATATYPE_RAW_NODE = javaIri(Node.class);
    private static final Iri DATATYPE_JAVA_OBJECT = javaIri(Object.class);

    public static final TypeMap INSTANCE = new TypeMap();

    private final Map<Iri, TypeMapping> standardByDatatype = new HashMap<>();
    private final Map<Iri, TypeMapping> byDatatype = new HashMap<>();
    private final Map<Integer, TypeMapping> standardByJdbcType = new HashMap<>();

    private TypeMap() {
        int max = Integer.MAX_VALUE;

        // Standard mappings Rdf <-> Jdbc --> Java
        standard(STRING, JDBCType.CHAR, JDBCType.VARCHAR, JDBCType.LONGVARCHAR, JDBCType.NCHAR, JDBCType.LONGNVARCHAR);
        standard(TypeMapping.simple(Xsd.BOOLEAN, JDBCType.BOOLEAN, Boolean.class, "false".length()), JDBCType.BIT);
        standard(TypeMapping.simple(Xsd.DATE, JDBCType.DATE, java.sql.Date.class, 15));
        standard(TypeMapping.simple(Xsd.TIME, JDBCType.TIME, java.sql.Time.class, 24));
        standard(TypeMapping.simple(Xsd.DATETIME, JDBCType.TIMESTAMP, java.sql.Timestamp.class, 40));
        standard(TypeMapping.simple(javaIri(OffsetTime.class), JDBCType.TIME_WITH_TIMEZONE, OffsetTime.class, 24));
        standard(TypeMapping.simple(Xsd.DATETIMESTAMP, JDBCType.TIMESTAMP_WITH_TIMEZONE, OffsetDateTime.class, 40));

        standard(TypeMapping.numeric(Xsd.BYTE, JDBCType.TINYINT, Byte.class, 3, 0, 0, true, true));
        standard(TypeMapping.numeric(Xsd.SHORT, JDBCType.SMALLINT, Short.class, 5, 0, 0, true, true));
        standard(TypeMapping.numeric(Xsd.INT, JDBCType.INTEGER, Integer.class, 10, 0, 0, true, true));
        standard(TypeMapping.numeric(Xsd.LONG, JDBCType.BIGINT, Long.class, 19, 0, 0, true, true));
        standard(TypeMapping.numeric(Xsd.FLOAT, JDBCType.REAL, Float.class, 9, 6, 9, true, false));
        standard(TypeMapping.numeric(Xsd.DOUBLE, JDBCType.DOUBLE, Double.class, 17, 15, 17, true, false), JDBCType.FLOAT);
        standard(TypeMapping.numeric(Xsd.DECIMAL, JDBCType.DECIMAL, BigDecimal.class, 38, 38, 38, true, true), JDBCType.NUMERIC);

        // data.world extension: map to the most natural native Java object
        standard(TypeMapping.simple(DATATYPE_JAVA_OBJECT, JDBCType.JAVA_OBJECT, Object.class, max));
        // data.world extension: map to RDF model object
        standard(TypeMapping.simple(DATATYPE_RAW_NODE, JDBCType.OTHER, Node.class, max));

        // Custom mappings Rdf -> Jdbc, Java
        custom(TypeMapping.simple(Rdfs.RESOURCE, JDBCType.NVARCHAR, URI.class, max));
        custom(TypeMapping.simple(Xsd.ANYURI, JDBCType.NVARCHAR, URI.class, max));
        custom(TypeMapping.simple(Xsd.DAYTIMEDURATION, JDBCType.NVARCHAR, Duration.class, 36));
        custom(TypeMapping.numeric(Xsd.GDAY, JDBCType.INTEGER, Integer.class, 2, 0, 0, false, true));
        custom(TypeMapping.numeric(Xsd.GMONTH, JDBCType.INTEGER, Month.class, 2, 0, 0, false, true));
        custom(TypeMapping.simple(Xsd.GMONTHDAY, JDBCType.NVARCHAR, MonthDay.class, 7));
        custom(TypeMapping.numeric(Xsd.GYEAR, JDBCType.INTEGER, Year.class, 9, 0, 0, true, true));
        custom(TypeMapping.simple(Xsd.GYEARMONTH, JDBCType.NVARCHAR, YearMonth.class, 12));
        custom(TypeMapping.numeric(Xsd.INTEGER, JDBCType.NUMERIC, BigInteger.class, 38, 0, 0, true, true));
        custom(TypeMapping.numeric(Xsd.NEGATIVEINTEGER, JDBCType.NUMERIC, BigInteger.class, 38, 0, 0, true, true));
        custom(TypeMapping.numeric(Xsd.NONNEGATIVEINTEGER, JDBCType.NUMERIC, BigInteger.class, 38, 0, 0, false, true));
        custom(TypeMapping.numeric(Xsd.NONPOSITIVEINTEGER, JDBCType.NUMERIC, BigInteger.class, 38, 0, 0, true, true));
        custom(TypeMapping.numeric(Xsd.POSITIVEINTEGER, JDBCType.NUMERIC, BigInteger.class, 38, 0, 0, false, true));
        custom(TypeMapping.numeric(Xsd.UNSIGNEDBYTE, JDBCType.SMALLINT, Short.class, 3, 0, 0, false, true));
        custom(TypeMapping.numeric(Xsd.UNSIGNEDINT, JDBCType.BIGINT, Long.class, 10, 0, 0, false, true));
        custom(TypeMapping.numeric(Xsd.UNSIGNEDLONG, JDBCType.NUMERIC, BigInteger.class, 20, 0, 0, false, true));
        custom(TypeMapping.numeric(Xsd.UNSIGNEDSHORT, JDBCType.INTEGER, Integer.class, 6, 0, 0, false, true));
        custom(TypeMapping.simple(Xsd.YEARMONTHDURATION, JDBCType.NVARCHAR, Period.class, 24));

        unmap(JDBCType.BINARY);
        unmap(JDBCType.VARBINARY);
        unmap(JDBCType.LONGVARBINARY);
        unmap(JDBCType.DISTINCT);
        unmap(JDBCType.CLOB);
        unmap(JDBCType.BLOB);
        unmap(JDBCType.ARRAY);
        unmap(JDBCType.STRUCT);
        unmap(JDBCType.REF);
        unmap(JDBCType.DATALINK);
        unmap(JDBCType.ROWID);
        unmap(JDBCType.NCLOB);
        unmap(JDBCType.SQLXML);
        unmap(JDBCType.REF_CURSOR);
        unmap(JDBCType.NULL);
    }

    public TypeMapping getStandard(Iri datatype) {
        return standardByDatatype.getOrDefault(datatype, STRING);
    }

    public TypeMapping getStandard(int jdbcType) {
        return standardByJdbcType.get(jdbcType);
    }

    public TypeMapping getStandardOrCustom(Iri datatype) {
        return byDatatype.getOrDefault(datatype, STRING);
    }

    public Collection<TypeMapping> getAll() {
        return byDatatype.values();
    }

    private void standard(TypeMapping mapping, JDBCType... additionalTypes) {
        put(byDatatype, mapping.getDatatype(), mapping);
        put(standardByDatatype, mapping.getDatatype(), mapping);
        put(standardByJdbcType, mapping.getJdbcType().getVendorTypeNumber(), mapping);
        for (JDBCType additionalType : additionalTypes) {
            put(standardByJdbcType, additionalType.getVendorTypeNumber(), mapping);
        }
    }

    private void custom(TypeMapping mapping) {
        TypeMapping standard = requireNonNull(getStandard(mapping.getTypeNumber()));
        put(byDatatype, mapping.getDatatype(), mapping);
        put(standardByDatatype, mapping.getDatatype(), standard);
    }

    private void unmap(JDBCType jdbcType) {
        put(standardByJdbcType, jdbcType.getVendorTypeNumber(), null);
    }

    // Visible for testing
    Collection<Integer> getMappedJdbcTypes() {
        return Collections.unmodifiableSet(standardByJdbcType.keySet());
    }

    private static Iri javaIri(Class<?> clazz) {
        return new Iri("java:" + clazz.getName());
    }

    private <K, V> void put(Map<K, V> map, K key, V value) {
        V old = map.put(key, value);
        checkState(old == null, "Duplicate key definition: %s -> %s conflicts with %s", key, old, value);
    }

    private void checkState(boolean flag, String format, Object... args) {
        if (!flag) {
            throw new IllegalArgumentException(String.format(format, args));
        }
    }
}
