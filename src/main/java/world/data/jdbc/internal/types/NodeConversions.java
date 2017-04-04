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

import lombok.experimental.UtilityClass;
import world.data.jdbc.model.Blank;
import world.data.jdbc.model.Iri;
import world.data.jdbc.model.Literal;
import world.data.jdbc.model.LiteralFactory;
import world.data.jdbc.model.Node;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URL;
import java.sql.CallableStatement;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Class with helpful utility methods for JDBC type conversions.
 */
@UtilityClass
public final class NodeConversions {

    private static final Map<Class<?>, ValueFunction<?>> VALUE_FUNCTIONS_BY_CLASS = new HashMap<>();
    private static final Map<Class<?>, NodeFunction<?>> NODE_FUNCTIONS_BY_CLASS = new HashMap<>();

    static {
        // Special cases
        map(Node.class, n -> n, null);
        map(String.class, NodeConversions::toString, LiteralFactory::createString);
        map(Object.class, NodeConversions::toObject, NodeConversions::toNode);

        // If primitive type requested, coerce null to false or zero
        map(boolean.class, n -> NodeValues.parseBoolean(n, false), null);
        map(byte.class, n -> NodeValues.parseByte(n, (byte) 0), null);
        map(double.class, n -> NodeValues.parseDouble(n, 0d), null);
        map(float.class, n -> NodeValues.parseFloat(n, 0f), null);
        map(int.class, n -> NodeValues.parseInteger(n, 0), null);
        map(long.class, n -> NodeValues.parseLong(n, 0L), null);
        map(short.class, n -> NodeValues.parseShort(n, (short) 0), null);

        // Objects
        map(BigDecimal.class, NodeValues::parseBigDecimal, LiteralFactory::createDecimal);
        map(BigInteger.class, NodeValues::parseBigInteger, LiteralFactory::createInteger);
        map(Boolean.class, NodeValues::parseBoolean, LiteralFactory::createBoolean);
        map(Byte.class, NodeValues::parseByte, LiteralFactory::createByte);
        map(Double.class, NodeValues::parseDouble, LiteralFactory::createDouble);
        map(Duration.class, NodeValues::parseDuration, LiteralFactory::createDayTimeDuration);
        map(Float.class, NodeValues::parseFloat, LiteralFactory::createFloat);
        map(Integer.class, NodeValues::parseInteger, LiteralFactory::createInteger);
        map(Instant.class, NodeValues::parseInstant, LiteralFactory::createDateTime);
        map(LocalDate.class, NodeValues::parseLocalDate, LiteralFactory::createDate);
        map(LocalDateTime.class, NodeValues::parseLocalDateTime, LiteralFactory::createDateTime);
        map(LocalTime.class, NodeValues::parseLocalTime, LiteralFactory::createTime);
        map(Long.class, NodeValues::parseLong, LiteralFactory::createLong);
        map(Month.class, NodeValues::parseMonth, LiteralFactory::createMonth);
        map(MonthDay.class, NodeValues::parseMonthDay, LiteralFactory::createMonthDay);
        map(Number.class, NodeValues::parseBestNumber, null);
        map(OffsetDateTime.class, NodeValues::parseOffsetDateTime, LiteralFactory::createDateTime);
        map(OffsetTime.class, NodeValues::parseOffsetTime, LiteralFactory::createTime);
        map(Period.class, NodeValues::parsePeriod, LiteralFactory::createYearMonthDuration);
        map(Short.class, NodeValues::parseShort, LiteralFactory::createShort);
        map(URI.class, NodeValues::parseUri, Iri::new);
        map(Year.class, NodeValues::parseYear, LiteralFactory::createYear);
        map(YearMonth.class, NodeValues::parseYearMonth, LiteralFactory::createYearMonth);
        map(ZonedDateTime.class, NodeValues::parseZonedDateTime, LiteralFactory::createDateTime);

        // Deprecated
        //noinspection deprecation
        map(URL.class, NodeValues::parseUrl, Iri::new);
        //noinspection deprecation
        map(java.sql.Date.class, NodeValues::parseSqlDate, LiteralFactory::createDate);
        //noinspection deprecation
        map(java.sql.Time.class, NodeValues::parseSqlTime, LiteralFactory::createTime);
        //noinspection deprecation
        map(java.sql.Timestamp.class, NodeValues::parseSqlTimestamp, LiteralFactory::createDateTime);
        //noinspection deprecation
        map(java.util.Date.class, NodeValues::parseUtilDate, LiteralFactory::createDate);
    }

    /** Helper to make sure getValueFunctionsByClass() matches key and value types. */
    private static <T> void map(Class<T> clazz, ValueFunction<T> valueFn, NodeFunction<T> nodeFn) {
        requireNonNull(valueFn, "valueFn");
        VALUE_FUNCTIONS_BY_CLASS.put(clazz, valueFn);
        if (nodeFn != null) {
            NODE_FUNCTIONS_BY_CLASS.put(clazz, nodeFn);
        }
    }

    /**
     * Converts a {@link Node} to a native Java type based for {@code ResultSet.getObject(label, Types.JAVA_OBJECT)}.
     */
    private static Object toObject(Node node) throws SQLException {
        if (node == null) {
            return null;
        } else if (node instanceof Literal) {
            Literal literal = (Literal) node;
            TypeMapping mapping = TypeMap.INSTANCE.getCustom(literal.getDatatype());
            return toObject(node, mapping.getJavaType());
        } else if (node instanceof Iri) {
            Iri iri = (Iri) node;
            return iri.toURI();
        } else if (node instanceof Blank) {
            return node.toString();
        } else {
            throw new UnsupportedOperationException("Unknown node type");  // should never happen
        }
    }

    /**
     * Converts a {@link Node} to a native Java type based for {@link ResultSet#getObject(String, Class)}.
     */
    public static <T> T toObject(Node node, Class<T> target) throws SQLException {
        if (node != null && target == node.getClass()) {
            return target.cast(node);
        }
        ValueFunction<?> valueFn = VALUE_FUNCTIONS_BY_CLASS.get(target);
        if (valueFn == null) {
            throw new SQLException(NodeValues.parseErrorMessage(node, target));
        }
        return target.cast(valueFn.toObject(node));
    }

    /**
     * Converts a {@link Node} to a native Java type based for {@link ResultSet#getObject(int)}.
     */
    public static Object toObject(Node node, int jdbcType) throws SQLException {
        if (node == null) {
            return null;
        }
        TypeMapping mapping = TypeMap.INSTANCE.getStandard(jdbcType);
        if (mapping == null) {
            throw new SQLException("Unable to marshal to the declared column type: " + jdbcType);
        }
        if (jdbcType == Types.TINYINT || jdbcType == Types.SMALLINT) {
            // Special case specified by table B-3 in JDBC 4.2 specification
            return toObject(node, Integer.class);
        } else {
            return toObject(node, mapping.getJavaType());
        }
    }

    /**
     * Converts a {@link Node} to String for eg. {@link ResultSet#getString(int)}.
     */
    public static String toString(Node node) throws SQLException {
        if (node == null) {
            return null;
        } else if (node instanceof Literal) {
            return ((Literal) node).getLexicalForm();
        } else if (node instanceof Iri) {
            return ((Iri) node).getIri();
        } else if (node instanceof Blank) {
            return node.toString();
        } else {
            throw new SQLException("Unable to marshal unknown node types to a string");  // should never happen
        }
    }

    /**
     * Converts an object to a {@link Node} for {@link PreparedStatement#setObject(int, Object)} and
     * {@link CallableStatement#setObject(String, Object)}.
     */
    @SuppressWarnings("deprecation")
    public static Node toNode(Object obj) throws SQLException {
        if (obj == null || obj instanceof Node) {
            return (Node) obj;
        }
        try {
            // Try to find the conversion function based on the specific class of 'obj'
            NodeFunction nodeFn = NODE_FUNCTIONS_BY_CLASS.get(obj.getClass());
            if (nodeFn != null) {
                //noinspection unchecked
                return nodeFn.toNode(obj);
            }
            // For now there are no interfaces or abstract classes that we look for
            throw new SQLException("setObject() received a value that could not be converted to a RDF node for use in a SPARQL query");
        } catch (SQLException e) {
            throw e;
        } catch (Throwable e) {
            throw new SQLException("Unexpected error trying to marshal a value to the desired target type", e);
        }
    }

    /**
     * Converts an object to a {@link Node} for {@link PreparedStatement#setObject(int, Object, int)} and
     * {@link CallableStatement#setObject(String, Object, int)}.
     */
    @SuppressWarnings("deprecation")
    public static Node toNode(Object obj, int jdbcType) throws SQLException {
        try {
            if (obj == null) {
                return null;
            }
            // The behavior of this method is specified by table B.5 in the JDBC 4.2 spec
            switch (jdbcType) {
                case Types.ARRAY:
                case Types.BINARY:
                case Types.BIT:
                case Types.BLOB:
                case Types.CLOB:
                case Types.DISTINCT:
                case Types.LONGVARBINARY:
                case Types.NCLOB:
                case Types.NULL:
                case Types.OTHER:
                case Types.REAL:
                case Types.REF:
                case Types.ROWID:
                case Types.SQLXML:
                case Types.STRUCT:
                case Types.VARBINARY:
                    throw new SQLException("The provided SQL Target Type cannot be translated into an appropriate RDF term type");
                case Types.BIGINT:
                    if (obj instanceof Long || obj instanceof Integer || obj instanceof Short || obj instanceof Byte) {
                        return LiteralFactory.createLong((long) (Number) obj);
                    } else {
                        return LiteralFactory.createLong(NodeValues.parseLong(toNode(obj)));
                    }
                case Types.BOOLEAN:
                    if (obj instanceof Boolean) {
                        return LiteralFactory.createBoolean((Boolean) obj);
                    } else {
                        return LiteralFactory.createBoolean(NodeValues.parseBoolean(toNode(obj)));
                    }
                case Types.DATE:
                    if (obj instanceof Date) {
                        return LiteralFactory.createDate((Date) obj);
                    } else if (obj instanceof TemporalAccessor) {
                        return LiteralFactory.createDate((TemporalAccessor) obj);
                    } else {
                        return LiteralFactory.createDate(NodeValues.parseLocalDate(toNode(obj)));
                    }
                case Types.NUMERIC:
                    if (obj instanceof BigInteger) {
                        return LiteralFactory.createInteger((BigInteger) obj);
                    } else if (obj instanceof Long || obj instanceof Integer || obj instanceof Short || obj instanceof Byte) {
                        return LiteralFactory.createInteger((long) (Number) obj);
                    } else {
                        return LiteralFactory.createInteger(NodeValues.parseBigInteger(toNode(obj)));
                    }
                case Types.DECIMAL:
                    if (obj instanceof BigDecimal) {
                        return LiteralFactory.createDecimal((BigDecimal) obj);
                    } else if (obj instanceof BigInteger) {
                        return LiteralFactory.createDecimal(new BigDecimal((BigInteger) obj));
                    } else if (obj instanceof Long || obj instanceof Integer || obj instanceof Short || obj instanceof Byte) {
                        return LiteralFactory.createDecimal(BigDecimal.valueOf((long) (Number) obj));
                    } else if (obj instanceof Double || obj instanceof Float) {
                        return LiteralFactory.createDecimal(BigDecimal.valueOf((double) (Number) obj));
                    } else {
                        return LiteralFactory.createDecimal(NodeValues.parseBigDecimal(toNode(obj)));
                    }
                case Types.DOUBLE:
                    if (obj instanceof Double || obj instanceof Float || obj instanceof Long ||  obj instanceof Integer || obj instanceof Short || obj instanceof Byte) {
                        return LiteralFactory.createDouble((double) (Number) obj);
                    } else {
                        return LiteralFactory.createDouble(NodeValues.parseDouble(toNode(obj)));
                    }
                case Types.FLOAT:
                    if (obj instanceof Float || obj instanceof Long ||  obj instanceof Integer || obj instanceof Short || obj instanceof Byte) {
                        return LiteralFactory.createFloat((float) (Number) obj);
                    } else {
                        return LiteralFactory.createFloat(NodeValues.parseFloat(toNode(obj)));
                    }
                case Types.INTEGER:
                    if (obj instanceof Integer || obj instanceof Short || obj instanceof Byte) {
                        return LiteralFactory.createInt((int) (Number) obj);
                    } else {
                        return LiteralFactory.createInt(NodeValues.parseInteger(toNode(obj)));
                    }
                case Types.JAVA_OBJECT:
                    return toNode(obj);
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                    if (obj instanceof String) {
                        return LiteralFactory.createString((String) obj);
                    } else {
                        return LiteralFactory.createString(toString(toNode(obj)));
                    }
                case Types.SMALLINT:
                    if (obj instanceof Short || obj instanceof Byte) {
                        return LiteralFactory.createShort((short) (Number) obj);
                    } else {
                        return LiteralFactory.createShort(NodeValues.parseShort(toNode(obj)));
                    }
                case Types.TIME:
                case Types.TIME_WITH_TIMEZONE:
                    if (obj instanceof Time) {
                        return LiteralFactory.createTime((Time) obj);
                    } else if (obj instanceof TemporalAccessor) {
                        return LiteralFactory.createTime((TemporalAccessor) obj);
                    } else {
                        return LiteralFactory.createTime(NodeValues.parseBestTime(toNode(obj)));
                    }
                case Types.TIMESTAMP:
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    if (obj instanceof Timestamp) {
                        return LiteralFactory.createDateTime((Timestamp) obj);
                    } else if (obj instanceof TemporalAccessor) {
                        return LiteralFactory.createDateTime((TemporalAccessor) obj);
                    } else {
                        return LiteralFactory.createDateTime(NodeValues.parseBestDateTime(toNode(obj)));
                    }
                case Types.TINYINT:
                    if (obj instanceof Byte) {
                        return LiteralFactory.createByte((Byte) obj);
                    } else {
                        return LiteralFactory.createByte(NodeValues.parseByte(toNode(obj)));
                    }
                default:
                    throw new SQLException("Cannot translate an unknown SQL Target Type into an appropriate RDF term type");
            }
        } catch (SQLException e) {
            throw e;
        } catch (Throwable e) {
            throw new SQLException("Unexpected error trying to marshal a value to the desired target type", e);
        }
    }

    /** Converts an RDF term to the most natural native Java type. */
    @FunctionalInterface
    private interface ValueFunction<T> {
        T toObject(Node node) throws SQLException;
    }

    /** Converts a native Java pojo to an RDF term. */
    @FunctionalInterface
    private interface NodeFunction<T> {
        Node toNode(T obj) throws SQLException;
    }
}
