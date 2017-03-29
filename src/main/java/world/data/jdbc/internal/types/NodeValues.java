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
import world.data.jdbc.model.Node;
import world.data.jdbc.vocab.Xsd;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URL;
import java.sql.SQLException;
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
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.GregorianCalendar;

import static world.data.jdbc.internal.util.Optionals.or;

/**
 * Parser utility methods for converting {@link Node} objects to plain old Java objects.
 */
@UtilityClass
@SuppressWarnings({"DeprecatedIsStillUsed", "WeakerAccess"})
public final class NodeValues {

    /** Wrap the {@code DatatypeFactory} singleton in its own class to postpone class loading until first use. */
    private static class XmlHolder {
        static final DatatypeFactory DATATYPE_FACTORY = newDatatypeFactory();

        private static DatatypeFactory newDatatypeFactory() {
            try {
                return DatatypeFactory.newInstance();
            } catch (DatatypeConfigurationException ex) {
                throw new IllegalStateException("Can't create a javax.xml.datatype.DatatypeFactory", ex);
            }
        }
    }

    private static final Iri[] BOOLEAN_AND_NUMBER_TYPES = {
            Xsd.BOOLEAN,
            Xsd.DECIMAL,
            Xsd.INTEGER,
            Xsd.DOUBLE,
            Xsd.FLOAT,
            Xsd.BYTE,
            Xsd.INT,
            Xsd.LONG,
            Xsd.SHORT,
            Xsd.UNSIGNEDBYTE,
            Xsd.UNSIGNEDINT,
            Xsd.UNSIGNEDLONG,
            Xsd.UNSIGNEDSHORT,
            Xsd.POSITIVEINTEGER,
            Xsd.NEGATIVEINTEGER,
            Xsd.NONPOSITIVEINTEGER,
            Xsd.NONNEGATIVEINTEGER
    };

    private static final long LONG_PRECISION = Long.toString(Long.MAX_VALUE).length();

    /** Returns either an {@link OffsetTime} or {@link LocalTime} depending on whether time zone is present. */
    public static TemporalAccessor parseBestTime(Node node) throws SQLException {
        return parseLiteral(node, TemporalAccessor.class,
                (s) -> DateTimeFormatter.ISO_TIME.parseBest(s, OffsetTime::from, LocalTime::from),
                Xsd.TIME, Xsd.DATETIME, Xsd.DATETIMESTAMP);
    }

    /** Returns either an {@link OffsetDateTime} or {@link LocalDateTime} depending on whether time zone is present. */
    public static TemporalAccessor parseBestDateTime(Node node) throws SQLException {
        return parseLiteral(node, TemporalAccessor.class,
                (s) -> DateTimeFormatter.ISO_DATE_TIME.parseBest(s, OffsetDateTime::from, LocalDateTime::from),
                Xsd.DATETIME, Xsd.DATETIMESTAMP);
    }
    /**
     * Returns {@link Double}, {@link Long} or {@link BigInteger} depending on whether the number contains a
     * fractional component and on the magnitude of the number to be parsed.
     */
    public static Number parseBestNumber(Node node) throws SQLException {
        return parseNumber(node, Number.class, (String lexicalForm) -> {
            if (lexicalForm.indexOf('.') != -1) {
                return parseDouble(lexicalForm);
            } else if (lexicalForm.length() < LONG_PRECISION + (lexicalForm.startsWith("-") ? 1 : 0)) {
                return Long.parseLong(lexicalForm);
            } else {
                BigInteger n = new BigInteger(lexicalForm);
                long l = n.longValue();
                return n.equals(BigInteger.valueOf(l)) ? l : n;
            }
        });
    }

    public static BigDecimal parseBigDecimal(Node node) throws SQLException {
        return parseNumber(node, BigDecimal.class, BigDecimal::new);
    }

    public static BigInteger parseBigInteger(Node node) throws SQLException {
        return parseNumber(node, BigInteger.class, BigInteger::new);
    }

    public static Boolean parseBoolean(Node node) throws SQLException {
        return parseLiteral(node, Boolean.class, (String lexicalForm, Iri datatype, String language) -> {
            if (Xsd.BOOLEAN.equals(datatype)) {
                return Boolean.parseBoolean(lexicalForm);
            } else if (Xsd.STRING.equals(datatype)) {
                // SQL-style "true", "yes", "1", "-1" being very liberal about ignoring chars after the first
                int ch = lexicalForm.isEmpty() ? 0 : Character.toLowerCase(lexicalForm.charAt(0));
                return ch == 't' || ch == 'y' || ch == '1' || "-1".equals(lexicalForm);
            } else {
                String lex = lexicalForm;
                int dot = lex.indexOf('.');
                if (dot != -1) {
                    lex = lex.substring(0, dot); // ignore fractional component (if any)
                }
                try {
                    return Long.parseLong(lex) != 0;
                } catch (NumberFormatException e) {
                    double d = parseDouble(lex);
                    return d != 0d && !Double.isNaN(d);
                }
            }
        }, BOOLEAN_AND_NUMBER_TYPES);
    }

    public static boolean parseBoolean(Node node, boolean defaultValue) throws SQLException {
        return or(parseBoolean(node), defaultValue);
    }

    // This method is located near parseBoolean() to make it easier to keep their implementations in sync
    private static <V> V parseNumber(Node node, Class<V> clazz, Parser<V> parser) throws SQLException {
        return parseLiteral(node, clazz, (String lexicalForm, Iri datatype, String language) -> {
            // JDBC spec allows freely converting back and forth between booleans and numbers
            if (Xsd.BOOLEAN.equals(datatype)) {
                lexicalForm = Boolean.parseBoolean(lexicalForm) ? "1" : "0";
            }
            return parser.parse(lexicalForm);
        }, BOOLEAN_AND_NUMBER_TYPES);
    }

    public static Byte parseByte(Node node) throws SQLException {
        // note: decode() supports several ways of representing a byte value
        return parseNumber(node, Byte.class, Byte::decode);
    }

    public static byte parseByte(Node node, byte defaultValue) throws SQLException {
        return or(parseByte(node), defaultValue);
    }

    public static Double parseDouble(Node node) throws SQLException {
        return parseNumber(node, Double.class, NodeValues::parseDouble);
    }

    private static Double parseDouble(String lexicalForm) {
        if ("INF".equals(lexicalForm)) {
            return Double.POSITIVE_INFINITY;
        } else if ("-INF".equals(lexicalForm)) {
            return Double.NEGATIVE_INFINITY;
        } else {
            return Double.parseDouble(lexicalForm);
        }
    }

    public static double parseDouble(Node node, double defaultValue) throws SQLException {
        return or(parseDouble(node), defaultValue);
    }

    public static Duration parseDuration(Node node) throws SQLException {
        return parseLiteral(node, Duration.class, Duration::parse, Xsd.DAYTIMEDURATION);
    }

    public static Float parseFloat(Node node) throws SQLException {
        return parseNumber(node, Float.class, NodeValues::parseFloat);
    }

    private static Float parseFloat(String lexicalForm) {
        if ("INF".equals(lexicalForm)) {
            return Float.POSITIVE_INFINITY;
        } else if ("-INF".equals(lexicalForm)) {
            return Float.NEGATIVE_INFINITY;
        } else {
            return Float.parseFloat(lexicalForm);
        }
    }

    public static float parseFloat(Node node, float defaultValue) throws SQLException {
        return or(parseFloat(node), defaultValue);
    }

    public static Integer parseInteger(Node node) throws SQLException {
        return parseNumber(node, Integer.class, Integer::parseInt);
    }

    public static int parseInteger(Node node, int defaultValue) throws SQLException {
        return or(parseInteger(node), defaultValue);
    }

    public static Instant parseInstant(Node node) throws SQLException {
        return parseLiteral(node, Instant.class, Instant::parse, Xsd.DATETIME, Xsd.DATETIMESTAMP);
    }

    public static LocalDate parseLocalDate(Node node) throws SQLException {
        return parseLiteral(node, LocalDate.class, LocalDate::parse, Xsd.DATE, Xsd.DATETIME, Xsd.DATETIMESTAMP);
    }

    public static LocalDateTime parseLocalDateTime(Node node) throws SQLException {
        return parseLiteral(node, LocalDateTime.class, LocalDateTime::parse, Xsd.DATE, Xsd.DATETIME, Xsd.DATETIMESTAMP);
    }

    public static LocalTime parseLocalTime(Node node) throws SQLException {
        return parseLiteral(node, LocalTime.class, LocalTime::parse, Xsd.TIME, Xsd.DATETIME, Xsd.DATETIMESTAMP);
    }

    public static Long parseLong(Node node) throws SQLException {
        return parseNumber(node, Long.class, Long::parseLong);
    }

    public static long parseLong(Node node, long defaultValue) throws SQLException {
        return or(parseLong(node), defaultValue);
    }

    public static Month parseMonth(Node node) throws SQLException {
        return parseLiteral(node, Month.class, (s) -> Month.of(Integer.parseInt(s)), Xsd.GMONTH);
    }

    public static MonthDay parseMonthDay(Node node) throws SQLException {
        return parseLiteral(node, MonthDay.class, MonthDay::parse, Xsd.GMONTHDAY);
    }

    public static OffsetDateTime parseOffsetDateTime(Node node) throws SQLException {
        return parseLiteral(node, OffsetDateTime.class, OffsetDateTime::parse, Xsd.DATETIME, Xsd.DATETIMESTAMP);
    }

    public static OffsetTime parseOffsetTime(Node node) throws SQLException {
        return parseLiteral(node, OffsetTime.class, OffsetTime::parse, Xsd.TIME, Xsd.DATETIME, Xsd.DATETIMESTAMP);
    }

    public static Period parsePeriod(Node node) throws SQLException {
        return parseLiteral(node, Period.class, Period::parse, Xsd.YEARMONTHDURATION);
    }

    public static Short parseShort(Node node) throws SQLException {
        return parseNumber(node, Short.class, Short::parseShort);
    }

    public static short parseShort(Node node, short defaultValue) throws SQLException {
        return or(parseShort(node), defaultValue);
    }

    public static ZonedDateTime parseZonedDateTime(Node node) throws SQLException {
        return parseLiteral(node, ZonedDateTime.class, ZonedDateTime::parse, Xsd.DATETIME, Xsd.DATETIMESTAMP);
    }

    /** @deprecated The {@link #parseLocalDate(Node)} method is preferred. */
    @Deprecated
    public static java.util.Date parseUtilDate(Node node) throws SQLException {
        // Alternative implementation: java.sql.Date.valueOf(NodeValues.parseLocalDate(node))?
        return parseLiteral(node, java.util.Date.class, s -> new java.util.Date(parseGregorianCalendar(s).getTimeInMillis()),
                Xsd.DATE, Xsd.DATETIME, Xsd.DATETIMESTAMP);
    }

    /** @deprecated The {@link #parseLocalDate(Node)} method is preferred. */
    @Deprecated
    public static java.sql.Date parseSqlDate(Node node) throws SQLException {
        // Alternative implementation: java.sql.Date.valueOf(NodeValues.parseLocalDate(node))?
        return parseLiteral(node, java.sql.Date.class, s -> new java.sql.Date(parseGregorianCalendar(s).getTimeInMillis()),
                Xsd.DATE, Xsd.DATETIME, Xsd.DATETIMESTAMP);
    }

    /** @deprecated The {@link #parseLocalTime(Node)} and {@link #parseOffsetTime(Node)} methods are preferred. */
    @Deprecated
    public static java.sql.Time parseSqlTime(Node node) throws SQLException {
        // Alternative implementation: java.sql.Time.valueOf(LocalTime.from(NodeValues.parseBestTime(node)))?
        return parseLiteral(node, java.sql.Time.class, s -> new java.sql.Time(parseGregorianCalendar(s).getTimeInMillis()),
                Xsd.TIME, Xsd.DATETIME, Xsd.DATETIMESTAMP);
    }

    /** @deprecated The {@link #parseLocalDateTime(Node)} and {@link #parseOffsetDateTime(Node)} methods are preferred. */
    @Deprecated
    public static java.sql.Timestamp parseSqlTimestamp(Node node) throws SQLException {
        // Alternative implementation: java.sql.Timestamp.valueOf(LocalDateTime.from(NodeValues.parseBestDateTime(node)))?
        return parseLiteral(node, java.sql.Timestamp.class, s -> new java.sql.Timestamp(parseGregorianCalendar(s).getTimeInMillis()),
                Xsd.DATE, Xsd.TIME, Xsd.DATETIME, Xsd.DATETIMESTAMP);  // DATE and TIME are mandated by table B-6 in JDBC spec
    }

    public static URI parseUri(Node node) throws SQLException {
        return parseLiteralOrIri(node, URI.class, URI::new, Xsd.ANYURI);
    }

    /** @deprecated The {@link #parseUri(Node)} method is preferred. */
    @Deprecated
    public static URL parseUrl(Node node) throws SQLException {
        return parseLiteralOrIri(node, URL.class, s -> new URI(s).toURL(), Xsd.ANYURI);
    }

    public static Year parseYear(Node node) throws SQLException {
        return parseLiteral(node, Year.class, Year::parse, Xsd.GYEAR);
    }

    public static YearMonth parseYearMonth(Node node) throws SQLException {
        return parseLiteral(node, YearMonth.class, YearMonth::parse, Xsd.GYEARMONTH);
    }

    //
    // Private helper functions
    //

    /** Uses XMLGregorianCalendar to parse old date/time types, don't use for newer java.time objects. */
    private static GregorianCalendar parseGregorianCalendar(String lexicalForm) {
        // XMLGregorianCalendar is useful since it implements the same xsd date/time formats as rdf.
        // Can java.time.DateTimeFormatter parse methods replace XMLGregorianCalendar?  Not sure, using
        // XMLGregorianCalendar for compatibility with old date/time types for now, isolating new java.time
        // parsing to just java.time types.  Note that XMLGregorianCalendar probably knows whether or not the
        // time zone is specified, but we don't take advantage of that information.
        return XmlHolder.DATATYPE_FACTORY.newXMLGregorianCalendar(lexicalForm).toGregorianCalendar();
    }

    private static <V> V parseLiteral(Node node, Class<V> clazz, Parser<V> parser, Iri... allowedTypes) throws SQLException {
        return parseLiteral(node, clazz, (lexicalForm, datatype, language) -> parser.parse(lexicalForm), allowedTypes);
    }

    private static <V> V parseLiteral(Node node, Class<V> clazz, LiteralParser<V> parser, Iri... allowedTypes) throws SQLException {
        if (node == null) {
            return null;
        } else if (!(node instanceof Literal)) {
            throw new SQLException(parseErrorMessage(node, clazz));
        }
        Literal literal = (Literal) node;
        // Always allow parsing from non-localized xsd:string
        if (!Arrays.asList(allowedTypes).contains(literal.getDatatype()) &&
                !(literal.getDatatype().equals(Xsd.STRING) && literal.getLanguage() == null)) {
            throw new SQLException(parseErrorMessage(node, clazz));
        }
        try {
            return parser.parse(literal.getLexicalForm(), literal.getDatatype(), literal.getLanguage());
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException(parseErrorMessage(node, clazz), e);
        }
    }

    private static <V> V parseIri(Node node, Class<V> clazz, Parser<V> parser) throws SQLException {
        if (node == null) {
            return null;
        } else if (!(node instanceof Iri)) {
            throw new SQLException(parseErrorMessage(node, clazz));
        }
        Iri iri = (Iri) node;
        try {
            return parser.parse(iri.getIri());
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException(parseErrorMessage(node, clazz), e);
        }
    }

    private static <V> V parseLiteralOrIri(Node node, Class<V> clazz, Parser<V> parser, Iri... allowedLiteralTypes) throws SQLException {
        if (node == null) {
            return null;
        } else if (node instanceof Literal) {
            return parseLiteral(node, clazz, parser, allowedLiteralTypes);
        } else if (node instanceof Iri) {
            return parseIri(node, clazz, parser);
        } else {
            throw new SQLException(parseErrorMessage(node, clazz));
        }
    }

    static <V> String parseErrorMessage(Node source, Class<V> target) {
        return String.format("Unable to marshal %s to %s", typeName(source), target.getName());
    }

    /** Returns eg. "xsd:dateTime" given XsdDatatypes.DATETIME. */
    private static String typeName(Node node) {
        if (node instanceof Literal) {
            String string = ((Literal) node).getDatatype().getIri();
            switch (getNamespace(string)) {
                case Xsd.NS:
                    return "xsd:" + getFragment(string);
            }
            return string;
        } else if (node instanceof Iri) {
            return "iri";
        } else if (node instanceof Blank) {
            return "blank node";
        } else {
            return "unknown node";  // should never happen
        }
    }

    private static String getNamespace(String uri) {
        return uri.substring(0, uri.indexOf('#') + 1);
    }

    private static String getFragment(String uri) {
        return uri.substring(uri.indexOf('#') + 1);
    }

    /** Parses a literal lexical form to the native Java type {@code <V>}. */
    @FunctionalInterface
    private interface Parser<V> {
        V parse(String string) throws Exception;
    }

    /** Parses a literal lexical form to the native Java type {@code <V>} using datatype and language hints. */
    @FunctionalInterface
    private interface LiteralParser<V> {
        V parse(String lexicalForm, Iri datatype, String language) throws Exception;
    }
}
