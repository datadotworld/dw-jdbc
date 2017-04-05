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
import world.data.jdbc.vocab.Rdfs;
import world.data.jdbc.vocab.Xsd;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeConstants;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
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
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.Calendar;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static world.data.jdbc.internal.util.Optionals.or;

/**
 * Utility methods for converting {@link Node} objects to plain old Java objects, usually by parsing the 'lexicalForm'
 * property of a {@link Literal}.  Many of these methods will convert between types as necessary to support required
 * JDBC functionality.
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

    /** Any plain integer numeric type: no fraction, exponential, infinity, NaN. */
    private static final Iri[] INTEGER_TYPES = {
            Xsd.INTEGER,
            Xsd.BYTE,
            Xsd.INT,
            Xsd.SHORT,
            Xsd.LONG,
            Xsd.UNSIGNEDBYTE,
            Xsd.UNSIGNEDINT,
            Xsd.UNSIGNEDSHORT,
            Xsd.UNSIGNEDLONG,
            Xsd.POSITIVEINTEGER,
            Xsd.NEGATIVEINTEGER,
            Xsd.NONPOSITIVEINTEGER,
            Xsd.NONNEGATIVEINTEGER,
    };

    /** Types that can be coerced to 'boolean'. */
    private static final Iri[] BOOLEAN_AND_NUMBER_TYPES = append(INTEGER_TYPES,
            Xsd.BOOLEAN,
            Xsd.DECIMAL,
            Xsd.DOUBLE,
            Xsd.FLOAT
    );

    /** Types that can be coerced to numbers. */
    private static final Iri[] BOOLEAN_AND_NUMERIC_TYPES = append(BOOLEAN_AND_NUMBER_TYPES,
            Xsd.GYEAR,
            Xsd.GMONTH,
            Xsd.GDAY
    );

    // Omit time zone since java.time.Year doesn't support it
    private static final DateTimeFormatter ISO_MONTH = new DateTimeFormatterBuilder()
            .appendLiteral("--")
            .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NOT_NEGATIVE)
            .toFormatter();

    // Omit time zone since java.time.Month doesn't support it
    private static final DateTimeFormatter ISO_DAY = new DateTimeFormatterBuilder()
            .appendLiteral("---")
            .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NOT_NEGATIVE)
            .toFormatter();

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
                return parseStringAsBoolean(lexicalForm);
            } else {
                return parseNumberAsBoolean(lexicalForm);
            }
        }, BOOLEAN_AND_NUMBER_TYPES);
    }

    private static Boolean parseStringAsBoolean(String lexicalForm) {
        // SQL-style "true", "yes", "1", "-1" being very liberal about ignoring chars after the first
        int ch = lexicalForm.isEmpty() ? 0 : Character.toLowerCase(lexicalForm.charAt(0));
        return ch == 't' || ch == 'y' || ch == '1' || "-1".equals(lexicalForm);
    }

    private static Boolean parseNumberAsBoolean(String lexicalForm) {
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

    public static boolean parseBoolean(Node node, boolean defaultValue) throws SQLException {
        return or(parseBoolean(node), defaultValue);
    }

    // This method is located near parseBoolean() to make it easier to keep their implementations in sync
    private static <V> V parseNumber(Node node, Class<V> clazz, Parser<V> parser) throws SQLException {
        return parseLiteral(node, clazz, (String lexicalForm, Iri datatype, String language) -> {
            // JDBC spec allows freely converting back and forth between booleans and numbers
            if (Xsd.BOOLEAN.equals(datatype)) {
                lexicalForm = Boolean.parseBoolean(lexicalForm) ? "1" : "0";
            } else if (Xsd.GYEAR.equals(datatype)) {
                lexicalForm = Integer.toString(Year.parse(lexicalForm).getValue());
            } else if (Xsd.GMONTH.equals(datatype)) {
                lexicalForm = Integer.toString(parseMonth(lexicalForm).getValue());
            } else if (Xsd.GDAY.equals(datatype)) {
                lexicalForm = Integer.toString(parseDay(lexicalForm));
            }
            return parser.parse(lexicalForm);
        }, BOOLEAN_AND_NUMERIC_TYPES);
    }

    public static Byte parseByte(Node node) throws SQLException {
        // note: decode() supports several ways of representing a byte value
        return parseNumber(node, Byte.class, Byte::decode);
    }

    public static byte parseByte(Node node, byte defaultValue) throws SQLException {
        return or(parseByte(node), defaultValue);
    }

    public static Integer parseDay(Node node) throws SQLException {
        return parseLiteral(node, Integer.class, (String lexicalForm, Iri datatype, String language) -> {
            if (Xsd.GDAY.equals(datatype)) {
                return parseDay(lexicalForm);
            } else {
                return ChronoField.DAY_OF_MONTH.checkValidIntValue(Integer.parseInt(lexicalForm));
            }
        }, append(INTEGER_TYPES, Xsd.GDAY));
    }

    private static int parseDay(String string) {
        return ISO_DAY.parse(string).get(ChronoField.DAY_OF_MONTH);
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
        return parseLiteral(node, Month.class, (String lexicalForm, Iri datatype, String language) -> {
            if (Xsd.GMONTH.equals(datatype)) {
                return parseMonth(lexicalForm);
            } else {
                return Month.of(Integer.parseInt(lexicalForm));
            }
        }, append(INTEGER_TYPES, Xsd.GMONTH));
    }

    private static Month parseMonth(String s) {
        return ISO_MONTH.parse(s, Month::from);
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
        return parseLiteral(node, java.util.Date.class,
                s -> new java.util.Date(parseCalendar(s, Calendar.getInstance()).getTimeInMillis()),
                Xsd.DATE, Xsd.DATETIME, Xsd.DATETIMESTAMP);
    }

    /** @deprecated The {@link #parseLocalDate(Node)} method is preferred. */
    @Deprecated
    @SuppressWarnings("deprecation")
    public static java.sql.Date parseSqlDate(Node node) throws SQLException {
        return parseSqlDate(node, Calendar.getInstance());
    }

    /** @deprecated The {@link #parseLocalDate(Node)} method is preferred. */
    @Deprecated
    public static java.sql.Date parseSqlDate(Node node, Calendar calendar) throws SQLException {
        return parseLiteral(node, java.sql.Date.class,
                s -> new java.sql.Date(parseCalendar(s, calendar).getTimeInMillis()),
                Xsd.DATE, Xsd.DATETIME, Xsd.DATETIMESTAMP);
    }

    /** @deprecated The {@link #parseLocalTime(Node)} and {@link #parseOffsetTime(Node)} methods are preferred. */
    @Deprecated
    @SuppressWarnings("deprecation")
    public static java.sql.Time parseSqlTime(Node node) throws SQLException {
        return parseSqlTime(node, Calendar.getInstance());
    }

    /** @deprecated The {@link #parseLocalTime(Node)} and {@link #parseOffsetTime(Node)} methods are preferred. */
    @Deprecated
    public static java.sql.Time parseSqlTime(Node node, Calendar calendar) throws SQLException {
        return parseLiteral(node, java.sql.Time.class,
                s -> new java.sql.Time(parseCalendar(s, calendar).getTimeInMillis()),
                Xsd.TIME, Xsd.DATETIME, Xsd.DATETIMESTAMP);
    }

    /** @deprecated The {@link #parseLocalDateTime(Node)} and {@link #parseOffsetDateTime(Node)} methods are preferred. */
    @Deprecated
    @SuppressWarnings("deprecation")
    public static java.sql.Timestamp parseSqlTimestamp(Node node) throws SQLException {
        return parseSqlTimestamp(node, Calendar.getInstance());
    }

    /** @deprecated The {@link #parseLocalDateTime(Node)} and {@link #parseOffsetDateTime(Node)} methods are preferred. */
    @Deprecated
    public static java.sql.Timestamp parseSqlTimestamp(Node node, Calendar calendar) throws SQLException {
        return parseLiteral(node, java.sql.Timestamp.class,
                s -> new java.sql.Timestamp(parseCalendar(s, calendar).getTimeInMillis()),
                Xsd.DATE, Xsd.TIME, Xsd.DATETIME, Xsd.DATETIMESTAMP);  // DATE and TIME are mandated by table B-6 in JDBC spec
    }

    public static URI parseUri(Node node) throws SQLException {
        return parseLiteralOrIri(node, URI.class, URI::new, Rdfs.RESOURCE, Xsd.ANYURI);
    }

    /** @deprecated The {@link #parseUri(Node)} method is preferred. */
    @Deprecated
    public static URL parseUrl(Node node) throws SQLException {
        return parseLiteralOrIri(node, URL.class, s -> new URI(s).toURL(), Rdfs.RESOURCE, Xsd.ANYURI);
    }

    public static Year parseYear(Node node) throws SQLException {
        return parseLiteral(node, Year.class, (String lexicalForm, Iri datatype, String language) -> {
            if (Xsd.GYEAR.equals(datatype)) {
                return Year.parse(lexicalForm);
            } else {
                return Year.of(Integer.parseInt(lexicalForm));
            }
        }, append(INTEGER_TYPES, Xsd.GYEAR));
    }

    public static YearMonth parseYearMonth(Node node) throws SQLException {
        return parseLiteral(node, YearMonth.class, YearMonth::parse, Xsd.GYEARMONTH);
    }

    //
    // Private helper functions
    //

    @SuppressWarnings("NumericOverflow")
    private static Calendar parseCalendar(String lexicalForm, Calendar calendar) {
        // XMLGregorianCalendar is useful since it implements the same xsd date/time formats as rdf.
        // Can java.time.DateTimeFormatter parse methods replace XMLGregorianCalendar?  Not sure, using
        // XMLGregorianCalendar for compatibility with old java.sql date/time types for now, isolating new
        // java.time parsing to just java.time types.
        XMLGregorianCalendar xmlCalendar = XmlHolder.DATATYPE_FACTORY.newXMLGregorianCalendar(lexicalForm);
        if (xmlCalendar.getTimezone() != DatatypeConstants.FIELD_UNDEFINED) {
            calendar.setTimeZone(xmlCalendar.getTimeZone(DatatypeConstants.FIELD_UNDEFINED));
        }
        // java.sql epoch start is 1970-01-01T00:00:00.000
        setWithDefault(calendar, Calendar.YEAR, xmlCalendar.getYear(), 1970, 0);
        setWithDefault(calendar, Calendar.MONTH, xmlCalendar.getMonth(), 1, -1);
        setWithDefault(calendar, Calendar.DAY_OF_MONTH, xmlCalendar.getDay(), 1, 0);
        setWithDefault(calendar, Calendar.HOUR_OF_DAY, xmlCalendar.getHour(), 0, 0);
        setWithDefault(calendar, Calendar.MINUTE, xmlCalendar.getMinute(), 0, 0);
        setWithDefault(calendar, Calendar.SECOND, xmlCalendar.getSecond(), 0, 0);
        setWithDefault(calendar, Calendar.MILLISECOND, xmlCalendar.getMillisecond(), 0, 0);
        return calendar;
    }

    private static void setWithDefault(Calendar calendar, int field, int value, int defaultValue, int adjust) {
        calendar.set(field, (value != DatatypeConstants.FIELD_UNDEFINED ? value : defaultValue) + adjust);
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
                case Rdfs.NS:
                    return "rdfs:" + getFragment(string);
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

    @SafeVarargs
    private static <T> T[] append(T[] a, T... b) {
        T[] t = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, t, a.length, b.length);
        return t;
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
