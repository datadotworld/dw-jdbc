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
package world.data.jdbc.model;

import lombok.experimental.UtilityClass;
import world.data.jdbc.vocab.Xsd;

import java.math.BigDecimal;
import java.math.BigInteger;
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
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Calendar;
import java.util.Optional;

/**
 * Factory methods for {@link Literal} objects of various data types.  It's OK to create {@link Literal} objects
 * directly with {@code new Literal(...)} but these methods may be more convenient.
 */
@UtilityClass
@SuppressWarnings({"WeakerAccess", "DeprecatedIsStillUsed"})
public final class LiteralFactory {

    public static final Literal TRUE = new Literal("true", Xsd.BOOLEAN);
    public static final Literal FALSE = new Literal("false", Xsd.BOOLEAN);

    public static Literal createString(String string) {
        return new Literal(string, Xsd.STRING);
    }

    public static Literal createString(String string, String lang) {
        return new Literal(string, Xsd.STRING, lang);
    }

    public static Literal createBoolean(boolean value) {
        return value ? TRUE : FALSE;
    }

    public static Literal createByte(byte value) {
        return new Literal(Byte.toString(value), Xsd.BYTE);
    }

    public static Literal createShort(short value) {
        return new Literal(Short.toString(value), Xsd.SHORT);
    }

    public static Literal createInt(int value) {
        return new Literal(Integer.toString(value), Xsd.INT);
    }

    public static Literal createLong(long value) {
        return new Literal(Long.toString(value), Xsd.LONG);
    }

    public static Literal createInteger(long value) {
        return new Literal(Long.toString(value), Xsd.INTEGER);
    }

    public static Literal createInteger(BigInteger value) {
        return new Literal(value.toString(), Xsd.INTEGER);
    }

    public static Literal createFloat(float value) {
        return new Literal(!Float.isInfinite(value) ? Float.toString(value) : value > 0 ? "INF" : "-INF", Xsd.FLOAT);
    }

    public static Literal createDouble(double value) {
        return new Literal(!Double.isInfinite(value) ? Double.toString(value) : value > 0 ? "INF" : "-INF", Xsd.DOUBLE);
    }

    public static Literal createDecimal(BigDecimal value) {
        return new Literal(value.toPlainString(), Xsd.DECIMAL);
    }

    public static Literal createYearMonthDuration(Period value) {
        if (value.getDays() != 0) {
            throw new IllegalArgumentException("Year month duration may not contain days: " + value);
        }
        return new Literal(value.toString(), Xsd.YEARMONTHDURATION);
    }

    public static Literal createDayTimeDuration(Duration value) {
        return new Literal(value.toString(), Xsd.DAYTIMEDURATION);
    }

    /** Returns an {@code xsd:gYearMonth}. */
    public static Literal createYearMonth(TemporalAccessor value) {
        return createYearMonth(YearMonth.from(toUtcDateTime(value).orElse(value)));
    }

    /** Returns an {@code xsd:gYearMonth}. */
    public static Literal createYearMonth(YearMonth value) {
        return new Literal(value.toString(), Xsd.GYEARMONTH);
    }

    /** Returns an {@code xsd:gMonthDay}. */
    public static Literal createMonthDay(TemporalAccessor value) {
        return createMonthDay(MonthDay.from(toUtcDateTime(value).orElse(value)));
    }

    /** Returns an {@code xsd:gMonthDay}. */
    public static Literal createMonthDay(MonthDay value) {
        return new Literal(value.toString(), Xsd.GMONTHDAY);
    }

    /** Returns an {@code xsd:gYear}. */
    public static Literal createYear(TemporalAccessor value) {
        return createYear(Year.from(toUtcDateTime(value).orElse(value)));
    }

    /** Returns an {@code xsd:gYear}. */
    public static Literal createYear(Year value) {
        return new Literal(value.toString(), Xsd.GYEAR);
    }

    /** Returns an {@code xsd:gMonth}. */
    public static Literal createMonth(TemporalAccessor value) {
        return createMonth(Month.from(toUtcDateTime(value).orElse(value)));
    }

    /** Returns an {@code xsd:gMonth}. */
    public static Literal createMonth(Month value) {
        return new Literal("--" + value.getValue(), Xsd.GMONTH);
    }

    /** Returns an {@code xsd:date}. */
    public static Literal createDate(TemporalAccessor value) {
        return createDate(LocalDate.from(toUtcDateTime(value).orElse(value)));
    }

    /** Returns an {@code xsd:date}. */
    public static Literal createDate(LocalDate value) {
        return new Literal(DateTimeFormatter.ISO_LOCAL_DATE.format(value), Xsd.DATE);
    }

    /** Returns an {@code xsd:dateTime}.  It will have a 'Z' time zone suffix if the temporal value has sufficient info. */
    public static Literal createDateTime(TemporalAccessor value) {
        return toUtcDateTime(value).map(t -> LiteralFactory.createDateTime(OffsetDateTime.from(t)))
                .orElseGet(() -> LiteralFactory.createDateTime(LocalDateTime.from(value)));
    }

    /** Returns an {@code xsd:dateTime} with a 'Z' suffix. */
    public static Literal createDateTime(OffsetDateTime value) {
        value = value.withOffsetSameInstant(ZoneOffset.UTC);
        return new Literal(trimZeros(DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(value)), Xsd.DATETIME);
    }

    /** Returns an {@code xsd:dateTime}. */
    public static Literal createDateTime(LocalDateTime value) {
        return new Literal(trimZeros(DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(value)), Xsd.DATETIME);
    }

    /** Returns an {@code xsd:time}.  It will have a 'Z' time zone suffix if the temporal value has sufficient info. */
    public static Literal createTime(TemporalAccessor value) {
        return toUtcTime(value).map(t -> LiteralFactory.createTime(OffsetTime.from(t)))
                .orElseGet(() -> LiteralFactory.createTime(LocalTime.from(value)));
    }

    /** Returns an {@code xsd:time} with a 'Z' suffix. */
    public static Literal createTime(OffsetTime value) {
        value = value.withOffsetSameInstant(ZoneOffset.UTC);
        return new Literal(trimZeros(DateTimeFormatter.ISO_OFFSET_TIME.format(value)), Xsd.TIME);
    }

    /** Returns an {@code xsd:time}. */
    public static Literal createTime(LocalTime value) {
        return new Literal(trimZeros(DateTimeFormatter.ISO_LOCAL_TIME.format(value)), Xsd.TIME);
    }

    //
    // The java.time.temporal.TemporalAccessor-related code is tricky to write and understand.  To help, here's
    // a table of temporal types and the ChronoFields they support, for relevant/interesting ChronoFields:
    //
    //             Essential To:           Instant             |   Local/Offset+Time/Date/DateTime    |      Year+Month+Day
    // Class              Field: InstantSeconds  NanoOfSecond  |  EpochDay  NanoOfDay  OffsetSeconds  |  Year  MonthOfYear  DayOfMonth
    // java.time.YearMonth        .               .            |   .         .          .             |   X     X            .
    // java.time.MonthDay         .               .            |   .         .          .             |   .     X            X
    // java.time.LocalTime        .               X            |   .         X          .             |   .     .            .
    // java.time.OffsetTime       .               X            |   .         X          X             |   .     .            .
    // java.time.LocalDate        .               .            |   X         .          .             |   X     X            X
    // java.time.LocalDateTime    .               X            |   X         X          .             |   X     X            X
    // java.time.OffsetDateTime   X               X            |   X         X          X             |   X     X            X
    // java.time.ZonedDateTime    X               X            |   X         X          X             |   X     X            X
    // java.time.Instant          X               X            |   .         .          .             |   .     .            .
    //

    /** Converts the value to an {@link OffsetDateTime} in the UTC time zone, if possible. */
    private Optional<TemporalAccessor> toUtcDateTime(TemporalAccessor value) {
        if (value.isSupported(ChronoField.OFFSET_SECONDS)) {
            // Has time zone so normalize to utc, eg. OffsetDateTime, ZonedDateTime
            return Optional.of(OffsetDateTime.from(value).withOffsetSameInstant(ZoneOffset.UTC));
        } else if (value.isSupported(ChronoField.INSTANT_SECONDS)) {
            // Assume utc, eg. Instant
            return Optional.of(Instant.from(value).atOffset(ZoneOffset.UTC));
        }
        return Optional.empty();
    }

    /** Converts the value to an {@link OffsetTime} in the UTC time zone, if possible. */
    private Optional<TemporalAccessor> toUtcTime(TemporalAccessor value) {
        if (value.isSupported(ChronoField.OFFSET_SECONDS)) {
            // Has time zone so normalize to utc, eg. OffsetTime, OffsetDateTime, ZonedDateTime
            return Optional.of(OffsetTime.from(value).withOffsetSameInstant(ZoneOffset.UTC));
        } else if (value.isSupported(ChronoField.INSTANT_SECONDS)) {
            // Assume utc, Eg. Instant
            return Optional.of(Instant.from(value).atOffset(ZoneOffset.UTC).toOffsetTime());
        }
        return Optional.empty();
    }

    /** Returns a {@code xsd:dateTime} with a UTC time zone suffix. */
    public static Literal createDateTime(java.util.Date value) {
        return createDateTime(value.toInstant());
    }

    /** Returns a {@code xsd:dateTime} with a UTC time zone suffix. */
    public static Literal createDateTime(Calendar value) {
        return createDateTime(value.toInstant());
    }

    /**
     * Returns a {@code xsd:date} without a time zone suffix.
     *
     * @deprecated The {@link java.time.Instant} type is preferred.
     */
    @Deprecated
    public static Literal createDate(java.util.Date value) {
        return createDate(value.toInstant());
    }

    /**
     * Returns a {@code xsd:date} without a time zone suffix.
     *
     * @deprecated The {@link java.time.LocalDate} type is preferred.
     */
    @Deprecated
    public static Literal createDate(java.sql.Date value) {
        return createDate(value.toLocalDate());
    }

    /**
     * Returns a {@code xsd:dateTime} without a time zone suffix.
     *
     * @deprecated The {@link java.time.LocalDateTime} and {@link java.time.OffsetDateTime} types are preferred.
     */
    @Deprecated
    public static Literal createDateTime(java.sql.Timestamp value) {
        return createDateTime(value.toLocalDateTime());
    }

    /**
     * Returns a {@code xsd:time} without a time zone suffix.
     *
     * @deprecated The {@link java.time.LocalTime} and {@link java.time.OffsetTime} types are preferred.
     */
    @Deprecated
    public static Literal createTime(java.sql.Time value) {
        return createTime((TemporalAccessor) value.toLocalTime().withNano((int) (value.getTime() % 1000) * 1_000_000));
    }

    /** Trims trailing zeros in the fractional component of a time string. */
    private static String trimZeros(String string) {
        boolean utc = string.endsWith("Z");
        int end = string.length() - 1 - (utc ? 1 : 0);
        // Find the location of the last 'significant' digit, the last non-zero
        int sig = end;
        for (; sig >= 0; sig--) {
            if (string.charAt(sig) != '0') {
                break;
            }
        }
        if (sig == end) {
            return string;  // Nothing to trim
        }
        // Find the location of the decimal separator, the last '.' character
        int dec = sig;
        for (; dec >= 0; dec--) {
            char ch = string.charAt(dec);
            if (ch == '.') {
                // Trim trailing zeros between 'sig' and 'end', possibly removing the decimal separator as well
                int stop = (dec == sig) ? sig : (sig + 1);
                StringBuilder buf = new StringBuilder(stop + (utc ? 1 : 0));
                buf.append(string, 0, stop);
                if (utc) {
                    buf.append('Z');
                }
                return buf.toString();
            } else if (ch < '0' || ch > '9') {
                break;
            }
        }
        return string;  // Nothing to trim
    }
}
