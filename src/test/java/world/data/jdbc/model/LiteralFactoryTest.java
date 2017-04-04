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

import org.junit.Test;
import world.data.jdbc.vocab.Xsd;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
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
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class LiteralFactoryTest {
    @Test
    public void testCreateString() throws Exception {
        assertThat(LiteralFactory.createString("foo bar\n")).hasToString(quoted("foo bar\\n"));
    }

    @Test
    public void testCreateString1() throws Exception {
        assertThat(LiteralFactory.createString("foo bar\n", "en")).hasToString(localized("foo bar\\n", "en"));
    }

    @Test
    public void testCreateBoolean() throws Exception {
        assertThat(LiteralFactory.createBoolean(false)).hasToString(typed("false", Xsd.BOOLEAN));
        assertThat(LiteralFactory.createBoolean(true)).hasToString(typed("true", Xsd.BOOLEAN));
    }

    @Test
    public void testCreateByte() throws Exception {
        assertThat(LiteralFactory.createByte((byte) 5)).hasToString(typed("5", Xsd.BYTE));
    }

    @Test
    public void testCreateShort() throws Exception {
        assertThat(LiteralFactory.createShort((short) 5)).hasToString(typed("5", Xsd.SHORT));
    }

    @Test
    public void testCreateInteger() throws Exception {
        assertThat(LiteralFactory.createInteger(5)).hasToString(typed("5", Xsd.INTEGER));
    }

    @Test
    public void testCreateBigInteger() throws Exception {
        assertThat(LiteralFactory.createInteger(10)).hasToString(typed("10", Xsd.INTEGER));
        assertThat(LiteralFactory.createInteger(BigInteger.valueOf(2).pow(81).subtract(BigInteger.ONE)))
                .hasToString(typed("2417851639229258349412351", Xsd.INTEGER));
    }

    @Test
    public void testCreateFloat() throws Exception {
        assertThat(LiteralFactory.createFloat(5.1f)).hasToString(typed("5.1", Xsd.FLOAT));
        assertThat(LiteralFactory.createFloat(Float.POSITIVE_INFINITY)).hasToString(typed("INF", Xsd.FLOAT));
        assertThat(LiteralFactory.createFloat(Float.NEGATIVE_INFINITY)).hasToString(typed("-INF", Xsd.FLOAT));
        assertThat(LiteralFactory.createFloat(Float.NaN)).hasToString(typed("NaN", Xsd.FLOAT));
    }

    @Test
    public void testCreateDouble() throws Exception {
        assertThat(LiteralFactory.createDouble(5.1f)).hasToString(typed("5.099999904632568", Xsd.DOUBLE));
        assertThat(LiteralFactory.createDouble(Double.POSITIVE_INFINITY)).hasToString(typed("INF", Xsd.DOUBLE));
        assertThat(LiteralFactory.createDouble(Double.NEGATIVE_INFINITY)).hasToString(typed("-INF", Xsd.DOUBLE));
        assertThat(LiteralFactory.createDouble(Double.NaN)).hasToString(typed("NaN", Xsd.DOUBLE));
    }

    @Test
    public void testCreateDecimal() throws Exception {
        assertThat(LiteralFactory.createDecimal(new BigDecimal("5.1"))).hasToString(typed("5.1", Xsd.DECIMAL));
    }

    @Test
    public void testCreateYear() throws Exception {
        assertThat(LiteralFactory.createYear(Year.of(0))).hasToString(typed("0", Xsd.GYEAR));
        assertThat(LiteralFactory.createYear(Year.of(2017))).hasToString(typed("2017", Xsd.GYEAR));

        LocalDateTime localDateTime = LocalDateTime.of(2016, 12, 31, 22, 25, 41, 123_450_000);
        OffsetDateTime offsetDateTime = OffsetDateTime.of(localDateTime, ZoneOffset.MIN);
        assertThat(LiteralFactory.createYear(localDateTime)).hasToString(typed("2016", Xsd.GYEAR));
        assertThat(LiteralFactory.createYear(offsetDateTime)).hasToString(typed("2017", Xsd.GYEAR));
    }

    @Test
    public void testCreateMonth() throws Exception {
        assertThat(LiteralFactory.createMonth(Month.JANUARY)).hasToString(typed("--1", Xsd.GMONTH));
        assertThat(LiteralFactory.createMonth(Month.OCTOBER)).hasToString(typed("--10", Xsd.GMONTH));

        LocalDateTime localDateTime = LocalDateTime.of(2016, 12, 31, 22, 25, 41, 123_450_000);
        OffsetDateTime offsetDateTime = OffsetDateTime.of(localDateTime, ZoneOffset.MIN);
        assertThat(LiteralFactory.createMonth(localDateTime)).hasToString(typed("--12", Xsd.GMONTH));
        assertThat(LiteralFactory.createMonth(offsetDateTime)).hasToString(typed("--1", Xsd.GMONTH));
    }

    @Test
    public void testCreateYearMonth() throws Exception {
        assertThat(LiteralFactory.createYearMonth(YearMonth.of(0, 1))).hasToString(typed("0000-01", Xsd.GYEARMONTH));
        assertThat(LiteralFactory.createYearMonth(YearMonth.of(2017, 5))).hasToString(typed("2017-05", Xsd.GYEARMONTH));

        LocalDateTime localDateTime = LocalDateTime.of(2016, 12, 31, 22, 25, 41, 123_450_000);
        OffsetDateTime offsetDateTime = OffsetDateTime.of(localDateTime, ZoneOffset.MIN);
        assertThat(LiteralFactory.createYearMonth(localDateTime)).hasToString(typed("2016-12", Xsd.GYEARMONTH));
        assertThat(LiteralFactory.createYearMonth(offsetDateTime)).hasToString(typed("2017-01", Xsd.GYEARMONTH));
    }

    @Test
    public void testCreateMonthDay() throws Exception {
        assertThat(LiteralFactory.createMonthDay(MonthDay.of(2, 29))).hasToString(typed("--02-29", Xsd.GMONTHDAY));
        assertThat(LiteralFactory.createMonthDay(MonthDay.of(12, 5))).hasToString(typed("--12-05", Xsd.GMONTHDAY));

        LocalDateTime localDateTime = LocalDateTime.of(2016, 12, 31, 22, 25, 41, 123_450_000);
        OffsetDateTime offsetDateTime = OffsetDateTime.of(localDateTime, ZoneOffset.MIN);
        assertThat(LiteralFactory.createMonthDay(localDateTime)).hasToString(typed("--12-31", Xsd.GMONTHDAY));
        assertThat(LiteralFactory.createMonthDay(offsetDateTime)).hasToString(typed("--01-01", Xsd.GMONTHDAY));
    }

    @Test
    public void testCreateYearMonthDuration() throws Exception {
        assertThat(LiteralFactory.createYearMonthDuration(Period.ofDays(0))).hasToString(typed("P0D", Xsd.YEARMONTHDURATION));
        assertThat(LiteralFactory.createYearMonthDuration(Period.of(3, 5, 0))).hasToString(typed("P3Y5M", Xsd.YEARMONTHDURATION));
        assertThatThrownBy(() -> LiteralFactory.createYearMonthDuration(Period.ofDays(1))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testCreateDayTimeDuration() throws Exception {
        assertThat(LiteralFactory.createDayTimeDuration(Duration.ofSeconds(0))).hasToString(typed("PT0S", Xsd.DAYTIMEDURATION));
        assertThat(LiteralFactory.createDayTimeDuration(Duration.ofSeconds(123456789))).hasToString(typed("PT34293H33M9S", Xsd.DAYTIMEDURATION));
    }

    @Test
    public void testCreateDate_Local() throws Exception {
        LocalDate localDate = LocalDate.of(2017, 3, 27);

        //noinspection deprecation
        assertThat(LiteralFactory.createDate(Date.valueOf(localDate))).hasToString(typed("2017-03-27", Xsd.DATE));

        assertThat(LiteralFactory.createDate(localDate)).hasToString(typed("2017-03-27", Xsd.DATE));

        LocalDateTime localDateTime = LocalDateTime.of(2016, 12, 31, 22, 25, 41, 123_450_000);
        OffsetDateTime offsetDateTime = OffsetDateTime.of(localDateTime, ZoneOffset.MIN);
        assertThat(LiteralFactory.createDate(localDateTime)).hasToString(typed("2016-12-31", Xsd.DATE));
        assertThat(LiteralFactory.createDate(offsetDateTime)).hasToString(typed("2017-01-01", Xsd.DATE));
    }

    @Test
    public void testCreateDateTime_Local() throws Exception {
        LocalDateTime localDateTime = LocalDateTime.of(2017, 3, 27, 16, 25, 41, 123_450_000);

        //noinspection deprecation
        assertThat(LiteralFactory.createDateTime(Timestamp.valueOf(localDateTime.withSecond(0).withNano(0))))
                .hasToString(typed("2017-03-27T16:25:00", Xsd.DATETIME));
        //noinspection deprecation
        assertThat(LiteralFactory.createDateTime(Timestamp.valueOf(localDateTime)))
                .hasToString(typed("2017-03-27T16:25:41.12345", Xsd.DATETIME));

        assertThat(LiteralFactory.createDateTime(localDateTime.withSecond(0).withNano(0)))
                .hasToString(typed("2017-03-27T16:25:00", Xsd.DATETIME));
        assertThat(LiteralFactory.createDateTime(localDateTime))
                .hasToString(typed("2017-03-27T16:25:41.12345", Xsd.DATETIME));
    }

    @Test
    public void testCreateDateTime_Zoned() throws Exception {
        LocalDateTime localDateTime = LocalDateTime.of(2017, 3, 27, 16, 25, 41, 123_450_000);

        assertThat(LiteralFactory.createDateTime(OffsetDateTime.of(localDateTime, ZoneOffset.UTC)))
                .hasToString(typed("2017-03-27T16:25:41.12345Z", Xsd.DATETIME));
        assertThat(LiteralFactory.createDateTime(OffsetDateTime.of(localDateTime, ZoneOffset.ofHours(6))))
                .hasToString(typed("2017-03-27T10:25:41.12345Z", Xsd.DATETIME));
        assertThat(LiteralFactory.createDateTime(ZonedDateTime.of(localDateTime, ZoneOffset.UTC)))
                .hasToString(typed("2017-03-27T16:25:41.12345Z", Xsd.DATETIME));
        assertThat(LiteralFactory.createDateTime(ZonedDateTime.of(localDateTime, ZoneOffset.ofHours(6))))
                .hasToString(typed("2017-03-27T10:25:41.12345Z", Xsd.DATETIME));
        assertThat(LiteralFactory.createDateTime(ZonedDateTime.of(localDateTime, ZoneOffset.ofHours(-10))))
                .hasToString(typed("2017-03-28T02:25:41.12345Z", Xsd.DATETIME));
        assertThat(LiteralFactory.createDateTime(ZonedDateTime.of(localDateTime, ZoneOffset.ofHours(6)).toInstant()))
                .hasToString(typed("2017-03-27T10:25:41.12345Z", Xsd.DATETIME));

        LocalDateTime localDateTime2 = LocalDateTime.of(2016, 12, 31, 22, 25, 41, 123_450_000);
        OffsetDateTime offsetDateTime = OffsetDateTime.of(localDateTime2, ZoneOffset.MIN);
        assertThat(LiteralFactory.createDateTime(localDateTime2)).hasToString(typed("2016-12-31T22:25:41.12345", Xsd.DATETIME));
        assertThat(LiteralFactory.createDateTime(offsetDateTime)).hasToString(typed("2017-01-01T16:25:41.12345Z", Xsd.DATETIME));
    }

    @Test
    public void testCreateTime_Local() throws Exception {
        LocalTime localTime = LocalTime.of(16, 25, 41, 123_450_000);

        //noinspection deprecation
        assertThat(LiteralFactory.createTime(Time.valueOf(localTime.withSecond(0).withNano(0))))
                .hasToString(typed("16:25:00", Xsd.TIME));

        //noinspection deprecation
        assertThat(LiteralFactory.createTime(localTimeToTimeWithMillis(localTime))).hasToString(typed("16:25:41.123", Xsd.TIME));

        assertThat(LiteralFactory.createTime(localTime.withSecond(0).withNano(0)))
                .hasToString(typed("16:25:00", Xsd.TIME));
        assertThat(LiteralFactory.createTime(localTime))
                .hasToString(typed("16:25:41.12345", Xsd.TIME));
    }

    @Test
    public void testCreateTime_Zoned() throws Exception {
        LocalTime localTime = LocalTime.of(16, 25, 41, 123_450_000);

        assertThat(LiteralFactory.createTime(OffsetTime.of(localTime.withSecond(0).withNano(0), ZoneOffset.UTC)))
                .hasToString(typed("16:25:00Z", Xsd.TIME));
        assertThat(LiteralFactory.createTime(OffsetTime.of(localTime, ZoneOffset.UTC)))
                .hasToString(typed("16:25:41.12345Z", Xsd.TIME));
        assertThat(LiteralFactory.createTime(OffsetTime.of(localTime, ZoneOffset.ofHours(6))))
                .hasToString(typed("10:25:41.12345Z", Xsd.TIME));
        assertThat(LiteralFactory.createTime(OffsetTime.of(localTime, ZoneOffset.ofHours(-10))))
                .hasToString(typed("02:25:41.12345Z", Xsd.TIME));

        LocalDateTime localDateTime = LocalDateTime.of(2016, 12, 31, 22, 25, 41, 123_450_000);
        OffsetDateTime offsetDateTime = OffsetDateTime.of(localDateTime, ZoneOffset.MIN);
        assertThat(LiteralFactory.createTime(localDateTime)).hasToString(typed("22:25:41.12345", Xsd.TIME));
        assertThat(LiteralFactory.createTime(offsetDateTime)).hasToString(typed("16:25:41.12345Z", Xsd.TIME));
    }

    private String quoted(String escapedString) {
        return String.format("\"%s\"", escapedString);
    }

    private String typed(String escapedString, Iri type) {
        return String.format("\"%s\"^^%s", escapedString, type);
    }

    private String localized(String escapedString, String lang) {
        return String.format("\"%s\"@%s", escapedString, lang);
    }

    private Time localTimeToTimeWithMillis(LocalTime localTime) {
        // It is surprisingly awkward to create a java.sql.Time from a LocalTime w/fractional seconds?
        Time time = Time.valueOf(localTime);
        time.setTime(time.getTime() + localTime.getNano() / 1_000_000);
        return time;
    }
}
