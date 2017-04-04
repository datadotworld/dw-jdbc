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

import org.junit.Test;
import world.data.jdbc.model.Iri;
import world.data.jdbc.model.Literal;
import world.data.jdbc.model.LiteralFactory;
import world.data.jdbc.vocab.Rdfs;
import world.data.jdbc.vocab.Xsd;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URL;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static world.data.jdbc.testing.MoreAssertions.assertSQLException;

public class NodeValuesTest {

    @Test
    public void testBestDateTime() throws Exception {
        LocalDateTime localDateTime = LocalDateTime.of(2017, 3, 27, 16, 25, 41, 123_450_000);
        OffsetDateTime offsetDateTime = OffsetDateTime.of(2017, 3, 27, 16, 25, 41, 123_450_000, ZoneOffset.UTC);
        assertThat(NodeValues.parseBestDateTime(null)).isNull();
        assertThat(NodeValues.parseBestDateTime(LiteralFactory.createDateTime(localDateTime))).isEqualTo(localDateTime);
        assertThat(NodeValues.parseBestDateTime(LiteralFactory.createDateTime(offsetDateTime))).isEqualTo(offsetDateTime);
    }

    @Test
    public void testBestTime() throws Exception {
        LocalTime localTime = LocalTime.of(16, 25, 41, 123_450_000);
        OffsetTime offsetTime = OffsetTime.of(16, 25, 41, 123_450_000, ZoneOffset.UTC);
        assertThat(NodeValues.parseBestTime(null)).isNull();
        assertThat(NodeValues.parseBestTime(LiteralFactory.createTime(localTime))).isEqualTo(localTime);
        assertThat(NodeValues.parseBestTime(LiteralFactory.createTime(offsetTime))).isEqualTo(offsetTime);
    }

    @Test
    public void testBigInteger() throws Exception {
        BigInteger minLongSub1 = BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE);
        BigInteger maxLongAdd1 = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
        assertThat(NodeValues.parseBigInteger(null)).isNull();
        assertThat(NodeValues.parseBigInteger(LiteralFactory.createInteger(5))).isEqualTo(BigInteger.valueOf(5));
        assertThat(NodeValues.parseBestNumber(LiteralFactory.createInteger(minLongSub1))).isEqualTo(minLongSub1);
        assertThat(NodeValues.parseBestNumber(LiteralFactory.createInteger(maxLongAdd1))).isEqualTo(maxLongAdd1);
    }

    @Test
    public void testBigDecimal() throws Exception {
        assertThat(NodeValues.parseBigDecimal(null)).isNull();
        assertThat(NodeValues.parseBigDecimal(LiteralFactory.createDecimal(new BigDecimal("5.1")))).isEqualTo(new BigDecimal("5.1"));
    }

    @Test
    public void testBoolean() throws Exception {
        assertThat(NodeValues.parseBoolean(null)).isNull();
        assertThat(NodeValues.parseBoolean(null, false)).isFalse();
        assertThat(NodeValues.parseBoolean(LiteralFactory.createBoolean(false), false)).isFalse();
        assertThat(NodeValues.parseBoolean(LiteralFactory.createBoolean(true), false)).isTrue();
        assertThat(NodeValues.parseBoolean(LiteralFactory.createInteger(0), false)).isFalse();
        assertThat(NodeValues.parseBoolean(LiteralFactory.createInteger(-123), false)).isTrue();
        assertThat(NodeValues.parseBoolean(LiteralFactory.createDouble(0.0), false)).isFalse();
        assertThat(NodeValues.parseBoolean(LiteralFactory.createDouble(0.99), false)).isFalse();
        assertThat(NodeValues.parseBoolean(LiteralFactory.createDouble(5.1), false)).isTrue();
        assertThat(NodeValues.parseBoolean(LiteralFactory.createDouble(5.1e44), false)).isTrue();
        assertThat(NodeValues.parseBoolean(new Literal("0", Xsd.NONPOSITIVEINTEGER), false)).isFalse();
        assertThat(NodeValues.parseBoolean(new Literal("-123", Xsd.NONPOSITIVEINTEGER), false)).isTrue();
        assertThat(NodeValues.parseBoolean(LiteralFactory.createString(""))).isFalse();
        assertThat(NodeValues.parseBoolean(LiteralFactory.createString("false"))).isFalse();
        assertThat(NodeValues.parseBoolean(LiteralFactory.createString("true"))).isTrue();
        assertThat(NodeValues.parseBoolean(LiteralFactory.createString("FALSE"))).isFalse();
        assertThat(NodeValues.parseBoolean(LiteralFactory.createString("TRUE"))).isTrue();
        assertThat(NodeValues.parseBoolean(LiteralFactory.createString("no"))).isFalse();
        assertThat(NodeValues.parseBoolean(LiteralFactory.createString("yes"))).isTrue();
        assertThat(NodeValues.parseBoolean(LiteralFactory.createString("0"))).isFalse();
        assertThat(NodeValues.parseBoolean(LiteralFactory.createString("1"))).isTrue();
        assertThat(NodeValues.parseBoolean(LiteralFactory.createString("-1"))).isTrue();
        assertThat(NodeValues.parseBoolean(LiteralFactory.createString("2"))).isFalse();
        assertThat(NodeValues.parseBoolean(new Literal("123456789012345678901234567890", Xsd.INTEGER))).isTrue();
        assertThat(NodeValues.parseBoolean(LiteralFactory.createDouble(1e29))).isTrue();
        assertThat(NodeValues.parseBoolean(LiteralFactory.createDouble(Double.NEGATIVE_INFINITY))).isTrue();
        assertThat(NodeValues.parseBoolean(LiteralFactory.createDouble(Double.POSITIVE_INFINITY))).isTrue();
        assertThat(NodeValues.parseBoolean(LiteralFactory.createDouble(Double.NaN))).isFalse();
    }

    @Test
    public void testByte() throws Exception {
        assertThat(NodeValues.parseByte(null)).isNull();
        assertThat(NodeValues.parseByte(null, (byte) 0)).isEqualTo((byte) 0);
        assertThat(NodeValues.parseByte(LiteralFactory.createByte((byte) 5), (byte) 0)).isEqualTo((byte) 5);
    }

    @Test
    public void testShort() throws Exception {
        assertThat(NodeValues.parseShort(null, (short) 0)).isEqualTo((short) 0);
        assertThat(NodeValues.parseShort(LiteralFactory.createShort((short) 5), (short) 0)).isEqualTo((short) 5);
    }

    @Test
    public void testInteger() throws Exception {
        assertThat(NodeValues.parseInteger(null, 0)).isEqualTo(0);
        assertThat(NodeValues.parseInteger(LiteralFactory.createInteger(5), 0)).isEqualTo(5);
        assertThatThrownBy(() -> NodeValues.parseInteger(LiteralFactory.createInteger(Long.MAX_VALUE), 0))
                .isInstanceOf(SQLException.class).hasMessage("Unable to marshal xsd:integer to java.lang.Integer")
                .hasCauseInstanceOf(NumberFormatException.class);
        assertThat(NodeValues.parseInteger(LiteralFactory.TRUE)).isEqualTo(1);
        assertThat(NodeValues.parseInteger(LiteralFactory.FALSE)).isEqualTo(0);
        assertThat(NodeValues.parseBestNumber(LiteralFactory.createInteger(Long.MAX_VALUE))).isEqualTo(Long.MAX_VALUE);
        assertThat(NodeValues.parseBestNumber(LiteralFactory.createInteger(Long.MIN_VALUE))).isEqualTo(Long.MIN_VALUE);
    }

    @Test
    public void testLong() throws Exception {
        assertThat(NodeValues.parseLong(null, 0L)).isEqualTo(0L);
        assertThat(NodeValues.parseLong(LiteralFactory.createInteger(5), 0L)).isEqualTo(5L);
    }

    @Test
    public void testFloat() throws Exception {
        assertThat(NodeValues.parseFloat(null, 0f)).isEqualTo(0f);
        assertThat(NodeValues.parseFloat(LiteralFactory.createFloat(5.1f), 0f)).isEqualTo(5.1f);
        assertThat(NodeValues.parseFloat(LiteralFactory.createFloat(Float.POSITIVE_INFINITY), 0f)).isEqualTo(Float.POSITIVE_INFINITY);
        assertThat(NodeValues.parseFloat(LiteralFactory.createFloat(Float.NEGATIVE_INFINITY), 0f)).isEqualTo(Float.NEGATIVE_INFINITY);
        assertThat(NodeValues.parseFloat(LiteralFactory.createFloat(Float.NaN), 0f)).isEqualTo(Float.NaN);
        assertThat(NodeValues.parseFloat(LiteralFactory.TRUE)).isEqualTo(1f);
        assertThat(NodeValues.parseFloat(LiteralFactory.FALSE)).isEqualTo(0f);
    }

    @Test
    public void testDouble() throws Exception {
        assertThat(NodeValues.parseDouble(null, 0d)).isEqualTo(0);
        assertThat(NodeValues.parseDouble(LiteralFactory.createDouble(5.1f), 0d)).isEqualTo(5.1f);
        assertThat(NodeValues.parseDouble(LiteralFactory.createDouble(Double.POSITIVE_INFINITY), 0d)).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(NodeValues.parseDouble(LiteralFactory.createDouble(Double.NEGATIVE_INFINITY), 0d)).isEqualTo(Double.NEGATIVE_INFINITY);
        assertThat(NodeValues.parseDouble(LiteralFactory.createDouble(Double.NaN), 0d)).isEqualTo(Double.NaN);
    }

    @Test
    public void testDuration() throws Exception {
        Duration duration = Duration.ofSeconds(6 + 60 * (5 + 60 * (4 + 24 * 3))); // 3D4H5M6S
        assertThat(NodeValues.parseDuration(null)).isNull();
        assertThat(NodeValues.parseDuration(LiteralFactory.createDayTimeDuration(duration))).isEqualTo(duration);
    }

    @Test
    public void testPeriod() throws Exception {
        Period period = Period.of(3, 8, 0);
        assertThat(NodeValues.parsePeriod(null)).isNull();
        assertThat(NodeValues.parsePeriod(LiteralFactory.createYearMonthDuration(period))).isEqualTo(period);
    }

    @Test
    public void testLocalDateTime() throws Exception {
        LocalDateTime localDateTime = LocalDateTime.of(2017, 3, 27, 16, 25, 41, 123_450_000);
        OffsetDateTime offsetDateTime = OffsetDateTime.of(2017, 3, 27, 16, 25, 41, 123_450_000, ZoneOffset.UTC);
        assertThat(NodeValues.parseLocalDateTime(null)).isNull();
        assertThat(NodeValues.parseLocalDateTime(LiteralFactory.createDateTime(localDateTime))).isEqualTo(localDateTime);
        assertThatThrownBy(() -> NodeValues.parseLocalDateTime(LiteralFactory.createDateTime(offsetDateTime)))
                .isInstanceOf(SQLException.class).hasMessage("Unable to marshal xsd:dateTime to java.time.LocalDateTime")
                .hasCauseInstanceOf(DateTimeParseException.class);
    }

    @Test
    public void testLocalTime() throws Exception {
        LocalTime localTime = LocalTime.of(16, 25, 41, 123_450_000);
        OffsetTime offsetTime = OffsetTime.of(16, 25, 41, 123_450_000, ZoneOffset.UTC);
        assertThat(NodeValues.parseLocalTime(null)).isNull();
        assertThat(NodeValues.parseLocalTime(LiteralFactory.createTime(localTime))).isEqualTo(localTime);
        assertThatThrownBy(() -> NodeValues.parseLocalTime(LiteralFactory.createTime(offsetTime)))
                .isInstanceOf(SQLException.class).hasMessage("Unable to marshal xsd:time to java.time.LocalTime");
    }

    @Test
    public void testOffsetDateTime() throws Exception {
        LocalDateTime localDateTime = LocalDateTime.of(2017, 3, 27, 16, 25, 41, 123_450_000);
        OffsetDateTime offsetDateTime = OffsetDateTime.of(2017, 3, 27, 16, 25, 41, 123_450_000, ZoneOffset.UTC);
        assertThat(NodeValues.parseOffsetDateTime(null)).isNull();
        assertThat(NodeValues.parseOffsetDateTime(LiteralFactory.createDateTime(offsetDateTime))).isEqualTo(offsetDateTime);
        assertThatThrownBy(() -> NodeValues.parseOffsetDateTime(LiteralFactory.createDateTime(localDateTime)))
                .isInstanceOf(SQLException.class).hasMessage("Unable to marshal xsd:dateTime to java.time.OffsetDateTime");
    }

    @Test
    public void testOffsetTime() throws Exception {
        LocalTime localTime = LocalTime.of(16, 25, 41, 123_450_000);
        OffsetTime offsetTime = OffsetTime.of(16, 25, 41, 123_450_000, ZoneOffset.UTC);
        assertThat(NodeValues.parseOffsetTime(null)).isNull();
        assertThat(NodeValues.parseOffsetTime(LiteralFactory.createTime(offsetTime))).isEqualTo(offsetTime);
        assertThatThrownBy(() -> NodeValues.parseOffsetTime(LiteralFactory.createTime(localTime)))
                .isInstanceOf(SQLException.class).hasMessage("Unable to marshal xsd:time to java.time.OffsetTime");
    }

    @Test
    public void testYear() throws Exception {
        Year year = Year.of(2017);
        assertThat(NodeValues.parseYear(null)).isNull();
        assertThat(NodeValues.parseYear(LiteralFactory.createYear(year))).isEqualTo(year);
    }

    @Test
    public void testMonth() throws Exception {
        Month month = Month.MARCH;
        assertThat(NodeValues.parseMonth(null)).isNull();
        assertThat(NodeValues.parseMonth(LiteralFactory.createMonth(month))).isEqualTo(month);
        assertSQLException(() -> NodeValues.parseMonth(new Literal("---1", Xsd.GMONTH)));  // month of '-1' not allowed
    }

    @Test
    public void testDay() throws Exception {
        assertThat(NodeValues.parseDay(null)).isNull();
        assertThat(NodeValues.parseDay(LiteralFactory.createDay(28))).isEqualTo(28);
        assertThat(NodeValues.parseDay(new Literal("---1", Xsd.GDAY))).isEqualTo(1);
        assertThat(NodeValues.parseDay(new Literal("---01", Xsd.GDAY))).isEqualTo(1);
        assertSQLException(() -> NodeValues.parseDay(new Literal("----1", Xsd.GDAY)));
        assertSQLException(() -> NodeValues.parseDay(new Literal("---32", Xsd.GDAY)));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testSqlDate() throws Exception {
        LocalDate localDate = LocalDate.of(2017, 3, 27);
        assertThat(NodeValues.parseSqlDate(null)).isNull();
        assertThat(NodeValues.parseSqlDate(LiteralFactory.createDate(localDate))).isEqualTo(Date.valueOf(localDate));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testSqlTime() throws Exception {
        LocalTime localTime = LocalTime.of(16, 25, 41, 123_450_000);
        assertThat(NodeValues.parseSqlTime(null)).isNull();
        assertThat(NodeValues.parseSqlTime(LiteralFactory.createTime(localTime)))
                .isEqualTo(localTimeToTimeWithMillis(localTime));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testSqlTimestamp() throws Exception {
        LocalDateTime localDateTime = LocalDateTime.of(2017, 3, 27, 16, 25, 41, 123_450_000);
        assertThat(NodeValues.parseSqlTimestamp(null)).isNull();
        assertThat(NodeValues.parseSqlTimestamp(LiteralFactory.createDateTime(localDateTime)))
                .isEqualTo(Timestamp.valueOf(localDateTime.withNano(123_000_000)));
    }

    @Test
    public void testURI() throws Exception {
        assertThat(NodeValues.parseUri(null)).isNull();
        assertThat(NodeValues.parseUri(new Iri("http://example.com#foo"))).isEqualTo(URI.create("http://example.com#foo"));
        assertThat(NodeValues.parseUri(new Literal("http://example.com#foo", Xsd.STRING))).isEqualTo(URI.create("http://example.com#foo"));
        assertThat(NodeValues.parseUri(new Literal("http://example.com#foo", Xsd.ANYURI))).isEqualTo(URI.create("http://example.com#foo"));
        assertThat(NodeValues.parseUri(new Literal("http://example.com#foo", Rdfs.RESOURCE))).isEqualTo(URI.create("http://example.com#foo"));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testURL() throws Exception {
        assertThat(NodeValues.parseUrl(null)).isNull();
        assertThat(NodeValues.parseUrl(new Iri("http://example.com#foo"))).isEqualTo(new URL("http://example.com#foo"));
    }

    private Time localTimeToTimeWithMillis(LocalTime localTime) {
        // It is surprisingly awkward to create a java.sql.Time from a LocalTime w/fractional seconds?
        Time time = Time.valueOf(localTime);
        time.setTime(time.getTime() + localTime.getNano() / 1_000_000);
        return time;
    }
}
