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
package world.data.jdbc.vocab;


import lombok.experimental.UtilityClass;
import world.data.jdbc.model.Iri;


/**
 * Common RDF datatypes derived from the
 * <a href="https://www.w3.org/TR/xmlschema11-2/#built-in-datatypes">XSD specification</a>.
 */
@UtilityClass
public final class Xsd {
    public static final String NS = "http://www.w3.org/2001/XMLSchema#";

    private static Iri xsd(String suffix) {
        return new Iri((NS + suffix).intern());
    }

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#anyURI">xsd:anyURI</a>. */
    public static final Iri ANYURI = xsd("anyURI");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#base64Binary">xsd:base64Binary</a>. */
    public static final Iri BASE64BINARY = xsd("base64Binary");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#boolean">xsd:boolean</a>. */
    public static final Iri BOOLEAN = xsd("boolean");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#byte">xsd:byte</a>. */
    public static final Iri BYTE = xsd("byte");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#date">xsd:date</a>. */
    public static final Iri DATE = xsd("date");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#dateTime">xsd:dateTime</a>. */
    public static final Iri DATETIME = xsd("dateTime");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#dateTimeStamp">xsd:dateTimeStamp</a>. */
    public static final Iri DATETIMESTAMP = xsd("dateTimeStamp");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#dayTimeDuration">xsd:dayTimeDuration</a>. */
    public static final Iri DAYTIMEDURATION = xsd("dayTimeDuration");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#decimal">xsd:decimal</a>. */
    public static final Iri DECIMAL = xsd("decimal");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#double">xsd:double</a>. */
    public static final Iri DOUBLE = xsd("double");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#duration">xsd:duration</a>. */
    public static final Iri DURATION = xsd("duration");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#ENTITY">xsd:ENTITY</a>. */
    public static final Iri ENTITY = xsd("ENTITY");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#float">xsd:float</a>. */
    public static final Iri FLOAT = xsd("float");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#gDay">xsd:gDay</a>. */
    public static final Iri GDAY = xsd("gDay");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#gMonth">xsd:gMonth</a>. */
    public static final Iri GMONTH = xsd("gMonth");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#gMonthDay">xsd:gMonthDay</a>. */
    public static final Iri GMONTHDAY = xsd("gMonthDay");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#gYear">xsd:gYear</a>. */
    public static final Iri GYEAR = xsd("gYear");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#gYearMonth">xsd:gYearMonth</a>. */
    public static final Iri GYEARMONTH = xsd("gYearMonth");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#hexBinary">xsd:hexBinary</a>. */
    public static final Iri HEXBINARY = xsd("hexBinary");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#ID">xsd:ID</a>. */
    public static final Iri ID = xsd("ID");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#IDREF">xsd:IDREF</a>. */
    public static final Iri IDREF = xsd("IDREF");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#int">xsd:int</a>. */
    public static final Iri INT = xsd("int");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#integer">xsd:integer</a>. */
    public static final Iri INTEGER = xsd("integer");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#language">xsd:language</a>. */
    public static final Iri LANGUAGE = xsd("language");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#long">xsd:long</a>. */
    public static final Iri LONG = xsd("long");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#Name">xsd:Name</a>. */
    public static final Iri NAME = xsd("Name");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#NCName">xsd:NCName</a>. */
    public static final Iri NCNAME = xsd("NCName");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#negativeInteger">xsd:negativeInteger</a>. */
    public static final Iri NEGATIVEINTEGER = xsd("negativeInteger");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#NMTOKEN">xsd:NMTOKEN</a>. */
    public static final Iri NMTOKEN = xsd("NMTOKEN");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#nonNegativeInteger">xsd:nonNegativeInteger</a>. */
    public static final Iri NONNEGATIVEINTEGER = xsd("nonNegativeInteger");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#nonPositiveInteger">xsd:nonPositiveInteger</a>. */
    public static final Iri NONPOSITIVEINTEGER = xsd("nonPositiveInteger");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#normalizedString">xsd:normalizedString</a>. */
    public static final Iri NORMALIZEDSTRING = xsd("normalizedString");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#NOTATION">xsd:NOTATION</a>. */
    public static final Iri NOTATION = xsd("NOTATION");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#positiveInteger">xsd:positiveInteger</a>. */
    public static final Iri POSITIVEINTEGER = xsd("positiveInteger");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#QName">xsd:QName</a>. */
    public static final Iri QNAME = xsd("QName");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#short">xsd:short</a>. */
    public static final Iri SHORT = xsd("short");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#string">xsd:string</a>. */
    public static final Iri STRING = xsd("string");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#time">xsd:time</a>. */
    public static final Iri TIME = xsd("time");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#token">xsd:token</a>. */
    public static final Iri TOKEN = xsd("token");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#unsignedByte">xsd:unsignedByte</a>. */
    public static final Iri UNSIGNEDBYTE = xsd("unsignedByte");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#unsignedInt">xsd:unsignedInt</a>. */
    public static final Iri UNSIGNEDINT = xsd("unsignedInt");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#unsignedLong">xsd:unsignedLong</a>. */
    public static final Iri UNSIGNEDLONG = xsd("unsignedLong");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#unsignedShort">xsd:unsignedShort</a>. */
    public static final Iri UNSIGNEDSHORT = xsd("unsignedShort");

    /** See <a href="https://www.w3.org/TR/xmlschema11-2/#yearMonthDuration">xsd:yearMonthDuration</a>. */
    public static final Iri YEARMONTHDURATION = xsd("yearMonthDuration");
}
