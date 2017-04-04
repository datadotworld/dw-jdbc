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
import world.data.jdbc.internal.util.CharTable;
import world.data.jdbc.model.Iri;
import world.data.jdbc.vocab.Xsd;

import java.util.regex.Pattern;

@UtilityClass
public final class NTriplesFormat {
    private static final Pattern PATTERN_LANGTAG = Pattern.compile("[a-zA-Z]+(-[a-zA-Z0-9]+)*");

    // See https://www.w3.org/TR/n-triples/#n-triples-grammar BLANK_NODE_LABEL, except don't bother w/unicode ranges
    private static final CharTable BLANK_FIRST = CharTable.forRange("A-Za-z0-9_:", true);  // PN_CHARS_U|[0-9]
    private static final CharTable BLANK_MIDDLE = CharTable.forRange("-A-Za-z0-9_:.", true);  // PN_CHARS|'.'
    private static final CharTable BLANK_LAST = CharTable.forRange("-A-Za-z0-9_:", true);  // PN_CHARS

    // See https://www.w3.org/TR/n-triples/#n-triples-grammar IRIREF, except don't bother w/unicode ranges
    private static final CharTable IRI_UNESCAPED = CharTable.forRange("\u0000-\u0020<>\"{}|^`\\", false).invert();  // IRIREF

    public static boolean isWellFormedLanguage(String language) {
        return PATTERN_LANGTAG.matcher(language).matches();
    }

    public static boolean isWellFormedLabel(String label) {
        return label.length() != 0 && BLANK_FIRST.contains(label.charAt(0)) &&
                (label.length() <= 2 || BLANK_MIDDLE.matchesAll(label, 1, label.length() - 1)) &&
                (label.length() <= 1 || BLANK_LAST.contains(label.charAt(label.length() - 1)));
    }

    public static String formatLiteral(String lexicalForm, Iri datatype, String language) {
        int padding = 5; // 2 for quotes and 3 for up to 3 escaped chars
        if (language != null && !language.isEmpty()) {
            // Localized string
            StringBuilder buf = new StringBuilder(lexicalForm.length() + language.length() + 1 + padding);
            appendQuoted(buf, lexicalForm);
            buf.append('@');
            buf.append(language);
            return buf.toString();
        } else if (Xsd.STRING.equals(datatype)) {
            // Plain-old string
            StringBuilder buf = new StringBuilder(lexicalForm.length() + padding);
            appendQuoted(buf, lexicalForm);
            return buf.toString();
        } else {
            // Typed string
            StringBuilder buf = new StringBuilder(lexicalForm.length() + datatype.getIri().length() + 4 + padding);
            appendQuoted(buf, lexicalForm);
            buf.append("^^").append(datatype);
            return buf.toString();
        }
    }

    private static void appendQuoted(StringBuilder buf, String string) {
        // See https://www.w3.org/TR/n-triples/#n-triples-grammar STRING_LITERAL_QUOTE
        buf.append('"');
        int len = string.length();
        for (int i = 0; i < len; i++) {
            char c = string.charAt(i);
            switch (c) {
                case '\n':
                    c = 'n';
                    buf.append('\\');
                    break;
                case '\t':
                    c = 't';
                    buf.append('\\');
                    break;
                case '\r':
                    c = 'r';
                    buf.append('\\');
                    break;
                case '\b':
                    c = 'b';
                    buf.append('\\');
                    break;
                case '\f':
                    c = 'f';
                    buf.append('\\');
                    break;
                case '\\':
                case '"':
                    buf.append('\\');
                    break;
            }
            buf.append(c);
        }
        buf.append('"');
    }

    public static String formatIri(String iri) {
        if (IRI_UNESCAPED.matchesAll(iri)) {
            // This is the common case
            return '<' + iri + '>';
        } else {
            // We may end up escaping characters that aren't allowed in IRIs, but that's ok as long as the
            // ntriple parser can round trip the formatted string.
            StringBuilder buf = new StringBuilder(iri.length() + 14);
            buf.append('<');
            for (int i = 0, len = iri.length(); i < len; i++) {
                char ch = iri.charAt(i);
                if (IRI_UNESCAPED.contains(ch)) {
                    buf.append(ch);
                } else {
                    buf.append(String.format("\\u%04x", (int) ch));
                }
            }
            buf.append('>');
            return buf.toString();
        }
    }

    public static String formatBlank(String label) {
        // Since there is no standard mechanism for escaping characters w/in a blank node label, make the
        // reasonable assumption that 'label' is valid and doesn't contain illegal characters,
        return "_:" + label;
    }
}
