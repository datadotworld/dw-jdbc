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
package world.data.jdbc.internal.util;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CharTableTest {
    @Test
    public void testRange() {
        // Test w/PN_CHARS range from https://www.w3.org/TR/n-triples/#n-triples-grammar
        CharTable chars = CharTable.forRange("-A-Za-z0-9_:", true);
        assertThat(chars).hasToString("CharTable[-0123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz+non-ascii]");
        for (char ch = 0; ch < 128; ch++) {
            boolean expected = Character.isLetterOrDigit(ch) || "-_:".indexOf(ch) != -1;
            assertThat(chars.contains(ch)).as(Character.toString(ch)).isEqualTo(expected);
        }
        assertThat(chars.contains('\u0370')).isTrue();
        assertThat(chars.contains('\ufffd')).isTrue();
    }

    @Test
    public void testInvertedRange() {
        // Test w/IRIREF range between '<', '>' from https://www.w3.org/TR/n-triples/#n-triples-grammar
        CharTable chars = CharTable.forRange("\u0000-\u0020<>\"{}|^`\\", false).invert();
        assertThat(chars).hasToString("CharTable[!#$%&'()*+,-./0123456789:;=?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[]_abcdefghijklmnopqrstuvwxyz~\u007F+non-ascii]");
        for (char ch = 0; ch < 128; ch++) {
            boolean expected = ch > ' ' && "<>\"{}|^`\\".indexOf(ch) == -1;
            assertThat(chars.contains(ch)).as(Character.toString(ch)).isEqualTo(expected);
        }
        assertThat(chars.contains('\u0370')).isTrue();
        assertThat(chars.contains('\ufffd')).isTrue();
    }
}
