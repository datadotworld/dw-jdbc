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

/**
 * A simple utility for testing whether char values are within a particular set, specialized for characters in the
 * ASCII range (0-127).
 */
public final class CharTable {
    /** b0 and b1 implement a bit set for 0-127. */
    private final long b0, b1;
    /** Include non-ascii unicode characters? */
    private final boolean nonAscii;

    public static CharTable forRange(String range, boolean nonAscii) {
        // Encode the ascii range 0-127 as a bit set backed by 2 numbers of type long
        // Simple char ranges of the form 'a-z' are supported, similar to regex character classes
        long b0 = 0, b1 = 0;
        for (int pos = 0, len = range.length(); pos < len; pos++) {
            char lo = range.charAt(pos);
            char hi = lo;
            if (pos + 2 < len && range.charAt(pos + 1) == '-') {
                hi = range.charAt(pos + 2);
                pos += 2;
            }
            for (char ch = lo; ch <= hi; ch++) {
                if (ch < 0x40) {
                    b0 |= (1L << ch);
                } else if (ch < 0x80) {
                    b1 |= (1L << (ch & 0x3f));
                } else {
                    throw new UnsupportedOperationException("Range string contains non-ascii characters: " + range);
                }
            }
        }
        return new CharTable(b0, b1, nonAscii);
    }

    private CharTable(long b0, long b1, boolean nonAscii) {
        this.b0 = b0;
        this.b1 = b1;
        this.nonAscii = nonAscii;
    }

    public CharTable invert() {
        return new CharTable(~b0, ~b1, !nonAscii);
    }

    public boolean contains(char ch) {
        return ch < 0x80 ? ((ch < 0x40 ? b0 : b1) & (1L << (ch & 0x3f))) != 0 : nonAscii;
    }

    public boolean matchesAll(String s) {
        return matchesAll(s, 0, s.length());
    }

    public boolean matchesAll(String s, int start, int end) {
        for (int i = start; i < end; i++) {
            if (!contains(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /** For debugging. */
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append(getClass().getSimpleName()).append('[');
        for (char ch = 0; ch < 0x80; ch++) {
            if (contains(ch)) {
                buf.append(ch);
            }
        }
        if (nonAscii) {
            buf.append("+non-ascii");
        }
        buf.append(']');
        return buf.toString();
    }
}
