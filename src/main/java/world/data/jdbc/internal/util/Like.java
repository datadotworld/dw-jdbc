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

import lombok.experimental.UtilityClass;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Helper for evaluating client-side LIKE patterns. */
@UtilityClass
class Like {
    private static final Pattern PART = Pattern.compile("(%+|_|[^%_]+)");

    static boolean matches(String string, String pattern) {
        // Only ok to check 'string.equals(pattern)' if like pattern support is limited to '%' and '_', not '[chars]'
        if ("%".equals(pattern) || string.equals(pattern)) {
            return true;
        }
        // Delegate to regex to do the pattern match
        return toRegex(pattern).matcher(string).matches();
    }

    private static Pattern toRegex(String pattern) {
        StringBuilder buf = new StringBuilder();
        Matcher matcher = PART.matcher(pattern);
        while (matcher.find()) {
            String part = matcher.group(1);
            switch (part) {
                case "%":
                    buf.append(".*");
                    break;
                case "_":
                    buf.append('.');
                    break;
                default:
                    buf.append(Pattern.quote(part));
                    break;
            }
        }
        return Pattern.compile(buf.toString(), Pattern.DOTALL);
    }
}
