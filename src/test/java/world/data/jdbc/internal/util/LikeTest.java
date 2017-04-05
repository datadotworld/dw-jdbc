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

public class LikeTest {
    @Test
    public void testMatches() throws Exception {
        assertThat(Like.matches("foo", "%")).isTrue();
        assertThat(Like.matches("foo", "foo%")).isTrue();
        assertThat(Like.matches("hello world", "hello%world")).isTrue();
        assertThat(Like.matches("foo", "foo")).isTrue();
        assertThat(Like.matches("foo", "fo_")).isTrue();
        assertThat(Like.matches("foo\n", "foo_")).isTrue();
        assertThat(Like.matches("fo", "fo_")).isFalse();
        assertThat(Like.matches("foab", "fo%a")).isFalse();
    }
}
