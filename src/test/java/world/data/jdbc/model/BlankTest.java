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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class BlankTest {
    @Test
    public void testUnique() {
        Blank unique = Blank.unique();
        assertThat(unique).isEqualTo(unique);
        assertThat(Blank.unique()).isNotEqualTo(unique);
        assertThat(new Blank(unique.getLabel())).isEqualTo(unique);
    }

    @Test
    public void testEquals() {
        assertThat(new Blank("a")).isEqualTo(new Blank("a"));
        assertThat(new Blank("a")).isNotEqualTo(new Blank("b"));
    }

    @Test
    public void testOkLabel() {
        assertThat(new Blank("a")).hasToString("_:a");
        assertThat(new Blank(":")).hasToString("_::");
        assertThat(new Blank("_")).hasToString("_:_");
        assertThat(new Blank("_:")).hasToString("_:_:");
        assertThat(new Blank("_-")).hasToString("_:_-");
        assertThat(new Blank("_.-")).hasToString("_:_.-");
        assertThat(new Blank("_:bB1-:.-")).hasToString("_:_:bB1-:.-");
        assertThat(new Blank("å")).hasToString("_:å");
        assertThat(new Blank("0")).hasToString("_:0");
    }

    @Test
    public void testJenaLabel() {
        assertThat(new Blank("1718b11a:15b1b136d7b:-7ff7")).hasToString("_:1718b11a:15b1b136d7b:-7ff7");
    }

    @Test
    public void testBadLabel() {
        assertBadLabel("");
        assertBadLabel(" ");
        assertBadLabel("'");
        assertBadLabel("\"");
        assertBadLabel("a\n");
        assertBadLabel("-a");
        assertBadLabel(".a");
        assertBadLabel("a.");
    }

    private void assertBadLabel(String label) {
        assertThatThrownBy(() -> new Blank(label)).isInstanceOf(IllegalArgumentException.class);
    }
}
