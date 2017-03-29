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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class LiteralTest {
    @Test
    public void testEscaping() {
        assertThat(new Literal("åbc def: \u0001\t\b\n\r\f\"'\\", Xsd.STRING))
                .hasToString("\"åbc def: \u0001\\t\\b\\n\\r\\f\\\"'\\\\\"");
    }

    @Test
    public void testLanguage() {
        assertThat(new Literal("foo", Xsd.STRING, "en"))
                .hasToString("\"foo\"@en");

        assertThatThrownBy(() -> new Literal("foo", Xsd.STRING, "en-"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid language tag: en-");

        assertThatThrownBy(() -> new Literal("foo", Xsd.INT, "en"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Language tag with datatype other than xsd:string: <http://www.w3.org/2001/XMLSchema#int>");
    }
}
