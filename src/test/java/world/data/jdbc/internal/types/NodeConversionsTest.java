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
import world.data.jdbc.model.Blank;
import world.data.jdbc.model.Iri;
import world.data.jdbc.model.LiteralFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;

public class NodeConversionsTest {

    @Test
    public void testString() throws Exception {
        assertThat(NodeConversions.toString(null)).isNull();
        assertThat(NodeConversions.toString(LiteralFactory.createString("foo bar\n"))).isEqualTo("foo bar\n");
        assertThat(NodeConversions.toString(LiteralFactory.createString("foo bar\n", "en"))).isEqualTo("foo bar\n");
        assertThat(NodeConversions.toString(new Iri("http://example.com#foo"))).isEqualTo("http://example.com#foo");
        assertThat(NodeConversions.toString(new Blank("a123"))).isEqualTo("_:a123");
    }

    @Test
    public void testToNode() throws SQLException {
        assertThat(NodeConversions.toNode(null)).isNull();
        assertThat(NodeConversions.toNode(true)).isEqualTo(LiteralFactory.createBoolean(true));
        assertThat(NodeConversions.toNode((byte) 1)).isEqualTo(LiteralFactory.createByte((byte) 1));
        assertThat(NodeConversions.toNode((short) 1)).isEqualTo(LiteralFactory.createShort((short) 1));
        assertThat(NodeConversions.toNode(1)).isEqualTo(LiteralFactory.createInteger(1));
        assertThat(NodeConversions.toNode(1.2f)).isEqualTo(LiteralFactory.createFloat(1.2f));
        assertThat(NodeConversions.toNode(1.2d)).isEqualTo(LiteralFactory.createDouble(1.2));
        assertThat(NodeConversions.toNode(BigInteger.TEN)).isEqualTo(LiteralFactory.createInteger(BigInteger.TEN));
        assertThat(NodeConversions.toNode(BigDecimal.TEN)).isEqualTo(LiteralFactory.createDecimal(BigDecimal.TEN));
    }
}
