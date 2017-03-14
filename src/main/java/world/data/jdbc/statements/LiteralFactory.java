/*
* dw-jdbc
* Copyright 2016 data.world, Inc.

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
package world.data.jdbc.statements;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.graph.NodeConst;
import org.apache.jena.sparql.util.NodeFactoryExtra;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.util.Calendar;
import java.util.TimeZone;

class LiteralFactory {

    static Node booleanToNode(boolean value) {
        return value ? NodeConst.nodeTrue : NodeConst.nodeFalse;
    }

    static Node shortToNode(short value) {
        return NodeFactory.createLiteral(Short.toString(value), XSDDatatype.XSDshort);
    }

    static Node byteToNode(byte value) {
        return NodeFactory.createLiteral(Byte.toString(value), XSDDatatype.XSDbyte);
    }

    static Node floatToNode(float value) {
        if (Float.isInfinite(value)) {
            return NodeFactory.createLiteral(value > 0 ? "INF" : "-INF", XSDDatatype.XSDfloat);
        } else {
            return NodeFactoryExtra.floatToNode(value);
        }
    }

    static Node doubleToNode(double value) {
        if (Double.isInfinite(value)) {
            return NodeFactory.createLiteral(value > 0 ? "INF" : "-INF", XSDDatatype.XSDdouble);
        } else {
            return NodeFactoryExtra.doubleToNode(value);
        }
    }

    static Node bigDecimalToNode(BigDecimal value) {
        return NodeFactory.createLiteral(value.toPlainString(), XSDDatatype.XSDdecimal);
    }

    static Node dateTimeToNode(Date value) {
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        c.setTimeInMillis(value.getTime());
        return NodeFactoryExtra.dateTimeToNode(c);
    }

    static Node timeToNode(Date value) {
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        c.setTimeInMillis(value.getTime());
        return NodeFactoryExtra.timeToNode(c);
    }

    static Node timeToNode(Time value) {
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        c.setTimeInMillis(value.getTime());
        return NodeFactoryExtra.timeToNode(c);
    }
}
