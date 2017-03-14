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
