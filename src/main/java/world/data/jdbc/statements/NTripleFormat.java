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

import org.apache.jena.atlas.io.StringWriterI;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.out.NodeFormatter;
import org.apache.jena.riot.out.NodeFormatterNT;
import org.apache.jena.riot.tokens.Token;
import org.apache.jena.riot.tokens.Tokenizer;
import org.apache.jena.riot.tokens.TokenizerFactory;

import static org.apache.jena.riot.tokens.TokenType.STRING2;

class NTripleFormat {
    private static final NodeFormatter FORMATTER = new NodeFormatterNT();

    /**
     * Formats a node in N-Triple representation.
     */
    static String toString(Node node) {
        StringWriterI buf = new StringWriterI();
        FORMATTER.format(buf, node);
        return buf.toString();
    }

    /**
     * Parses a string in one of the following formats:
     * <pre>
     * &lt;http://example.com#p>
     * "hello world"
     * "hello world"@en
     * "3.14159"^^&lt;http://www.w3.org/2001/XMLSchema#decimal>
     * _:label
     * </pre>
     * This rejects any strings that wouldn't be valid in an N-Triples file.
     */
    static Node parseNode(String string) {
        // This is similar to NodeFactoryExtra.parseNode() but is more strict in that it only accepts strings
        // formatted in N-Triples format, it will reject single quoted strings, unquoted numbers, 'true', 'false' etc.
        try {
            Tokenizer tokenizer = TokenizerFactory.makeTokenizerString(string);
            Token token;
            if (!tokenizer.hasNext() || !isNTripleTerm(token = tokenizer.next()) || tokenizer.hasNext()) {
                throw new IllegalArgumentException("Invalid NTriple-encoded value: " + string);
            }
            return token.asNode();
        } catch (RiotException e) {
            throw new IllegalArgumentException("Invalid NTriple-encoded value: " + string);
        }
    }

    private static boolean isNTripleTerm(Token token) {
        switch (token.getType()) {
            case IRI:
            case BNODE:
            case STRING2:
                return true;
            case LITERAL_DT:
            case LITERAL_LANG:
                return token.getSubToken1().hasType(STRING2);
            default:
                return false;
        }
    }
}