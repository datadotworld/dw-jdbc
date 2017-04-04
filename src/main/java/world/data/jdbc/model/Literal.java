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

import lombok.Value;
import world.data.jdbc.internal.types.NTriplesFormat;
import world.data.jdbc.vocab.Xsd;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

@Value
public final class Literal implements Node {
    @Nonnull
    private String lexicalForm;
    @Nonnull
    private Iri datatype;
    private String language;

    public Literal(String lexicalForm, Iri datatype) {
        this(lexicalForm, datatype, null);
    }

    public Literal(String lexicalForm, Iri datatype, @Nullable String language) {
        this.lexicalForm = requireNonNull(lexicalForm, "lexicalForm");
        this.datatype = requireNonNull(datatype, "datatype");
        this.language = language;

        if (language != null) {
            if (!NTriplesFormat.isWellFormedLanguage(language)) {
                throw new IllegalArgumentException("Invalid language tag: " + language);
            } else if (!Xsd.STRING.equals(datatype)) {
                throw new IllegalArgumentException("Language tag with datatype other than xsd:string: " + datatype);
            }
        }
    }

    /** Returns the literal formatted as an NTriples-compatible string. */
    @Override
    public String toString() {
        return NTriplesFormat.formatLiteral(lexicalForm, datatype, language);
    }
}
