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

import world.data.jdbc.internal.types.NTriplesFormat;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLStreamHandlerFactory;

import static java.util.Objects.requireNonNull;

/**
 * An international resource identifier as described by <a href="https://www.ietf.org/rfc/rfc3987.txt">RFC&nbsp;3987</a>.
 * <p>
 * This is a superset of uri (as described by <a href="https://www.ietf.org/rfc/rfc3986.txt">RFC&nbsp;3986</a>) that
 * allows for unescaped non-ascii (unicode) characters.  Note that {@link URI#toASCIIString()} restricts URIs to
 * ascii characters as described by RFC&nbsp;3896 but the {@code URI} class itself accepts URIs with non-ascii
 * characters and generally handles them just fine.
 */
@lombok.Value
@SuppressWarnings("WeakerAccess")
public final class Iri implements Node {
    private final String iri;

    public Iri(URI uri) {
        this(uri.toString());
    }

    public Iri(URL url) {
        this(url.toString());
    }

    public Iri(String iri) {
        this.iri = requireNonNull(iri, "iri");
    }

    public URI toURI() {
        return URI.create(iri);
    }

    /**
     * URL support is included for compatibility, but note that Java's {@link URL} constructor rejects protocols
     * it doesn't know about unless you set a custom {@link URLStreamHandlerFactory}.  The list of protocols
     * supported by default include the following: file, ftp, http, https, jar, mailto, netdoc.  For most RDF
     * purposes it's far better to use {@link URI} instead.
     */
    public URL toURL() throws MalformedURLException {
        // URL constructor recommends starting with URI then using toURL() to ensure encoding/decoding occurs correctly
        return toURI().toURL();
    }

    /** Returns the IRI formatted as an NTriples-compatible string. */
    @Override
    public String toString() {
        return NTriplesFormat.formatIri(iri);
    }
}
