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

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class IriTest {

    @Test
    public void testNull() {
        assertThatThrownBy(() -> new Iri((String) null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testUnescaped() throws Exception {
        assertUnescaped("http://example.com;bar?a=b#foo");
        assertUnescaped("http://example.com/å∫ç∂∆");
    }

    @Test
    public void testMailUri() throws Exception {
        Iri iri = new Iri("mailto:bob@work.example.org");
        assertThat(iri).hasToString("<mailto:bob@work.example.org>");
        assertThat(iri.toURI()).isEqualTo(URI.create("mailto:bob@work.example.org"));
        assertThat(iri.toURL()).isEqualTo(new URL("mailto:bob@work.example.org"));
    }

    @Test
    public void testBogoUrl() throws Exception {
        Iri iri = new Iri("bogo://some@custom:uri/protocol");
        assertThat(iri).hasToString("<bogo://some@custom:uri/protocol>");
        assertThat(iri.toURI()).isEqualTo(URI.create("bogo://some@custom:uri/protocol"));
        assertThatThrownBy(iri::toURL).isInstanceOf(MalformedURLException.class).hasMessage("unknown protocol: bogo");
    }

    private void assertUnescaped(String uri) throws Exception {
        Iri iri = new Iri(uri);
        assertThat(iri).hasToString("<" + uri + ">");
        assertThat(iri.toURI()).hasToString(uri);
        assertThat(iri.toURL()).hasToString(uri);
    }

    @Test
    public void testEscape() {
        assertThat(new Iri("< \t\n\\∂X\">")).hasToString("<\\u003c\\u0020\\u0009\\u000a\\u005c∂X\\u0022\\u003e>");
    }
}
