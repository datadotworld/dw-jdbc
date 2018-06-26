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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class LimitedIteratorTest {

    @Test
    public void testEmpty() throws Exception {
        Iterator<String> sourceIter = Arrays.asList("a", "b").iterator();
        LimitedIterator<String> limitedIter = new LimitedIterator<>(sourceIter, 0);
        assertThat(limitedIter.hasNext()).isFalse();
        assertThatThrownBy(limitedIter::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    public void testOne() throws Exception {
        Iterator<String> sourceIter = Arrays.asList("a", "b").iterator();
        LimitedIterator<String> limitedIter = new LimitedIterator<>(sourceIter, 1);

        assertThat(limitedIter.hasNext()).isTrue();
        assertThat(limitedIter.next()).isEqualTo("a");

        assertThat(limitedIter.hasNext()).isFalse();
        assertThatThrownBy(limitedIter::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    public void testMore() throws Exception {
        Iterator<String> sourceIter = Collections.singletonList("a").iterator();
        LimitedIterator<String> limitedIter = new LimitedIterator<>(sourceIter, 2);

        assertThat(limitedIter.hasNext()).isTrue();
        assertThat(limitedIter.next()).isEqualTo("a");

        assertThat(limitedIter.hasNext()).isFalse();
        assertThatThrownBy(limitedIter::next).isInstanceOf(NoSuchElementException.class);
    }
}
