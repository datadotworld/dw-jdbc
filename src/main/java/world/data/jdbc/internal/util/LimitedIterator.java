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

import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static world.data.jdbc.internal.util.Conditions.check;

/**
 * An {@code Iterator} that returns at most {@code max} items.
 */
public final class LimitedIterator<T> implements Iterator<T> {
    private final Iterator<T> delegate;
    private int remaining;

    public LimitedIterator(Iterator<T> delegate, int max) throws SQLException {
        check(max >= 0, "max must be non-negative");
        this.delegate = delegate;
        this.remaining = max;
    }

    @Override
    public boolean hasNext() {
        return remaining > 0 && delegate.hasNext();
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        remaining--;
        return delegate.next();
    }

    @Override
    public void remove() {
        delegate.remove();
    }
}
