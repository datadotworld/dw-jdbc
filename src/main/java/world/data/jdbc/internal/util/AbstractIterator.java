/*
 * Copyright (C) 2007 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package world.data.jdbc.internal.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A copy of Guava's {@code AbstractIterator} to avoid pulling in the entirety of Guava as a dependency.
 */
public abstract class AbstractIterator<T> implements Iterator<T> {
    private State state = State.NOT_READY;

    protected AbstractIterator() {
    }

    private enum State {
        /** We have computed the next element and haven't returned it yet. */
        READY,

        /** We haven't yet computed or have already returned the element. */
        NOT_READY,

        /** We have reached the end of the data and are finished. */
        DONE,

        /** We've suffered an exception and are kaput. */
        FAILED,
    }

    private T next;

    protected abstract T computeNext();

    protected final T endOfData() {
        state = State.DONE;
        return null;
    }

    @Override
    public final boolean hasNext() {
        switch (state) {
            case DONE:
                return false;
            case READY:
                return true;
            case FAILED:
                throw new IllegalStateException();
            default:
        }
        return tryToComputeNext();
    }

    private boolean tryToComputeNext() {
        state = State.FAILED; // temporary pessimism
        next = computeNext();
        if (state != State.DONE) {
            state = State.READY;
            return true;
        }
        return false;
    }

    @Override
    public final T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        state = State.NOT_READY;
        T result = next;
        next = null;
        return result;
    }
}
