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
package world.data.jdbc.testing;

import org.junit.rules.ExternalResource;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class CloserResource extends ExternalResource {
    private List<AutoCloseable> closeables;

    @Override
    protected void before() throws Throwable {
        this.closeables = new ArrayList<>();
    }

    public <T extends AutoCloseable> T register(T closeable) {
        requireNonNull(closeable, "closeable");
        requireNonNull(closeables, "closeables");
        closeables.add(closeable);
        return closeable;
    }

    public <T> T register(T obj, Consumer<T> closer) {
        requireNonNull(obj, "obj");
        requireNonNull(closer, "closer");
        register(() -> closer.accept(obj));
        return obj;
    }

    @Override
    protected void after() {
        try {
            closeAll(closeables);
        } finally {
            closeables = null;
        }
    }

    private static void closeAll(List<AutoCloseable> closeables) {
        RuntimeException first = null;
        for (AutoCloseable closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception e) {
                if (first == null) {
                    first = (e instanceof RuntimeException) ? (RuntimeException) e : new RuntimeException(e);
                } else {
                    first.addSuppressed(e);
                }
            }
        }
        if (first != null) {
            throw first;
        }
    }
}
