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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * Keeps track of a collection of resources to be closed when the {@code ResourceManager} itself are closed.
 * The resources are held using weak references so that the {@code ResourceManager} itself won't keep them
 * alive.  This is useful for resources like HTTP connections that are closed automatically via their finalize
 * method, but it's preferable for the application to close them explicitly before the garbage collector gets
 * around to them.
 */
public class ResourceManager implements AutoCloseable {
    private final Set<AutoCloseable> resources = Collections.newSetFromMap(new WeakHashMap<>());

    public synchronized void register(AutoCloseable closeable) {
        resources.add(closeable);
    }

    public synchronized void remove(AutoCloseable closeable) {
        resources.remove(closeable);
    }

    private synchronized List<AutoCloseable> snapshot() {
        return new ArrayList<>(resources);
    }

    @Override
    public void close() throws Exception {
        // Make a copy to avoid concurrent modification exceptions
        List<AutoCloseable> closeables = snapshot();

        // Resources are expected to remove themselves from 'resources' in their close()
        Exception firstException = null;
        for (AutoCloseable closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception e) {
                if (firstException == null) {
                    firstException = e;
                } else {
                    firstException.addSuppressed(e);
                }
            }
        }
        if (firstException != null) {
            throw firstException;
        }
    }
}
