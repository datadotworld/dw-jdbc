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
package world.data.jdbc.internal.transport;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.AbstractQueuedLongSynchronizer;

import static java.util.Objects.requireNonNull;

/**
 * An {@link InputStream} that protects backend http servers from slow readers.
 * <ol>
 * <li>The first 'n' bytes are read into memory immediately to handle small responses.</li>
 * <li>For bigger responses, a background thread starts downloading the rest of the response to a temporary file.</li>
 * <li>Content downloaded so far can be read via the {@code FileBackedInputStream}.  Readers don't have to wait
 * for the entire download to complete before they can make progress--a semaphore is used to ensure readers don't
 * get ahead of the download thread.</li>
 * </ol>
 */
class FileBackedInputStream extends InputStream {
    private InputStream memIn;
    private final Sync sync;
    private File file;
    private final InputStream fileIn;
    /** Flag used by the main thread to tell the copyAsync thread that the main thread is done. */
    private volatile boolean fileInClosed;
    /** Flag used by the copyAsync thread to tell the main thread that copyAsync terminated abnormally. */
    private volatile Throwable throwable;

    FileBackedInputStream(InputStream in, int memLimit, Executor cachedThreadPool) throws IOException {
        requireNonNull(in, "in");
        requireNonNull(cachedThreadPool, "cachedThreadPool");

        // Read the first 'memLimit' bytes immediately.
        byte[] buf = new byte[memLimit];
        int remaining = memLimit, length = 0, count;
        while (remaining > 0 && (count = in.read(buf, length, remaining)) != -1) {
            length += count;
            remaining -= count;
        }
        this.memIn = new ByteArrayInputStream(buf, 0, length);

        if (length < memLimit) {
            // All content fits in memory
            in.close();
            this.sync = null;
            this.fileIn = null;
            this.fileInClosed = true;
        } else {
            // First 'memLimit' bytes are in memory. Asynchronously download the rest to a file as fast as possible.
            // The reader can still read from FileBackedInputStream while the download is in progress.
            this.sync = new Sync();
            this.file = File.createTempFile("dw-jdbc", ".tmp");
            this.fileIn = new FileInputStream(file);
            OutputStream fileOut = new FileOutputStream(file);
            cachedThreadPool.execute(() -> copyAsync(in, fileOut));
        }
    }

    private void copyAsync(InputStream source, OutputStream target) {
        try (InputStream in = source; OutputStream out = target) {
            byte[] buf = new byte[4096];
            int count;
            while (!fileInClosed && (count = in.read(buf)) != -1) {
                out.write(buf, 0, count);
                sync.releaseShared(count);
            }
        } catch (Throwable t) {
            throwable = t;
        } finally {
            sync.releaseShared(Long.MAX_VALUE);
            deleteTempFile();
        }
    }

    private void checkAsyncException() throws IOException {
        if (throwable != null) {
            throw new IOException(throwable.getMessage(), throwable);
        }
    }

    @Override
    public int read() throws IOException {
        if (memIn != null) {
            int b = memIn.read();
            if (b != -1) {
                return b;
            }
            memIn = null;
        }
        if (fileIn != null) {
            try {
                sync.acquireSharedInterruptibly(1);
                checkAsyncException();
                return fileIn.read();
            } catch (InterruptedException e) {
                throw new IOException("Interrupted while reading from file.", e);
            }
        }
        return -1;
    }

    @Override
    public int read(@Nonnull byte[] b, int off, int len) throws IOException {
        if (memIn != null) {
            int count = memIn.read(b, off, len);
            if (count != -1) {
                return count;
            }
            memIn = null;
        }
        if (fileIn != null) {
            try {
                sync.acquireSharedInterruptibly(len);
                checkAsyncException();
                return fileIn.read(b, off, len);
            } catch (InterruptedException e) {
                throw new IOException("Interrupted while reading from file.", e);
            }
        }
        return -1;
    }

    @Override
    public void close() throws IOException {
        if (!fileInClosed) {
            fileInClosed = true;
            fileIn.close();
            deleteTempFile();
        }
    }

    private synchronized void deleteTempFile() {
        // Note that Windows won't delete an open file so we must attempt cleanup from both threads to be
        // sure both input and output file handles are closed at the time of the delete.
        if (fileInClosed && file != null && file.delete()) {
            file = null;
        }
    }

    /** A 64-bit semaphore used to make sure the file reader doesn't get ahead of the writer. */
    @SuppressWarnings("serial")
    private static class Sync extends AbstractQueuedLongSynchronizer {
        @Override
        protected long tryAcquireShared(long acquires) {
            for (; ; ) {
                long current = getState();
                long next = current - acquires;
                if (next < 0 || compareAndSetState(current, next)) {
                    return next;
                }
            }
        }

        @Override
        protected boolean tryReleaseShared(long releases) {
            for (; ; ) {
                long current = getState();
                long next;
                if (releases == Long.MAX_VALUE) {
                    // Special case: allow sync.acquire() to obtain as much as it wants without blocking.
                    next = Long.MAX_VALUE;
                } else {
                    next = current + releases;
                    if (next < current) {
                        throw new Error("Maximum count exceeded"); // overflow
                    }
                }
                if (compareAndSetState(current, next)) {
                    return true;
                }
            }
        }
    }
}
