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

import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import world.data.jdbc.testing.CloserResource;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

public class FileBackedInputStreamTest {

    @Rule
    public final CloserResource closer = new CloserResource();

    @Test
    public void testMem() throws Exception {
        byte[] content = genSampleBytes(256);
        Executor executor = mock(Executor.class);
        InputStream contentIn = spy(new ByteArrayInputStream(content));
        try (FileBackedInputStream in = new FileBackedInputStream(contentIn, 1024, executor)) {
            int pos = 0;
            assertThat(in.read()).isEqualTo(content[pos++]);
            assertThat(in.read()).isEqualTo(content[pos++]);
            assertThat(in.read()).isEqualTo(content[pos++]);

            byte[] buf1 = new byte[16];
            assertThat(in.read(buf1)).isEqualTo(buf1.length);
            assertThat(buf1).isEqualTo(Arrays.copyOfRange(content, pos, pos + buf1.length));
            pos += buf1.length;

            byte[] buf2 = new byte[4096];
            assertThat(in.read(buf2)).isEqualTo(content.length - pos);
            assertThat(buf2).startsWith(Arrays.copyOfRange(content, pos, content.length));

            assertThat(in.read()).isEqualTo(-1);
            assertThat(in.read(buf1)).isEqualTo(-1);
        }
        verifyZeroInteractions(executor);
        verify(contentIn).close();
    }

    @Test
    public void testFile() throws Exception {
        byte[] content = genSampleBytes(256_000);
        InputStream contentIn = spy(new ByteArrayInputStream(content));

        // Setup an executor that waits a bit before starting the async copy
        Semaphore semaphore = new Semaphore(0);
        Executor realExecutor = newCachedExecutor();
        Executor delayExecutor = mock(Executor.class);
        doAnswer((InvocationOnMock iom) -> delayedExecute(realExecutor, iom.getArgument(0), semaphore))
                .when(delayExecutor).execute(any());

        try (FileBackedInputStream in = new FileBackedInputStream(contentIn, 2, delayExecutor)) {
            int pos = 0;

            // Read the first 2 bytes in memory
            assertThat(in.read()).isEqualTo(content[pos++]);
            assertThat(in.read()).isEqualTo(content[pos++]);

            // Read the next byte from the temp file
            semaphore.release();
            assertThat(in.read()).isEqualTo(content[pos++]);
            verify(delayExecutor).execute(any());

            byte[] buf = new byte[4096];
            int count;
            while ((count = in.read(buf)) != -1) {
                assertThat(buf).startsWith(Arrays.copyOfRange(content, pos, pos + count));
                pos += count;
            }

            assertThat(pos).isEqualTo(content.length);
            assertThat(in.read()).isEqualTo(-1);
            assertThat(in.read(buf)).isEqualTo(-1);
        }
        verify(contentIn).close();
    }

    @Test
    public void testEarlyClose() throws Exception {
        byte[] content = genSampleBytes(16_000);
        InputStream contentIn = spy(new ByteArrayInputStream(content));

        // Setup an executor that waits for FileBackedInputStream.close() before starting the async copy
        Semaphore semaphore = new Semaphore(0);
        Executor realExecutor = newCachedExecutor();
        Executor delayExecutor = mock(Executor.class);
        doAnswer((InvocationOnMock iom) -> delayedExecute(realExecutor, iom.getArgument(0), semaphore))
                .when(delayExecutor).execute(any());

        new FileBackedInputStream(contentIn, 0, delayExecutor).close();
        verifyZeroInteractions(contentIn);

        // Except for closing the original InputStream, the async copy should no-op because FileBackedInputStream is closed
        semaphore.release();
        verify(contentIn, timeout(1000)).close();
        verifyNoMoreInteractions(contentIn);
    }

    @Test
    public void testDownloadException() throws Exception {
        byte[] content = genSampleBytes(8000);
        // Simulate the socket being closed before all expected bytes were downloaded.
        InputStream contentIn = new FilterInputStream(new ByteArrayInputStream(content)) {
            @Override
            public int read(@Nonnull byte[] b, int off, int len) throws IOException {
                int count = super.read(b, off, len);
                if (count == -1) {
                    throw new EOFException("Unexpected end of input");
                }
                return count;
            }
        };
        try (InputStream in = new FileBackedInputStream(contentIn, 0, newCachedExecutor())) {
            //noinspection ResultOfMethodCallIgnored
            assertThatThrownBy(() -> in.skip(10000))
                    .isExactlyInstanceOf(IOException.class)
                    .hasMessage("Unexpected end of input")
                    .hasCauseExactlyInstanceOf(EOFException.class);
        }
    }

    private byte[] genSampleBytes(int length) {
        byte[] buf = new byte[length];
        for (int i = 0; i < buf.length; i++) {
            buf[i] = (byte) i;
        }
        return buf;
    }

    private ExecutorService newCachedExecutor() {
        return closer.register(Executors.newCachedThreadPool(), ExecutorService::shutdown);
    }

    private Void delayedExecute(Executor executor, Runnable task, Semaphore semaphore) {
        executor.execute(() -> {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            task.run();
        });
        return null;
    }
}
