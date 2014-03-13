/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package at.salzburgresearch.kmt.zkconfig;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * A simple boolean barrier, can be lowered ({@link #await()} will return) or raised ({@link #await()} will block).
 */
public class ConnectionBarrier {

    private final Sync sync;

    public ConnectionBarrier() {
        sync = new Sync(1);
    }

    private static final class Sync extends AbstractQueuedSynchronizer {

        public Sync(int initState) {
            this.setState(initState);
        }

        protected boolean tryBlockShared() {
            this.setState(1);
            return true;
        }

        @Override
        protected boolean tryReleaseShared(int arg) {
            this.setState(0);
            return true;
        }

        @Override
        protected int tryAcquireShared(int arg) {
            return (getState()==0)?1:-1;
        }
    }

    /**
     * Raise the barrier, from now on {@link #await()} will block until the barrier is lowered.
     *
     * @see #lowerBarrier()
     */
    public void raiseBarrier() {
        // Block/reset
        sync.tryBlockShared();
    }

    /**
     * Raise the barrier, from now on {@link #await()} will return until the barrier is raised again.
     *
     * @see #raiseBarrier()
     */
    public void lowerBarrier() {
        // Free
        sync.releaseShared(1);
    }

    /**
     * Causes the current thread to wait until the barrier is down,
     * unless the thread is {@linkplain Thread#interrupt interrupted}.
     *
     * <p>If the barrier is down then this method returns immediately.
     *
     * @throws InterruptedException if the current thread is interrupted
     *         while waiting
     */
    public void await() throws InterruptedException {
        // wait for barrier to go down.
        sync.acquireSharedInterruptibly(1);
    }
    /**
     * Causes the current thread to wait until the barrier is down,
     * unless the thread is {@linkplain Thread#interrupt interrupted},
     * or the specified waiting time elapses.
     *
     * <p>If the barrier is down then this method returns immediately
     * with the value {@code true}.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit of the {@code timeout} argument
     * @return {@code true} if the barrier was lowered and {@code false}
     *         if the waiting time elapsed before the barrier was lowered
     * @throws InterruptedException if the current thread is interrupted
     *         while waiting
     */
    public boolean await(long timeout, TimeUnit unit)
            throws InterruptedException {
        return sync.tryAcquireSharedNanos(1, unit.toNanos(timeout));
    }

}
