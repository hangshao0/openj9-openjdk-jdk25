/*
 * Copyright (c) 2023, 2025, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

/*
 * @test id=default
 * @summary Test virtual thread with monitor enter/exit
 * @modules java.base/com.ibm.oti.vm java.base/java.lang:+open jdk.management
 * @library /test/lib
 * @build LockingMode
 * @run junit/othervm/native --enable-native-access=ALL-UNNAMED MonitorEnterExit
 */

/*
 * @test id=LM_LEGACY
 * @modules java.base/com.ibm.oti.vm java.base/java.lang:+open jdk.management
 * @library /test/lib
 * @build LockingMode
 * @run junit/othervm/native -XX:LockingMode=1 --enable-native-access=ALL-UNNAMED MonitorEnterExit
 */

/*
 * @test id=LM_LIGHTWEIGHT
 * @modules java.base/com.ibm.oti.vm java.base/java.lang:+open jdk.management
 * @library /test/lib
 * @build LockingMode
 * @run junit/othervm/native -XX:LockingMode=2 --enable-native-access=ALL-UNNAMED MonitorEnterExit
 */

/*
 * @test id=Xint-LM_LEGACY
 * @modules java.base/com.ibm.oti.vm java.base/java.lang:+open jdk.management
 * @library /test/lib
 * @build LockingMode
 * @run junit/othervm/native -Xint -XX:LockingMode=1 --enable-native-access=ALL-UNNAMED MonitorEnterExit
 */

/*
 * @test id=Xint-LM_LIGHTWEIGHT
 * @modules java.base/com.ibm.oti.vm java.base/java.lang:+open jdk.management
 * @library /test/lib
 * @build LockingMode
 * @run junit/othervm/native -Xint -XX:LockingMode=2 --enable-native-access=ALL-UNNAMED MonitorEnterExit
 */

/*
 * @test id=Xcomp-LM_LEGACY
 * @modules java.base/com.ibm.oti.vm java.base/java.lang:+open jdk.management
 * @library /test/lib
 * @build LockingMode
 * @run junit/othervm/native -Xcomp -XX:LockingMode=1 --enable-native-access=ALL-UNNAMED MonitorEnterExit
 */

/*
 * @test id=Xcomp-LM_LIGHTWEIGHT
 * @modules java.base/com.ibm.oti.vm java.base/java.lang:+open jdk.management
 * @library /test/lib
 * @build LockingMode
 * @run junit/othervm/native -Xcomp -XX:LockingMode=2 --enable-native-access=ALL-UNNAMED MonitorEnterExit
 */

/*
 * @test id=Xcomp-TieredStopAtLevel1-LM_LEGACY
 * @modules java.base/com.ibm.oti.vm java.base/java.lang:+open jdk.management
 * @library /test/lib
 * @build LockingMode
 * @run junit/othervm/native -Xcomp -XX:TieredStopAtLevel=1 -XX:LockingMode=1 --enable-native-access=ALL-UNNAMED MonitorEnterExit
 */

/*
 * @test id=Xcomp-TieredStopAtLevel1-LM_LIGHTWEIGHT
 * @modules java.base/com.ibm.oti.vm java.base/java.lang:+open jdk.management
 * @library /test/lib
 * @build LockingMode
 * @run junit/othervm/native -Xcomp -XX:TieredStopAtLevel=1 -XX:LockingMode=2 --enable-native-access=ALL-UNNAMED MonitorEnterExit
 */

/*
 * @test id=Xcomp-noTieredCompilation-LM_LEGACY
 * @modules java.base/com.ibm.oti.vm java.base/java.lang:+open jdk.management
 * @library /test/lib
 * @build LockingMode
 * @run junit/othervm/native -Xcomp -XX:-TieredCompilation -XX:LockingMode=1 --enable-native-access=ALL-UNNAMED MonitorEnterExit
 */

/*
 * @test id=Xcomp-noTieredCompilation-LM_LIGHTWEIGHT
 * @modules java.base/com.ibm.oti.vm java.base/java.lang:+open jdk.management
 * @library /test/lib
 * @build LockingMode
 * @run junit/othervm/native -Xcomp -XX:-TieredCompilation -XX:LockingMode=2 --enable-native-access=ALL-UNNAMED MonitorEnterExit
 */

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import jdk.test.lib.thread.VThreadPinner;
import jdk.test.lib.thread.VThreadRunner;   // ensureParallelism requires jdk.management
import jdk.test.lib.thread.VThreadScheduler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.MethodSource;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.*;

class MonitorEnterExit {
    static final int MAX_VTHREAD_COUNT = 4 * Runtime.getRuntime().availableProcessors();
    static final int MAX_ENTER_DEPTH = 64;

    @BeforeAll
    static void setup() {
        // need >=2 carriers for tests that pin
        VThreadRunner.ensureParallelism(2);
    }

    /**
     * Test monitor enter with contention, monitor will be held by caller thread.
     */
    private void testEnterWithContention() throws Exception {
        var lock = new Object();
        var started = new CountDownLatch(1);
        var entered = new AtomicBoolean();
        var vthread = Thread.ofVirtual().unstarted(() -> {
            started.countDown();
            synchronized (lock) {
                assertTrue(Thread.holdsLock(lock));
                entered.set(true);
            }
            assertFalse(Thread.holdsLock(lock));
        });
        try {
            synchronized (lock) {
                vthread.start();

                // wait for thread to start and block
                started.await();
                await(vthread, Thread.State.BLOCKED);

                assertFalse(entered.get());
            }
        } finally {
            vthread.join();
        }
        assertTrue(entered.get());
    }

    /**
     * Test monitor reenter.
     */
    @Test
    void testReenter() throws Exception {
        var lock = new Object();
        VThreadRunner.run(() -> {
            testReenter(lock, 0);
            assertFalse(Thread.holdsLock(lock));
        });
    }

    private void testReenter(Object lock, int depth) {
        if (depth < MAX_ENTER_DEPTH) {
            synchronized (lock) {
                assertTrue(Thread.holdsLock(lock));
                testReenter(lock, depth + 1);
                assertTrue(Thread.holdsLock(lock));
            }
        }
    }

    /**
     * Test monitor reenter when there are other threads blocked trying to enter.
     */
    @Test
    @DisabledIf("LockingMode#isLegacy")
    void testReenterWithContention() throws Exception {
        var lock = new Object();
        VThreadRunner.run(() -> {
            List<Thread> threads = new ArrayList<>();
            testReenter(lock, 0, threads);

            // wait for threads to terminate
            for (Thread vthread : threads) {
                vthread.join();
            }
        });
    }

    private void testReenter(Object lock, int depth, List<Thread> threads) throws Exception {
        if (depth < MAX_ENTER_DEPTH) {
            synchronized (lock) {
                assertTrue(Thread.holdsLock(lock));

                // start platform or virtual thread that blocks waiting to enter
                var started = new CountDownLatch(1);
                ThreadFactory factory = ThreadLocalRandom.current().nextBoolean()
                        ? Thread.ofPlatform().factory()
                        : Thread.ofVirtual().factory();
                var thread = factory.newThread(() -> {
                    started.countDown();
                    synchronized (lock) {
                        /* do nothing */
                    }
                });
                thread.start();

                // wait for thread to start and block
                started.await();
                await(thread, Thread.State.BLOCKED);
                threads.add(thread);

                // test reenter
                testReenter(lock, depth + 1, threads);
            }
        }
    }


    /**
     * Returns a stream of elements that are ordered pairs of platform and virtual thread
     * counts. 0,2,4,..16 platform threads. 2,4,6,..32 virtual threads.
     */
    static Stream<Arguments> threadCounts() {
        return IntStream.range(0, 17)
                .filter(i -> i % 2 == 0)
                .mapToObj(i -> i)
                .flatMap(np -> IntStream.range(2, 33)
                        .filter(i -> i % 2 == 0)
                        .mapToObj(vp -> Arguments.of(np, vp)));
    }


    
    /**
     * Waits for the given thread to reach a given state.
     */
    private void await(Thread thread, Thread.State expectedState) throws InterruptedException {
        int sleepCount = 0;
    	final int PRINT_EVERY = 1000; 
    	Thread.State state = thread.getState();
        while (state != expectedState) {
            assertTrue(state != Thread.State.TERMINATED, "Thread has terminated");
 			sleepCount++;
        	if (sleepCount % PRINT_EVERY == 0) {
            	System.out.println("Current thread name: " + Thread.currentThread().getName()
                    + " Current thread: " + Thread.currentThread()
                    + ", Target thread name: " + thread.getName()
                    + " thread: " + thread
                    + ", state: " + state
                    + ", after " + sleepCount + " sleeps");
        	}
            Thread.sleep(10);
            state = thread.getState();
        }
    }
}
