/** (C) Copyright 2010 Hal Hildebrand, All Rights Reserved
 * 
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
package com.chiralBehaviors.slp.hive;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.chiralBehaviors.slp.hive.configuration.BroadcastConfiguration;
import com.chiralBehaviors.slp.hive.configuration.EngineConfiguration;
import com.chiralBehaviors.slp.hive.configuration.MulticastConfiguration;

/**
 * Basic end to end testing
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
public class EndToEndTest {
    private class Receiver implements EngineListener {

        private final CountDownLatch[] latches;

        Receiver(int members, int id) {
            latches = new CountDownLatch[members];
            setLatches(id);
        }

        public boolean await(int timeout, TimeUnit unit)
                                                        throws InterruptedException {
            for (CountDownLatch latche : latches) {
                if (!latche.await(timeout, unit)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void deregister(UUID id) {
            System.err.println(String.format("Sould never have abandoned state %s",
                                             id));
            deregistered.set(true);
        }

        @Override
        public void register(UUID id, byte[] state) {
            int currentCount = count.incrementAndGet();
            if (currentCount % 10 == 0) {
                System.out.print('.');
            } else if (currentCount % 100 == 0) {
                System.out.println();
            }

            ByteBuffer buffer = ByteBuffer.wrap(state);
            latches[buffer.getInt()].countDown();
        }

        @Override
        public void update(UUID id, byte[] state) {
            assert state != null;
            // System.out.println("Heartbeat received: " + hb);
            int currentCount = count.incrementAndGet();
            if (currentCount % 10 == 0) {
                System.out.print('.');
            } else if (currentCount % 100 == 0) {
                System.out.println();
            }

            ByteBuffer buffer = ByteBuffer.wrap(state);
            latches[buffer.getInt()].countDown();
        }

        void setLatches(int id) {
            for (int i = 0; i < latches.length; i++) {
                int count = i == id ? 0 : 1;
                latches[i] = new CountDownLatch(count);
            }
        }
    }

    private static final AtomicInteger count        = new AtomicInteger();
    private static final AtomicBoolean deregistered = new AtomicBoolean(false);
    private List<Engine>               members;
    private UUID[]                     stateIds;

    @Test
    public void testBroadcast() throws Exception {
        test(true);
    }

    @Test
    public void testMulticast() throws Exception {
        test(false);
    }

    private Engine createDefaultCommunications(EngineListener receiver,
                                               boolean broadcast)
                                                                 throws IOException {
        EngineConfiguration config = broadcast ? new BroadcastConfiguration()
                                              : MulticastConfiguration.fromYaml(getClass().getResourceAsStream("/multicast.yml"));
        Engine engine = config.construct();
        engine.setListener(receiver);
        engine.start();
        return engine;
    }

    private void test(boolean broadcast) throws Exception {
        int membership = 16;
        stateIds = new UUID[membership];

        Receiver[] receivers = new Receiver[membership];
        for (int i = 0; i < membership; i++) {
            receivers[i] = new Receiver(membership, i);
        }
        members = new ArrayList<Engine>();
        for (int i = 0; i < membership; i++) {
            members.add(createDefaultCommunications(receivers[i], broadcast));
        }
        try {
            int id = 0;
            for (Engine member : members) {
                byte[] state = new byte[4];
                ByteBuffer buffer = ByteBuffer.wrap(state);
                buffer.putInt(id);
                member.start();
                stateIds[id] = member.register(state);
                id++;
            }
            for (int i = 0; i < membership; i++) {
                assertTrue(String.format("initial iteration did not receive all notifications for %s",
                                         members.get(i)),
                           receivers[i].await(30, TimeUnit.SECONDS));
            }
            System.out.println();
            System.out.println("Initial iteration completed");
            for (int i = 1; i < 5; i++) {
                updateAndAwait(i, membership, receivers, members);
                System.out.println();
                System.out.println("Iteration " + (i + 1) + " completed");
            }
        } finally {
            System.out.println();
            for (Engine member : members) {
                member.stop();
            }
        }
        assertFalse("state was deregistered", deregistered.get());
    }

    private void updateAndAwait(int iteration, int membership,
                                Receiver[] receivers, List<Engine> members2)
                                                                            throws InterruptedException {
        int id = 0;
        for (Receiver receiver : receivers) {
            receiver.setLatches(id++);
        }
        id = 0;
        for (Engine member : members2) {
            ByteBuffer state = ByteBuffer.wrap(new byte[4]);
            state.putInt(id);
            member.update(stateIds[id], state.array());
            id++;
        }
        for (int i = 0; i < membership; i++) {
            assertTrue(String.format("Iteration %s did not receive all notifications for %s",
                                     i, members2.get(i)),
                       receivers[i].await(20, TimeUnit.SECONDS));
        }
    }
}
