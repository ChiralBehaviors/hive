/** 
 * (C) Copyright 2010 Hal Hildebrand, All Rights Reserved
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.chiralBehaviors.slp.hive.configuration.BroadcastConfiguration;
import com.chiralBehaviors.slp.hive.configuration.EngineConfiguration;
import com.chiralBehaviors.slp.hive.configuration.MulticastConfiguration;
import com.hellblazer.slp.ServiceEvent;
import com.hellblazer.slp.ServiceEvent.EventType;
import com.hellblazer.slp.ServiceListener;
import com.hellblazer.slp.ServiceScope;
import com.hellblazer.slp.ServiceURL;

/**
 * @author hhildebrand
 * 
 */
public class FunctionalTest {

    private static class Listener implements ServiceListener {
        final Map<EventType, List<ServiceURL>> events = new HashMap<ServiceEvent.EventType, List<ServiceURL>>();

        final CountDownLatch                   modified;

        final CountDownLatch                   registered;
        final CountDownLatch                   unregistered;

        /**
         * @param registered
         * @param modified
         * @param unregistered
         */
        public Listener(CountDownLatch registered, CountDownLatch modified,
                        CountDownLatch unregistered) {
            this.registered = registered;
            this.modified = modified;
            this.unregistered = unregistered;
            events.put(EventType.REGISTERED, new ArrayList<ServiceURL>());
            events.put(EventType.MODIFIED, new ArrayList<ServiceURL>());
            events.put(EventType.UNREGISTERED, new ArrayList<ServiceURL>());
        }

        @Override
        public void serviceChanged(ServiceEvent event) {
            events.get(event.getType()).add(event.getReference().getUrl());
            switch (event.getType()) {
                case REGISTERED: {
                    registered.countDown();
                    break;
                }
                case MODIFIED: {
                    modified.countDown();
                    break;
                }
                case UNREGISTERED: {
                    unregistered.countDown();
                    break;
                }
            }
        }
    }

    @Test
    public void testBroadcast() throws Exception {
        functionalTest(true);
    }

    @Test
    public void testMulticast() throws Exception {
        functionalTest(false);
    }

    private Engine createCommunications(boolean broadcast) throws IOException {
        EngineConfiguration config = broadcast ? new BroadcastConfiguration()
                                              : MulticastConfiguration.fromYaml(getClass().getResourceAsStream("/multicast.yml"));
        return config.construct();
    }

    private List<Engine> createEngines(int membership, boolean broadcast)
                                                                         throws IOException {
        List<Engine> members = new ArrayList<Engine>();
        for (int i = 0; i < membership; i++) {
            members.add(createCommunications(broadcast));
        }
        return members;
    }

    private void functionalTest(boolean broadcast) throws Exception {
        int members = 16;
        final CountDownLatch registered = new CountDownLatch(members);
        final CountDownLatch modified = new CountDownLatch(members);
        final CountDownLatch unregistered = new CountDownLatch(members);
        List<HiveScope> scopes = new ArrayList<HiveScope>();
        List<Listener> listeners = new ArrayList<FunctionalTest.Listener>();
        for (Engine engine : createEngines(members, broadcast)) {
            HiveScope scope = new HiveScope(engine);
            Listener listener = new Listener(registered, modified, unregistered);
            scope.addServiceListener(listener,
                                     String.format("(%s=*)",
                                                   ServiceScope.SERVICE_TYPE));
            scopes.add(scope);
            listeners.add(listener);
            scope.start();
        }

        HiveScope scope = scopes.get(0);
        ServiceURL url = new ServiceURL(
                                        "service:jmx:http://foo.bar.baz.bozo.com/some/resource/ish/thing");
        UUID registration = scope.register(url, new HashMap<String, String>());

        System.out.println("Waiting for registrations");
        assertTrue("did not receive all registrations",
                   registered.await(120, TimeUnit.SECONDS));
        for (Listener listener : listeners) {
            assertEquals("Received more than one registration", 1,
                         listener.events.get(EventType.REGISTERED).size());
        }
        System.out.println("All registrations received");

        Map<String, String> properties = new HashMap<String, String>();
        properties.put("update.group", "1");
        properties.put("threads", "2");
        scopes.get(0).setProperties(registration, properties);

        System.out.println("Waiting for modfications");
        assertTrue("did not receive all modifications",
                   modified.await(30, TimeUnit.SECONDS));
        for (Listener listener : listeners) {
            assertEquals("Received more than one modification", 1,
                         listener.events.get(EventType.MODIFIED).size());
        }
        System.out.println("All modifications received");

        scopes.get(0).unregister(registration);
        System.out.println("Waiting for unregistrations");
        assertTrue("did not receive all unregistrations",
                   unregistered.await(30, TimeUnit.SECONDS));
        for (Listener listener : listeners) {
            assertEquals("Received more than one unregistration", 1,
                         listener.events.get(EventType.UNREGISTERED).size());
        }
        System.out.println("All unregistrations received");

        for (ServiceScope s : scopes) {
            s.stop();
        }
    }

}
