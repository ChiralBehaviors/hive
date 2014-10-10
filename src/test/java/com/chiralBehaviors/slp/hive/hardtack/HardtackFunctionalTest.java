/** 
 * (C) Copyright 2014 Chiral Behaviors, All Rights Reserved
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

package com.chiralBehaviors.slp.hive.hardtack;

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

import com.chiralBehaviors.slp.hive.Engine;
import com.chiralBehaviors.slp.hive.HiveScope;
import com.chiralBehaviors.slp.hive.hardtack.configuration.AggregatorConfiguration;
import com.chiralBehaviors.slp.hive.hardtack.configuration.PushConfiguration;
import com.hellblazer.slp.ServiceEvent;
import com.hellblazer.slp.ServiceEvent.EventType;
import com.hellblazer.slp.ServiceListener;
import com.hellblazer.slp.ServiceScope;
import com.hellblazer.slp.ServiceURL;

/**
 * @author hhildebrand
 * 
 */
public class HardtackFunctionalTest {

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
    public void testPush() throws Exception {
        int members = 16;
        final CountDownLatch registered = new CountDownLatch(members);
        final CountDownLatch modified = new CountDownLatch(members);
        final CountDownLatch unregistered = new CountDownLatch(members);
        List<HiveScope> scopes = new ArrayList<HiveScope>();
        for (Engine engine : createEngines(members)) {
            HiveScope scope = new HiveScope(engine);
            scopes.add(scope);
            scope.start();
        }

        HiveScope aggregator = new HiveScope(
                                             new AggregatorConfiguration().construct());
        Listener listener = new Listener(registered, modified, unregistered);
        aggregator.addServiceListener(listener,
                                      String.format("(%s=*)",
                                                    ServiceScope.SERVICE_TYPE));
        aggregator.start();
        ServiceURL url = new ServiceURL(
                                        "service:jmx:http://foo.bar.baz.bozo.com/some/resource/ish/thing");
        List<UUID> registrations = new ArrayList<>();
        for (ServiceScope scope : scopes) {
            registrations.add(scope.register(url, new HashMap<String, String>()));
        }
        System.out.println("Waiting for registrations");
        assertTrue("did not receive all registrations",
                   registered.await(120, TimeUnit.SECONDS));
        assertEquals("did not recieve all registrations", members,
                     listener.events.get(EventType.REGISTERED).size());
        System.out.println("All registrations received");

        int i = 0;
        for (ServiceScope scope : scopes) {
            Map<String, String> properties = new HashMap<String, String>();
            properties.put("update.group", "1");
            properties.put("threads", "2");
            scope.setProperties(registrations.get(i++), properties);
        }
        System.out.println("Waiting for modfications");
        assertTrue("did not receive all modifications",
                   modified.await(30, TimeUnit.SECONDS));
        assertEquals("did not receive all modifications", members,
                     listener.events.get(EventType.MODIFIED).size());
        System.out.println("All modifications received");

        i = 0;
        for (ServiceScope scope : scopes) {
            scope.unregister(registrations.get(i++));
        }

        System.out.println("Waiting for unregistrations");
        assertTrue("did not receive all unregistrations",
                   unregistered.await(30, TimeUnit.SECONDS));
        assertEquals("did not receive all registrations", members,
                     listener.events.get(EventType.UNREGISTERED).size());
        System.out.println("All unregistrations received");
        aggregator.stop();
        for (ServiceScope s : scopes) {
            s.stop();
        }
    }

    private List<Engine> createEngines(int membership) throws IOException {
        List<Engine> members = new ArrayList<Engine>();
        for (int i = 0; i < membership; i++) {
            members.add(new PushConfiguration().construct());
        }
        return members;
    }

}
