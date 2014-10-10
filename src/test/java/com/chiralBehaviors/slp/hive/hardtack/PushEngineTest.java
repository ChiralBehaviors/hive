/*
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
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.crypto.Mac;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.chiralBehaviors.slp.hive.Common;
import com.chiralBehaviors.slp.hive.EngineListener;
import com.chiralBehaviors.slp.hive.ReplicatedState;
import com.fasterxml.uuid.NoArgGenerator;
import com.hellblazer.utils.fd.FailureDetectorFactory;

/**
 * @author hhildebrand
 * 
 */
public class PushEngineTest {
    @Mock
    private DatagramSocket           p2p;
    @Mock
    private FailureDetectorFactory   fdFactory;
    @Mock
    private NoArgGenerator           idGenerator;
    private Mac                      mac        = Common.defaultMac();
    @Mock
    private ScheduledExecutorService executor;
    @Mock
    private EngineListener           listener;

    private List<InetSocketAddress>  aggregator = Arrays.asList(new InetSocketAddress(
                                                                                      0));
    private PushEngine               engine;

    @Before
    public void setup() throws SocketException {
        MockitoAnnotations.initMocks(this);
        engine = new PushEngine(p2p, mac, idGenerator, aggregator, 1,
                                TimeUnit.SECONDS, executor);
        engine.setListener(listener);
        when(idGenerator.generate()).thenReturn(new UUID(0, 666));
    }

    @Test
    public void testHeartbeat() throws Exception {
        Runnable heartbeatTask = engine.heartbeatTask();
        ArgumentCaptor<DatagramPacket> captor = ArgumentCaptor.forClass(DatagramPacket.class);
        heartbeatTask.run();
        verify(p2p).send(captor.capture());
        DatagramPacket packet = captor.getValue();
        assertNotNull(packet);
        assertEquals(aggregator.get(0), packet.getSocketAddress());

    }

    @Test
    public void testRegister() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(1500);
        byte[] byteState = new byte[] { 0, 1, 2, 3 };
        UUID stateId = UUID.randomUUID();
        ReplicatedState state = new ReplicatedState(stateId, 1, byteState);
        state.writeTo(buffer);
        buffer.flip();
        assertEquals(new UUID(0, 666), engine.register(byteState));
        ArgumentCaptor<DatagramPacket> captor = ArgumentCaptor.forClass(DatagramPacket.class);
        verify(p2p).send(captor.capture());
        DatagramPacket packet = captor.getValue();
        assertNotNull(packet);
        assertEquals(aggregator.get(0), packet.getSocketAddress());
    }
}
