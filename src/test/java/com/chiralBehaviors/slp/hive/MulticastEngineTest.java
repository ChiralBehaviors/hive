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
package com.chiralBehaviors.slp.hive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.verify;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.crypto.Mac;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.fasterxml.uuid.NoArgGenerator;
import com.hellblazer.utils.fd.FailureDetectorFactory;

/**
 * @author hhildebrand
 * 
 */
public class MulticastEngineTest {
    @Mock
    private DatagramSocket           p2p;
    @Mock
    private DatagramSocket           multicast;
    @Mock
    private FailureDetectorFactory   fdFactory;
    @Mock
    private NoArgGenerator           idGenerator;
    private Mac                      mac          = Common.defaultMac();
    @Mock
    private ScheduledExecutorService executor;
    @Mock
    private EngineListener           listener;

    private InetSocketAddress        groupAddress = new InetSocketAddress(0);
    private MulticastEngine                   engine;

    @Before
    public void setup() throws SocketException {
        MockitoAnnotations.initMocks(this);
        engine = new MulticastEngine(executor, fdFactory, idGenerator, 0,
                            TimeUnit.SECONDS, multicast, groupAddress, 1, 1,
                            mac, p2p);
        engine.setListener(listener);
    }

    @Test
    public void testSendDigests() throws Exception {
        Runnable heartbeatTask = engine.heartbeatTask();
        ArgumentCaptor<DatagramPacket> captor = ArgumentCaptor.forClass(DatagramPacket.class);
        heartbeatTask.run();
        verify(p2p).send(captor.capture());
        DatagramPacket packet = captor.getValue();
        assertNotNull(packet);
        assertEquals(groupAddress, packet.getSocketAddress());

    }

    @Test
    public void testUpdate() throws Exception {
        InetSocketAddress endpoint = new InetSocketAddress("localhost", 666);
        ByteBuffer buffer = ByteBuffer.allocate(1500);
        byte[] byteState = new byte[] { 0, 1, 2, 3 };
        UUID stateId = UUID.randomUUID();
        ReplicatedState state = new ReplicatedState(stateId, 1, byteState);
        state.writeTo(buffer);
        buffer.flip();
        engine.handleUpdate(endpoint, buffer);
        verify(listener).register(stateId, byteState);

        state = new ReplicatedState(stateId, 2, byteState);
        buffer.rewind();
        state.writeTo(buffer);
        buffer.flip();
        engine.handleUpdate(endpoint, buffer);
        verify(listener).update(stateId, byteState);
    }

    @Test
    public void testRequestState() throws Exception {
        InetSocketAddress endpoint = new InetSocketAddress("localhost", 666);
        ByteBuffer buffer = ByteBuffer.allocate(1500);
        UUID stateId = UUID.randomUUID();
        buffer.put((byte) 1);
        buffer.putLong(stateId.getMostSignificantBits());
        buffer.putLong(stateId.getLeastSignificantBits());
        buffer.putLong(1);
        buffer.flip();
        engine.handleDigests(endpoint, buffer);
        ArgumentCaptor<DatagramPacket> captor = ArgumentCaptor.forClass(DatagramPacket.class);
        verify(p2p).send(captor.capture());
        DatagramPacket packet = captor.getValue();
        assertNotNull(packet);
        assertEquals(endpoint, packet.getSocketAddress());
        assertEquals(38, packet.getLength());
        buffer = ByteBuffer.wrap(packet.getData());
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.limit(packet.getLength());
        assertEquals(Messages.MAGIC, buffer.getInt());
        assertEquals(Messages.STATE_REQUEST, buffer.get());
        assertEquals(1, buffer.get());
        UUID requestedId = new UUID(buffer.getLong(), buffer.getLong());
        assertEquals(stateId, requestedId);
        assertEquals(Common.defaultMac().getMacLength(), buffer.remaining());
    }
}
