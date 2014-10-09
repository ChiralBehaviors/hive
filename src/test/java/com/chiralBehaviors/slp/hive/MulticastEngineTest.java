/**
 * Copyright (c) 2012, salesforce.com, inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided
 * that the following conditions are met:
 *
 *    Redistributions of source code must retain the above copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 *    Redistributions in binary form must reproduce the above copyright notice, this list of conditions and
 *    the following disclaimer in the documentation and/or other materials provided with the distribution.
 *
 *    Neither the name of salesforce.com, inc. nor the names of its contributors may be used to endorse or
 *    promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
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
