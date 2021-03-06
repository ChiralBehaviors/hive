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

package com.chiralBehaviors.slp.hive;

import static com.chiralBehaviors.slp.hive.Messages.BYTE_SIZE;
import static com.chiralBehaviors.slp.hive.Messages.DEREGISTER;
import static com.chiralBehaviors.slp.hive.Messages.DIGESTS;
import static com.chiralBehaviors.slp.hive.Messages.DIGEST_BYTE_SIZE;
import static com.chiralBehaviors.slp.hive.Messages.MAGIC;
import static com.chiralBehaviors.slp.hive.Messages.MAGIC_BYTE_SIZE;
import static com.chiralBehaviors.slp.hive.Messages.MAX_SEG_SIZE;
import static com.chiralBehaviors.slp.hive.Messages.MESSAGE_HEADER_BYTE_SIZE;
import static com.chiralBehaviors.slp.hive.Messages.STATE_REQUEST;
import static com.chiralBehaviors.slp.hive.Messages.UPDATE;
import static com.chiralBehaviors.slp.hive.Messages.UUID_BYTE_SIZE;
import static java.lang.Math.min;
import static java.lang.String.format;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.crypto.Mac;
import javax.crypto.ShortBufferException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.uuid.NoArgGenerator;
import com.hellblazer.utils.ByteBufferPool;
import com.hellblazer.utils.fd.FailureDetectorFactory;

/**
 * @author hhildebrand
 * 
 */
public class MulticastEngine implements Engine {
    private static final Logger                              log        = LoggerFactory.getLogger(MulticastEngine.class);

    private final ByteBufferPool                             bufferPool = new ByteBufferPool(
                                                                                             "Engine Comms",
                                                                                             100);
    private final ScheduledExecutorService                   executor;
    private final FailureDetectorFactory                     fdFactory;
    private final InetSocketAddress                          groupAddress;
    private final int                                        heartbeatPeriod;
    private ScheduledFuture<?>                               heartbeatTask;
    private final TimeUnit                                   heartbeatUnit;
    private final Mac                                        hmac;
    private final NoArgGenerator                             idGenerator;
    private EngineListener                                   listener;
    private final InetSocketAddress                          localAddress;
    private final ConcurrentMap<UUID, ReplicatedState>       localState = new ConcurrentHashMap<>();
    private final int                                        maxDigests;
    private final int                                        maxUuids;
    private final ConcurrentMap<InetSocketAddress, Endpoint> members    = new ConcurrentHashMap<>();
    private final DatagramSocket                             multicastSocket;

    private final DatagramSocket                             p2pSocket;

    private final AtomicBoolean                              running    = new AtomicBoolean();

    public MulticastEngine(FailureDetectorFactory fdFactory,
                           NoArgGenerator idGenerator, int heartbeatPeriod,
                           TimeUnit heartbeatUnit,
                           DatagramSocket multicastSocket,
                           InetSocketAddress groupAddress,
                           int receiveBufferMultiplier,
                           int sendBufferMultiplier, Mac mac,
                           NetworkInterface iface) throws SocketException {
        this(Executors.newScheduledThreadPool(2), fdFactory, idGenerator,
             heartbeatPeriod, heartbeatUnit, multicastSocket, groupAddress,
             receiveBufferMultiplier, sendBufferMultiplier, mac,
             Common.connect(iface, groupAddress));
    }

    public MulticastEngine(ScheduledExecutorService executor,
                           FailureDetectorFactory fdFactory,
                           NoArgGenerator idGenerator, int heartbeatPeriod,
                           TimeUnit heartbeatUnit,
                           DatagramSocket multicastSocket,
                           InetSocketAddress groupAddress,
                           int receiveBufferMultiplier,
                           int sendBufferMultiplier, Mac mac,
                           DatagramSocket p2pSocket) throws SocketException {
        this.executor = executor;
        this.fdFactory = fdFactory;
        this.idGenerator = idGenerator;
        this.heartbeatPeriod = heartbeatPeriod;
        this.heartbeatUnit = heartbeatUnit;
        this.multicastSocket = multicastSocket;
        this.groupAddress = groupAddress;
        try {
            multicastSocket.setReceiveBufferSize(MAX_SEG_SIZE
                                                 * receiveBufferMultiplier);
            multicastSocket.setSendBufferSize(MAX_SEG_SIZE
                                              * sendBufferMultiplier);
        } catch (SocketException e) {
            log.error(format("Unable to configure endpoint: %s",
                             multicastSocket));
            throw e;
        }
        hmac = mac;
        int payloadByteSize = MAX_SEG_SIZE - MESSAGE_HEADER_BYTE_SIZE
                              - mac.getMacLength();
        maxDigests = (payloadByteSize - BYTE_SIZE) // 1 byte for #digests
                     / DIGEST_BYTE_SIZE;
        maxUuids = (payloadByteSize - BYTE_SIZE) // 1 byte for #uuids
                   / UUID_BYTE_SIZE;
        this.p2pSocket = p2pSocket;
        localAddress = (InetSocketAddress) p2pSocket.getLocalSocketAddress();
    }

    /* (non-Javadoc)
     * @see com.chiralBehaviors.slp.hive.AbstractEngine#deregister(java.util.UUID)
     */
    @Override
    public void deregister(UUID id) {
        if (id == null) {
            throw new NullPointerException(
                                           "replicated state id must not be null");
        }
        localState.remove(id);
        if (log.isDebugEnabled()) {
            log.debug(String.format("Member: %s abandoning replicated state",
                                    getLocalAddress()));
        }
        ByteBuffer buffer = bufferPool.allocate(MAX_SEG_SIZE);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.position(MESSAGE_HEADER_BYTE_SIZE);
        try {
            buffer.putLong(id.getMostSignificantBits());
            buffer.putLong(id.getLeastSignificantBits());
            send(DEREGISTER, buffer, groupAddress);
        } finally {
            bufferPool.free(buffer);
        }
    }

    /* (non-Javadoc)
     * @see com.chiralBehaviors.slp.hive.AbstractEngine#getLocalAddress()
     */
    @Override
    public InetSocketAddress getLocalAddress() {
        return localAddress;
    }

    /* (non-Javadoc)
     * @see com.chiralBehaviors.slp.hive.AbstractEngine#getMaxStateSize()
     */
    @Override
    public int getMaxStateSize() {
        return MAX_SEG_SIZE - hmac.getMacLength() - MESSAGE_HEADER_BYTE_SIZE;
    }

    /* (non-Javadoc)
     * @see com.chiralBehaviors.slp.hive.AbstractEngine#register(byte[])
     */
    @Override
    public UUID register(byte[] replicatedState) {
        if (replicatedState == null) {
            throw new NullPointerException("replicated state must not be null");
        }
        if (replicatedState.length > getMaxStateSize()) {
            throw new IllegalArgumentException(
                                               String.format("State size %s must not be > %s",
                                                             replicatedState.length,
                                                             getMaxStateSize()));
        }
        UUID id = idGenerator.generate();
        ReplicatedState state = new ReplicatedState(id,
                                                    System.currentTimeMillis(),
                                                    replicatedState);
        localState.put(id, state);
        if (log.isDebugEnabled()) {
            log.debug(String.format("Member: %s registering replicated state: %s",
                                    getLocalAddress(), id));
        }
        ByteBuffer buffer = bufferPool.allocate(MAX_SEG_SIZE);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.position(MESSAGE_HEADER_BYTE_SIZE);
        try {
            state.writeTo(buffer);
            send(UPDATE, buffer, groupAddress);
        } finally {
            bufferPool.free(buffer);
        }
        return id;
    }

    /* (non-Javadoc)
     * @see com.chiralBehaviors.slp.hive.AbstractEngine#setListener(com.chiralBehaviors.slp.hive.EngineListener)
     */
    @Override
    public void setListener(EngineListener listener) {
        this.listener = listener;
    }

    /* (non-Javadoc)
     * @see com.chiralBehaviors.slp.hive.AbstractEngine#start()
     */
    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            Executors.newSingleThreadExecutor().execute(multicastServiceTask());
            Executors.newSingleThreadExecutor().execute(p2pServiceTask());
            heartbeatTask = executor.scheduleAtFixedRate(heartbeatTask(), 0,
                                                         heartbeatPeriod,
                                                         heartbeatUnit);
        }
    }

    /* (non-Javadoc)
     * @see com.chiralBehaviors.slp.hive.AbstractEngine#stop()
     */
    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            if (log.isInfoEnabled()) {
                log.info(String.format("Terminating UDP Communications on %s",
                                       localAddress));
            }
            heartbeatTask.cancel(true);
            multicastSocket.close();
            p2pSocket.close();
            log.info(bufferPool.toString());
        }
    }

    @Override
    public String toString() {
        return String.format("BroadcastComms[%s]", getLocalAddress());
    }

    /* (non-Javadoc)
     * @see com.chiralBehaviors.slp.hive.AbstractEngine#update(java.util.UUID, byte[])
     */
    @Override
    public void update(UUID id, byte[] replicatedState) {
        if (id == null) {
            throw new NullPointerException(
                                           "replicated state id must not be null");
        }
        if (replicatedState == null) {
            throw new NullPointerException("replicated state must not be null");
        }
        if (replicatedState.length > getMaxStateSize()) {
            throw new IllegalArgumentException(
                                               String.format("State size %s must not be > %s",
                                                             replicatedState.length,
                                                             getMaxStateSize()));
        }
        ReplicatedState state = new ReplicatedState(id,
                                                    System.currentTimeMillis(),
                                                    replicatedState);
        localState.put(id, state);
        if (log.isDebugEnabled()) {
            log.debug(String.format("Member: %s updating replicated state",
                                    getLocalAddress()));
        }
        ByteBuffer buffer = bufferPool.allocate(MAX_SEG_SIZE);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.position(MESSAGE_HEADER_BYTE_SIZE);
        try {
            state.writeTo(buffer);
            send(UPDATE, buffer, groupAddress);
        } finally {
            bufferPool.free(buffer);
        }
    }

    private synchronized void addMac(byte[] data, int offset, int length)
                                                                         throws ShortBufferException {
        hmac.reset();
        hmac.update(data, offset, length);
        hmac.doFinal(data, offset + length);
    }

    private synchronized boolean checkMac(byte[] data, int start, int length) {
        hmac.reset();
        hmac.update(data, start, length);
        byte[] checkMAC = hmac.doFinal();
        int len = checkMAC.length;
        assert len == hmac.getMacLength();

        for (int i = 0; i < len; i++) {
            if (checkMAC[i] != data[start + length + i]) {
                return false;
            }
        }
        return true;
    }

    private UUID[] extractIds(SocketAddress sender, ByteBuffer msg) {
        int count = msg.get();
        final UUID[] uuids = new UUID[count];
        for (int i = 0; i < count; i++) {
            UUID id;
            try {
                id = new UUID(msg.getLong(), msg.getLong());
            } catch (Throwable e) {
                if (log.isWarnEnabled()) {
                    log.warn(String.format("Cannot deserialize uuid. Ignoring the uuid: %s\n%s",
                                           i,
                                           Common.prettyPrint(sender,
                                                              getLocalAddress(),
                                                              msg.array(),
                                                              msg.limit())), e);
                }
                continue;
            }
            uuids[i] = id;
        }
        return uuids;
    }

    private void send(byte msgType, ByteBuffer buffer, InetSocketAddress target) {
        if (p2pSocket.isClosed()) {
            log.trace(String.format("Sending on a closed socket"));
            return;
        }
        int msgLength = buffer.position();
        int totalLength = msgLength + hmac.getMacLength();
        buffer.putInt(0, MAGIC);
        buffer.put(MAGIC_BYTE_SIZE, msgType);
        byte[] bytes = buffer.array();
        try {
            addMac(bytes, 0, msgLength);
        } catch (ShortBufferException e) {
            log.error("Invalid message (%s) %s",
                      type(msgType),
                      Common.prettyPrint(getLocalAddress(), target,
                                         buffer.array(), msgLength));
            return;
        } catch (SecurityException e) {
            log.error("No key provided for HMAC");
            return;
        }
        try {
            DatagramPacket packet = new DatagramPacket(bytes, totalLength,
                                                       target);
            if (log.isTraceEnabled()) {
                log.trace(String.format("sending %s packet mac start: %s %s",
                                        type(msgType), msgLength,
                                        Common.prettyPrint(getLocalAddress(),
                                                           target,
                                                           buffer.array(),
                                                           totalLength)));
            }
            p2pSocket.send(packet);
        } catch (SocketException e) {
            if (!"Socket is closed".equals(e.getMessage())
                && !"Bad file descriptor".equals(e.getMessage())) {
                if (log.isWarnEnabled()) {
                    log.warn("Error sending packet", e);
                }
            }
        } catch (IOException e) {
            if (log.isWarnEnabled()) {
                log.warn(String.format("Error sending packet to: %s", target),
                         e);
            }
        }
    }

    private void sendDigests(List<Digest> digests) {
        ByteBuffer buffer = bufferPool.allocate(MAX_SEG_SIZE);
        buffer.order(ByteOrder.BIG_ENDIAN);
        for (int i = 0; i < digests.size();) {
            byte count = (byte) min(maxDigests, digests.size() - i);
            buffer.position(MESSAGE_HEADER_BYTE_SIZE);
            buffer.put(count);
            for (Digest digest : digests.subList(i, i + count)) {
                digest.writeTo(buffer);
            }
            send(DIGESTS, buffer, groupAddress);
            i += count;
            buffer.clear();
        }
        bufferPool.free(buffer);
    }

    private void service(final DatagramSocket socket, final String tag)
                                                                       throws IOException {
        final ByteBuffer buffer = bufferPool.allocate(MAX_SEG_SIZE);
        buffer.order(ByteOrder.BIG_ENDIAN);
        final DatagramPacket packet = new DatagramPacket(buffer.array(),
                                                         buffer.array().length);
        if (log.isTraceEnabled()) {
            log.trace(String.format("listening for %s packet on %s", tag,
                                    socket.getLocalSocketAddress()));
        }
        socket.receive(packet);
        buffer.limit(packet.getLength());
        if (log.isTraceEnabled()) {
            log.trace(String.format("Received %s packet %s",
                                    tag,
                                    Common.prettyPrint(packet.getSocketAddress(),
                                                       getLocalAddress(),
                                                       buffer.array(),
                                                       packet.getLength())));
        }
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int magic = buffer.getInt();
                if (MAGIC == magic) {
                    try {
                        if (!checkMac(buffer.array(), 0, packet.getLength()
                                                         - hmac.getMacLength())) {
                            if (log.isWarnEnabled()) {
                                log.warn(format("Error processing inbound %s message on: %s, HMAC does not check",
                                                tag,
                                                socket.getLocalSocketAddress()));
                            }
                            return;
                        }
                    } catch (SecurityException e) {
                        if (log.isWarnEnabled()) {
                            log.warn(format("Error processing %s inbound message on: %s, HMAC does not check",
                                            tag, socket.getLocalSocketAddress()),
                                     e);
                        }
                        return;
                    }
                    buffer.limit(packet.getLength() - hmac.getMacLength());
                    try {
                        processInbound((InetSocketAddress) packet.getSocketAddress(),
                                       buffer);
                    } catch (Throwable e) {
                        if (log.isWarnEnabled()) {
                            log.warn(format("Error processing %s inbound message on: %s",
                                            tag, socket.getLocalSocketAddress()),
                                     e);
                        }
                    }
                } else {
                    if (log.isWarnEnabled()) {
                        log.warn(format("%s msg with invalid MAGIC header [%s] discarded %s",
                                        tag,
                                        magic,
                                        Common.prettyPrint(packet.getSocketAddress(),
                                                           getLocalAddress(),
                                                           buffer.array(),
                                                           packet.getLength())));
                    }
                }
                bufferPool.free(buffer);
            }
        });
    }

    private Runnable serviceTask(final DatagramSocket socket, final String tag) {
        return new Runnable() {
            @Override
            public void run() {
                if (log.isInfoEnabled()) {
                    log.info(String.format("UDP %s communications started on %s",
                                           tag, socket.getLocalSocketAddress()));
                }
                while (running.get()) {
                    try {
                        service(socket, tag);
                    } catch (SocketException e) {
                        if ("Socket closed".equals(e.getMessage())) {
                            if (log.isTraceEnabled()) {
                                log.trace("Socket closed, shutting down");
                                stop();
                                return;
                            }
                        }
                    } catch (Throwable e) {
                        if (log.isWarnEnabled()) {
                            log.warn("Exception processing inbound message", e);
                        }
                    }
                }
            }
        };
    }

    /**
     * @param msgType
     * @return
     */
    private String type(byte msgType) {
        switch (msgType) {
            case DIGESTS: {
                return "digests";
            }
            case UPDATE: {
                return "update";
            }
            case DEREGISTER: {
                return "deregister";
            }
            case STATE_REQUEST: {
                return "state request";
            }
            default: {
                if (log.isInfoEnabled()) {
                    log.info(format("invalid message type: %s", msgType));
                }
                throw new IllegalArgumentException(
                                                   String.format("Invalid message type %s",
                                                                 msgType));
            }
        }
    }

    Digest[] extractDigests(SocketAddress sender, ByteBuffer msg) {
        int count = msg.get();
        final Digest[] digests = new Digest[count];
        for (int i = 0; i < count; i++) {
            Digest digest;
            try {
                digest = new Digest(msg);
            } catch (Throwable e) {
                if (log.isWarnEnabled()) {
                    log.warn(String.format("Cannot deserialize digest. Ignoring the digest: %s\n%s",
                                           i,
                                           Common.prettyPrint(sender,
                                                              getLocalAddress(),
                                                              msg.array(),
                                                              msg.limit())), e);
                }
                continue;
            }
            digests[i] = digest;
        }
        return digests;
    }

    void handleDeregister(InetSocketAddress sender, ByteBuffer buffer) {
        if (sender.equals(localAddress)) {
            if (log.isTraceEnabled()) {
                log.trace(format("Ignoring deregister from self %s", sender));
            }
            return;
        }
        UUID stateId = new UUID(buffer.getLong(), buffer.getLong());
        Endpoint endpoint = members.get(sender);
        if (endpoint == null) {
            log.trace(String.format("Remove %s from unknown member %s",
                                    stateId, sender));
            return;
        }
        if (endpoint.remove(stateId) != null) {
            listener.deregister(stateId);
        }
    }

    void handleDigests(InetSocketAddress sender, ByteBuffer buffer)
                                                                   throws UnknownHostException {
        if (sender.equals(localAddress)) {
            if (log.isTraceEnabled()) {
                log.trace(String.format("ignoring digests received from self on %s",
                                        localAddress));
            }
            return;
        }
        Digest[] digests = extractDigests(sender, buffer);
        if (log.isTraceEnabled()) {
            log.trace(String.format("Digests from %s are %s", sender,
                                    Arrays.toString(digests)));
        }
        Endpoint endpoint = members.get(sender);
        if (endpoint == null) {
            Endpoint newEndpoint = new Endpoint(fdFactory.create());
            endpoint = members.putIfAbsent(sender, newEndpoint);
            if (endpoint == null) {
                if (log.isDebugEnabled()) {
                    log.debug(String.format("Discovered %s on %s", sender,
                                            localAddress));
                }
                endpoint = newEndpoint;
            }
        }
        List<UUID> updates = endpoint.getUpdates(digests);
        if (!updates.isEmpty()) {
            if (log.isTraceEnabled()) {
                log.trace(String.format("Requesting updates %s from %s from %s",
                                        updates, sender, localAddress));
            }
            requestState(sender, updates);
        }
    }

    void handleStateRequest(InetSocketAddress sender, ByteBuffer buffer) {
        UUID[] ids = extractIds(sender, buffer);
        if (log.isTraceEnabled()) {
            log.trace(String.format("State request ids from %s are %s", sender,
                                    Arrays.toString(ids)));
        }
        ByteBuffer msg = bufferPool.allocate(MAX_SEG_SIZE);
        for (UUID id : ids) {
            ReplicatedState state = localState.get(id);
            if (state != null) {
                msg.clear();
                msg.position(MESSAGE_HEADER_BYTE_SIZE);
                state.writeTo(msg);
                if (log.isTraceEnabled()) {
                    log.trace(String.format("Sending state %s update from %s, requested by %s",
                                            id, localAddress, sender));
                }
            } else {
                log.warn(String.format("unable to find state %s on %s requested from %s",
                                       id, localAddress, sender));
            }
            send(UPDATE, msg, sender);
        }
    }

    void handleUpdate(InetSocketAddress sender, ByteBuffer buffer) {
        if (sender.equals(localAddress)) {
            if (log.isTraceEnabled()) {
                log.trace(format("Ignoring update from self %s", sender));
            }
            return;
        }
        final ReplicatedState state;
        try {
            state = new ReplicatedState(buffer);
        } catch (Throwable e) {
            if (log.isWarnEnabled()) {
                log.warn("Cannot deserialize state. Ignoring the state.", e);
            }
            return;
        }
        Endpoint endpoint = new Endpoint(fdFactory.create());
        Endpoint prev = members.putIfAbsent(sender, endpoint);
        if (prev != null) {
            endpoint = prev;
        }
        endpoint.update(state, listener);
        if (log.isTraceEnabled()) {
            log.trace(format("Update state %s from %s is : %s", state.getId(),
                             sender, state));
        }
    }

    Runnable heartbeatTask() {
        return new Runnable() {
            @Override
            public void run() {
                long now = System.currentTimeMillis();
                List<Digest> digests = new ArrayList<>();
                digests.add(new Digest(Common.HEARTBEAT, now));
                for (ReplicatedState state : localState.values()) {
                    digests.add(new Digest(state.getId(), state.getTime()));
                }
                sendDigests(digests);
            }
        };
    }

    /**
     * @return
     */
    Runnable multicastServiceTask() {
        return serviceTask(multicastSocket, "multicast");
    }

    /**
     * @return
     */
    Runnable p2pServiceTask() {
        return serviceTask(p2pSocket, "p2p");
    }

    void processInbound(InetSocketAddress sender, ByteBuffer buffer)
                                                                    throws UnknownHostException {
        byte msgType = buffer.get();
        switch (msgType) {
            case DIGESTS: {
                handleDigests(sender, buffer);
                break;
            }
            case UPDATE: {
                handleUpdate(sender, buffer);
                break;
            }
            case DEREGISTER: {
                handleDeregister(sender, buffer);
                break;
            }
            case STATE_REQUEST: {
                handleStateRequest(sender, buffer);
                break;
            }
            default: {
                if (log.isInfoEnabled()) {
                    log.info(format("invalid message type: %s from: %s",
                                    msgType, this));
                }
            }
        }
    }

    void requestState(InetSocketAddress sender, List<UUID> updates) {
        ByteBuffer buffer = bufferPool.allocate(MAX_SEG_SIZE);
        buffer.order(ByteOrder.BIG_ENDIAN);
        for (int i = 0; i < updates.size();) {
            buffer.position(MESSAGE_HEADER_BYTE_SIZE);
            byte count = (byte) min(maxUuids, updates.size() - i);
            buffer.put(count);
            for (UUID id : updates.subList(i, i + count)) {
                buffer.putLong(id.getMostSignificantBits());
                buffer.putLong(id.getLeastSignificantBits());
            }
            send(STATE_REQUEST, buffer, sender);
            i += count;
            buffer.clear();
        }
        bufferPool.free(buffer);
    }
}
