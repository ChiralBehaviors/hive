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
import static com.chiralBehaviors.slp.hive.Messages.DIGESTS;
import static com.chiralBehaviors.slp.hive.Messages.DIGEST_BYTE_SIZE;
import static com.chiralBehaviors.slp.hive.Messages.MAGIC;
import static com.chiralBehaviors.slp.hive.Messages.MAGIC_BYTE_SIZE;
import static com.chiralBehaviors.slp.hive.Messages.MAX_SEG_SIZE;
import static com.chiralBehaviors.slp.hive.Messages.MESSAGE_HEADER_BYTE_SIZE;
import static com.chiralBehaviors.slp.hive.Messages.REMOVE;
import static com.chiralBehaviors.slp.hive.Messages.STATE_REQUEST;
import static com.chiralBehaviors.slp.hive.Messages.UPDATE;
import static com.chiralBehaviors.slp.hive.Messages.UUID_BYTE_SIZE;
import static java.lang.Math.min;
import static java.lang.String.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
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
import javax.crypto.spec.SecretKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.uuid.NoArgGenerator;
import com.hellblazer.utils.ByteBufferPool;
import com.hellblazer.utils.HexDump;
import com.hellblazer.utils.fd.FailureDetectorFactory;

/**
 * @author hhildebrand
 * 
 */
public class Engine {
    public static final int     DEFAULT_RECEIVE_BUFFER_MULTIPLIER = 4;
    public static final int     DEFAULT_SEND_BUFFER_MULTIPLIER    = 4;
    public static final UUID    HEARTBEAT                         = new UUID(0,
                                                                             0);
    // Default MAC key used strictly for message integrity
    private static byte[]       DEFAULT_KEY_DATA                  = {
            (byte) 0x23, (byte) 0x45, (byte) 0x83, (byte) 0xad, (byte) 0x23,
            (byte) 0x46, (byte) 0x83, (byte) 0xad, (byte) 0x23, (byte) 0x45,
            (byte) 0x83, (byte) 0xad, (byte) 0x23, (byte) 0x45, (byte) 0x83,
            (byte) 0xad                                          };
    // Default MAC used strictly for message integrity
    private static String       DEFAULT_MAC_TYPE                  = "HmacMD5";
    private static final Logger log                               = LoggerFactory.getLogger(Engine.class);

    public static MulticastSocket connect(InetSocketAddress mcastaddr, int ttl,
                                          NetworkInterface netIf)
                                                                 throws IOException {
        MulticastSocket s;
        try {
            s = new MulticastSocket(mcastaddr.getPort());
        } catch (IOException e) {
            log.error(format("Unable to bind multicast socket"), e);
            throw e;
        }
        try {
            s.joinGroup(mcastaddr.getAddress());
        } catch (IOException e) {
            log.error(format("Unable to join group %s on %s for %s", mcastaddr,
                             netIf, s));
            throw e;
        }
        s.setTimeToLive(ttl);
        return s;
    }

    /**
     * @return a default mac, with a fixed key. Used for validation only, no
     *         authentication
     */
    public static Mac defaultMac() {
        Mac mac;
        try {
            mac = Mac.getInstance(DEFAULT_MAC_TYPE);
            mac.init(new SecretKeySpec(DEFAULT_KEY_DATA, DEFAULT_MAC_TYPE));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(
                                            String.format("Unable to create default mac %s",
                                                          DEFAULT_MAC_TYPE));
        } catch (InvalidKeyException e) {
            throw new IllegalStateException(
                                            String.format("Invalid default key %s for default mac %s",
                                                          Arrays.toString(DEFAULT_KEY_DATA),
                                                          DEFAULT_MAC_TYPE));
        }
        return mac;
    }

    public static String prettyPrint(SocketAddress sender,
                                     SocketAddress target, byte[] bytes,
                                     int length) {
        final StringBuilder sb = new StringBuilder(length * 2);
        sb.append('\n');
        sb.append(new SimpleDateFormat().format(new Date()));
        sb.append(" sender: ");
        sb.append(sender);
        sb.append(" target: ");
        sb.append(target);
        sb.append(" length: ");
        sb.append(length);
        sb.append('\n');
        sb.append(toHex(bytes, 0, length));
        return sb.toString();
    }

    public static String toHex(byte[] data, int offset, int length) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
        PrintStream stream = new PrintStream(baos);
        HexDump.hexdump(stream, data, offset, length);
        stream.close();
        return baos.toString();
    }

    private final ByteBufferPool                             bufferPool = new ByteBufferPool(
                                                                                             "Engine Comms",
                                                                                             100);
    private final ScheduledExecutorService                   executor;
    private final FailureDetectorFactory                     fdFactory;
    private final InetSocketAddress                          groupAddess;
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
    private final AtomicBoolean                              running    = new AtomicBoolean();
    private final DatagramSocket                             socket;

    public Engine(FailureDetectorFactory fdFactory, NoArgGenerator idGenerator,
                  int heartbeatPeriod, TimeUnit heartbeatUnit,
                  DatagramSocket socket, InetSocketAddress groupAddress,
                  int receiveBufferMultiplier, int sendBufferMultiplier, Mac mac)
                                                                                 throws SocketException {
        executor = Executors.newSingleThreadScheduledExecutor();
        this.fdFactory = fdFactory;
        this.idGenerator = idGenerator;
        this.heartbeatPeriod = heartbeatPeriod;
        this.heartbeatUnit = heartbeatUnit;
        this.socket = socket;
        groupAddess = groupAddress;
        try {
            socket.setReceiveBufferSize(MAX_SEG_SIZE * receiveBufferMultiplier);
            socket.setSendBufferSize(MAX_SEG_SIZE * sendBufferMultiplier);
        } catch (SocketException e) {
            log.error(format("Unable to configure endpoint: %s", socket));
            throw e;
        }
        hmac = mac;
        localAddress = new InetSocketAddress(socket.getLocalAddress(),
                                             socket.getLocalPort());
        int payloadByteSize = MAX_SEG_SIZE - MESSAGE_HEADER_BYTE_SIZE
                              - mac.getMacLength();
        maxDigests = (payloadByteSize - BYTE_SIZE) // 1 byte for #digests
                     / DIGEST_BYTE_SIZE;
        maxUuids = (payloadByteSize - BYTE_SIZE) // 1 byte for #uuids
                   / UUID_BYTE_SIZE;
    }

    public Engine(FailureDetectorFactory fdFactory,
                  NoArgGenerator timeBasedGenerator, int heartbeatPeriod,
                  TimeUnit heartbeatUnit, int receiveBufferMultiplier,
                  int sendBufferMultiplier, InetSocketAddress groupAddress,
                  NetworkInterface nintf, int ttl, Mac mac)
                                                           throws SocketException,
                                                           IOException {
        this(fdFactory, timeBasedGenerator, heartbeatPeriod, heartbeatUnit,
             connect(groupAddress, ttl, nintf), groupAddress,
             receiveBufferMultiplier, sendBufferMultiplier, mac);
    }

    public void deregister(UUID id) {
        if (id == null) {
            throw new NullPointerException(
                                           "replicated state id must not be null");
        }
        synchronized (localState) {
            localState.remove(id);
        }
        if (log.isDebugEnabled()) {
            log.debug(String.format("Member: %s abandoning replicated state",
                                    getLocalAddress()));
        }
    }

    public InetSocketAddress getLocalAddress() {
        return localAddress;
    }

    public int getMaxStateSize() {
        return MAX_SEG_SIZE - hmac.getMacLength() - MESSAGE_HEADER_BYTE_SIZE;
    }

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
            log.debug(String.format("Member: %s registering replicated state",
                                    getLocalAddress()));
        }
        return id;
    }

    public void setListener(EngineListener listener) {
        this.listener = listener;
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            Executors.newSingleThreadExecutor().execute(serviceTask());
            heartbeatTask = executor.schedule(heartbeatTask(), heartbeatPeriod,
                                              heartbeatUnit);
        }
    }

    public void terminate() {
        if (running.compareAndSet(true, false)) {
            if (log.isInfoEnabled()) {
                log.info(String.format("Terminating UDP Communications on %s",
                                       socket.getLocalSocketAddress()));
            }
            heartbeatTask.cancel(true);
            socket.close();
            log.info(bufferPool.toString());
        }
    }

    @Override
    public String toString() {
        return String.format("BroadcastComms[%s]", getLocalAddress());
    }

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
        synchronized (localState) {
            localState.put(id, state);
        }
        if (log.isDebugEnabled()) {
            log.debug(String.format("Member: %s updating replicated state",
                                    getLocalAddress()));
        }
        ByteBuffer buffer = bufferPool.allocate(MAX_SEG_SIZE);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.position(MAGIC_BYTE_SIZE);
        try {
            state.writeTo(buffer);
            send(UPDATE, buffer, groupAddess);
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

    private Digest[] extractDigests(SocketAddress sender, ByteBuffer msg) {
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
                                           prettyPrint(sender,
                                                       getLocalAddress(),
                                                       msg.array(), msg.limit())),
                             e);
                }
                continue;
            }
            digests[i] = digest;
        }
        return digests;
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
                                           prettyPrint(sender,
                                                       getLocalAddress(),
                                                       msg.array(), msg.limit())),
                             e);
                }
                continue;
            }
            uuids[i] = id;
        }
        return uuids;
    }

    private void handleDigests(InetSocketAddress sender, ByteBuffer buffer) {
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
                endpoint = newEndpoint;
            }
        }
        List<UUID> updates = endpoint.getUpdates(digests);
        if (!updates.isEmpty()) {
            requestState(sender, updates);
        }
    }

    private void handleRemove(InetSocketAddress sender, ByteBuffer buffer) {
        UUID stateId = new UUID(buffer.getLong(), buffer.getLong());
        Endpoint endpoint = members.get(sender);
        if (endpoint == null) {
            log.trace(String.format("Remove %s from unknown member %s",
                                    stateId, sender));
            return;
        }
        endpoint.remove(stateId);
        listener.deregister(stateId);
    }

    private void handleStateRequest(InetSocketAddress sender, ByteBuffer buffer) {
        UUID[] ids = extractIds(sender, buffer);
        if (log.isTraceEnabled()) {
            log.trace(String.format("State request ids from %s are %s", sender,
                                    Arrays.toString(ids)));
        }
        ByteBuffer msg = bufferPool.allocate(MAX_SEG_SIZE);
        for (UUID id : ids) {
            ReplicatedState state = localState.get(id);
            state.writeTo(msg);
            send(UPDATE, msg, sender);
        }
    }

    private void handleUpdate(InetSocketAddress sender, ByteBuffer buffer) {
        final ReplicatedState state;
        try {
            state = new ReplicatedState(buffer);
        } catch (Throwable e) {
            if (log.isWarnEnabled()) {
                log.warn("Cannot deserialize state. Ignoring the state.", e);
            }
            return;
        }
        if (log.isTraceEnabled()) {
            log.trace(format("Update state from %s is : %s", sender, state));
        }
        Endpoint endpoint = new Endpoint(fdFactory.create(), state);
        if (members.putIfAbsent(sender, endpoint) == null) {
            listener.register(state.getId(), state.getState());
        } else {
            endpoint.update(state.getDigest());
            listener.update(state.getId(), state.getState());
        }
    }

    private Runnable heartbeatTask() {
        return new Runnable() {
            @Override
            public void run() {
                long now = System.currentTimeMillis();
                List<Digest> digests = new ArrayList<>();
                digests.add(new Digest(HEARTBEAT, now));
                for (ReplicatedState state : localState.values()) {
                    digests.add(new Digest(state.getId(), state.getTime()));
                }
                sendDigests(digests);
            }
        };
    }

    private void processInbound(InetSocketAddress sender, ByteBuffer buffer) {
        byte msgType = buffer.get();
        switch (msgType) {
            case DIGESTS: {
                handleDigests(sender, buffer);
                break;
            }
            case REMOVE: {
                handleRemove(sender, buffer);
                break;
            }
            case UPDATE: {
                handleUpdate(sender, buffer);
                break;
            }
            case STATE_REQUEST: {
                handleStateRequest(sender, buffer);
            }
            default: {
                if (log.isInfoEnabled()) {
                    log.info(format("invalid message type: %s from: %s",
                                    msgType, this));
                }
            }
        }
    }

    private void requestState(InetSocketAddress sender, List<UUID> updates) {
        ByteBuffer buffer = bufferPool.allocate(MAX_SEG_SIZE);
        buffer.order(ByteOrder.BIG_ENDIAN);
        for (int i = 0; i < updates.size();) {
            byte count = (byte) min(maxUuids, updates.size() - i);
            buffer.position(MAGIC_BYTE_SIZE);
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

    private void send(byte msgType, ByteBuffer buffer, InetSocketAddress target) {
        if (target.getPort() == 0) {

        }
        if (socket.isClosed()) {
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
            log.error("Invalid message %s",
                      prettyPrint(getLocalAddress(), target, buffer.array(),
                                  msgLength));
            return;
        } catch (SecurityException e) {
            log.error("No key provided for HMAC");
            return;
        }
        try {
            log.info(String.format("Sending packet to %s from %s", target,
                                   socket.getLocalSocketAddress()));
            DatagramPacket packet = new DatagramPacket(bytes, totalLength,
                                                       target);
            if (log.isTraceEnabled()) {
                log.trace(String.format("sending packet mac start: %s %s",
                                        msgLength,
                                        prettyPrint(getLocalAddress(), target,
                                                    buffer.array(), totalLength)));
            }
            socket.send(packet);
        } catch (SocketException e) {
            if (!"Socket is closed".equals(e.getMessage())
                && !"Bad file descriptor".equals(e.getMessage())) {
                if (log.isWarnEnabled()) {
                    log.warn("Error sending packet", e);
                }
            }
        } catch (IOException e) {
            if (log.isWarnEnabled()) {
                log.warn("Error sending packet", e);
            }
        }
    }

    private void sendDigests(List<Digest> digests) {
        ByteBuffer buffer = bufferPool.allocate(MAX_SEG_SIZE);
        buffer.order(ByteOrder.BIG_ENDIAN);
        for (int i = 0; i < digests.size();) {
            byte count = (byte) min(maxDigests, digests.size() - i);
            buffer.position(MAGIC_BYTE_SIZE);
            buffer.put(count);
            int position;
            for (Digest digest : digests.subList(i, i + count)) {
                digest.writeTo(buffer);
                position = buffer.position();
                Integer.toString(position);
            }
            send(DIGESTS, buffer, groupAddess);
            i += count;
            buffer.clear();
        }
        bufferPool.free(buffer);
    }

    private void service() throws IOException {
        final ByteBuffer buffer = bufferPool.allocate(MAX_SEG_SIZE);
        buffer.order(ByteOrder.BIG_ENDIAN);
        final DatagramPacket packet = new DatagramPacket(buffer.array(),
                                                         buffer.array().length);
        if (log.isTraceEnabled()) {
            log.trace(String.format("listening for packet on %s",
                                    socket.getLocalSocketAddress()));
        }
        socket.receive(packet);
        buffer.limit(packet.getLength());
        if (log.isTraceEnabled()) {
            log.trace(String.format("Received packet %s",
                                    prettyPrint(packet.getSocketAddress(),
                                                getLocalAddress(),
                                                buffer.array(),
                                                packet.getLength())));
        } else if (log.isTraceEnabled()) {
            log.trace("Received packet from: " + packet.getSocketAddress());
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
                                log.warn(format("Error processing inbound message on: %s, HMAC does not check",
                                                getLocalAddress()));
                            }
                            return;
                        }
                    } catch (SecurityException e) {
                        if (log.isWarnEnabled()) {
                            log.warn(format("Error processing inbound message on: %s, HMAC does not check",
                                            getLocalAddress()), e);
                        }
                        return;
                    }
                    buffer.limit(packet.getLength() - hmac.getMacLength());
                    try {
                        processInbound((InetSocketAddress) packet.getSocketAddress(),
                                       buffer);
                    } catch (Throwable e) {
                        if (log.isWarnEnabled()) {
                            log.warn(format("Error processing inbound message on: %s",
                                            getLocalAddress()), e);
                        }
                    }
                } else {
                    if (log.isWarnEnabled()) {
                        log.warn(format("Msg with invalid MAGIC header [%s] discarded %s",
                                        magic,
                                        prettyPrint(packet.getSocketAddress(),
                                                    getLocalAddress(),
                                                    buffer.array(),
                                                    packet.getLength())));
                    }
                }
                bufferPool.free(buffer);
            }
        });
    }

    private Runnable serviceTask() {
        return new Runnable() {
            @Override
            public void run() {
                if (log.isInfoEnabled()) {
                    log.info(String.format("UDP muticast communications started on %s",
                                           getLocalAddress()));
                }
                while (running.get()) {
                    try {
                        service();
                    } catch (SocketException e) {
                        if ("Socket closed".equals(e.getMessage())) {
                            if (log.isTraceEnabled()) {
                                log.trace("Socket closed, shutting down");
                                terminate();
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
}
