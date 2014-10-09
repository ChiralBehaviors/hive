/** (C) Copyright 2014 Chiral Behaviors, All Rights Reserved
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

import static java.lang.String.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.SocketException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.UUID;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hellblazer.utils.HexDump;

/**
 * @author <a href="mailto:hal.hildebrand@me.com">Chiral Behaviors</a>
 *
 */
public class Common {
    private static Logger    log                               = LoggerFactory.getLogger(Common.class);

    public static final int  DEFAULT_RECEIVE_BUFFER_MULTIPLIER = 4;
    public static final int  DEFAULT_SEND_BUFFER_MULTIPLIER    = 4;
    public static final UUID HEARTBEAT                         = new UUID(0, 0);
    // Default MAC key used strictly for message integrity
    private static byte[]    DEFAULT_KEY_DATA                  = { (byte) 0x23,
            (byte) 0x45, (byte) 0x83, (byte) 0xad, (byte) 0x23, (byte) 0x46,
            (byte) 0x83, (byte) 0xad, (byte) 0x23, (byte) 0x45, (byte) 0x83,
            (byte) 0xad, (byte) 0x23, (byte) 0x45, (byte) 0x83, (byte) 0xad };
    // Default MAC used strictly for message integrity
    private static String    DEFAULT_MAC_TYPE                  = "HmacMD5";

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

    public static MulticastSocket connect(InetSocketAddress mcastaddr, int ttl,
                                          NetworkInterface netIf)
                                                                 throws IOException {
        MulticastSocket s = null;
        try {
            for (InterfaceAddress address : netIf.getInterfaceAddresses()) {
                InetAddress ip = address.getAddress();
                if (ip != null
                    && mcastaddr.getAddress().getClass().equals(ip.getClass())) {
                    s = new MulticastSocket(
                                            new InetSocketAddress(
                                                                  mcastaddr.getPort()));
                    break;
                }
            }
            if (s == null) {
                throw new IllegalStateException(
                                                "Cannot find a suitable internet address");
            }
        } catch (IOException e) {
            log.error(format("Unable to bind multicast socket"), e);
            throw e;
        }
        try {
            s.joinGroup(mcastaddr, netIf);
        } catch (IOException e) {
            log.error(format("Unable to join group %s on %s for %s", mcastaddr,
                             netIf, s));
            throw e;
        }
        s.setTimeToLive(ttl);
        return s;
    }

    public static DatagramSocket connect(NetworkInterface iface,
                                         InetSocketAddress groupAddress)
                                                                        throws SocketException {

        InetAddress bind = null;
        for (Enumeration<InetAddress> addresses = iface.getInetAddresses(); addresses.hasMoreElements();) {
            InetAddress address = addresses.nextElement();
            if (address.getClass().equals(groupAddress.getAddress().getClass())) {
                bind = address;
                break;
            }
        }
        if (bind == null) {
            throw new IllegalArgumentException(
                                               String.format("No matching address for %s found on %s",
                                                             groupAddress,
                                                             iface));
        }
        return new DatagramSocket(new InetSocketAddress(bind, 0));
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
}
