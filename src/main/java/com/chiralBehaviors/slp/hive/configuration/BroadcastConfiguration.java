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

package com.chiralBehaviors.slp.hive.configuration;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;

import com.chiralBehaviors.slp.hive.Engine;
import com.fasterxml.uuid.Generators;

/**
 * @author hhildebrand
 * 
 */
public class BroadcastConfiguration extends EngineConfiguration {

    public boolean ipv4 = true;
    public int     port = 56999;

    /* (non-Javadoc)
     * @see com.chiralBehaviors.slp.hive.configuration.EngineConfiguration#construct()
     */
    @Override
    public Engine construct() throws IOException {
        NetworkInterface networkInterface = getNetworkInterface();
        InetSocketAddress broadcastAddress = null;
        for (InterfaceAddress addr : networkInterface.getInterfaceAddresses()) {
            if (ipv4) {
                if (addr.getBroadcast() != null
                    && addr.getBroadcast().getAddress().length == 4) {
                    broadcastAddress = new InetSocketAddress(
                                                             addr.getBroadcast(),
                                                             port);
                    break;
                }
            } else {
                if (addr.getBroadcast() != null
                    && addr.getBroadcast().getAddress().length > 4) {
                    broadcastAddress = new InetSocketAddress(
                                                             addr.getBroadcast(),
                                                             port);
                    break;
                }
            }
        }
        if (broadcastAddress == null) {
            throw new IllegalStateException(
                                            String.format("Unable to find a suitable address on interface %s",
                                                          networkInterface));
        }
        DatagramSocket socket = new MulticastSocket(new InetSocketAddress(port));
        socket.setReuseAddress(true);
        socket.setBroadcast(true);
        return new Engine(getFdFactory(), Generators.timeBasedGenerator(),
                          heartbeatPeriod, heartbeatUnit, socket,
                          broadcastAddress, receiveBufferMultiplier,
                          sendBufferMultiplier, getMac(), networkInterface);
    }

}
