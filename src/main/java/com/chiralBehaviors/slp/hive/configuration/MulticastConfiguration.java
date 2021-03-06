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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

import com.chiralBehaviors.slp.hive.Common;
import com.chiralBehaviors.slp.hive.MulticastEngine;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.uuid.Generators;

/**
 * @author hhildebrand
 * 
 */
public class MulticastConfiguration extends EngineConfiguration {
    public static MulticastConfiguration fromYaml(File yaml)
                                                            throws JsonParseException,
                                                            JsonMappingException,
                                                            IOException {
        return fromYaml(new FileInputStream(yaml));
    }

    public static MulticastConfiguration fromYaml(InputStream yaml)
                                                                   throws JsonParseException,
                                                                   JsonMappingException,
                                                                   IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        //mapper.registerModule(new EngineModule());
        return mapper.readValue(yaml, MulticastConfiguration.class);
    }

    public InetSocketAddress multicastGroup = new InetSocketAddress(
                                                                    "233.1.2.30",
                                                                    1966);
    public int               ttl            = 1;

    @Override
    public MulticastEngine construct() throws IOException {
        NetworkInterface networkInterface = getNetworkInterface();
        MulticastSocket socket = Common.connect(multicastGroup, ttl,
                                                networkInterface);
        return new MulticastEngine(getFdFactory(),
                                   Generators.timeBasedGenerator(),
                                   heartbeatPeriod, heartbeatUnit, socket,
                                   multicastGroup, receiveBufferMultiplier,
                                   sendBufferMultiplier, getMac(),
                                   networkInterface);
    }

    public NetworkInterface getNetworkInterface() throws SocketException {
        if (networkInterface == null) {
            for (Enumeration<NetworkInterface> intfs = NetworkInterface.getNetworkInterfaces(); intfs.hasMoreElements();) {
                NetworkInterface intf = intfs.nextElement();
                if (intf.supportsMulticast()) {
                    return intf;
                }
            }
            throw new IllegalStateException(
                                            "No interface supporting multicast was discovered");
        }
        NetworkInterface iface = NetworkInterface.getByName(networkInterface);
        if (iface == null) {
            throw new IllegalArgumentException(
                                               String.format("Cannot find network interface: %s ",
                                                             networkInterface));
        }
        return iface;
    }
}
