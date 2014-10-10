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

package com.chiralBehaviors.slp.hive.hardtack.configuration;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

import com.chiralBehaviors.slp.hive.configuration.EngineConfiguration;

/**
 * @author <a href="mailto:hal.hildebrand@me.com">Chiral Behaviors</a>
 *
 */
public abstract class HardtackConfiguration extends EngineConfiguration {

    public boolean ipv4 = true;

    protected NetworkInterface getNetworkInterface() throws SocketException {
        if (networkInterface == null) {
            for (Enumeration<NetworkInterface> intfs = NetworkInterface.getNetworkInterfaces(); intfs.hasMoreElements();) {
                return intfs.nextElement();
            }
            throw new IllegalStateException(
                                            "No interface supporting broadcast was discovered");
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