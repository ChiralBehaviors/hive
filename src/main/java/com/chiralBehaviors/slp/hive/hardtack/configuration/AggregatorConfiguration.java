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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.BindException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.util.concurrent.Executors;

import com.chiralBehaviors.slp.hive.Engine;
import com.chiralBehaviors.slp.hive.hardtack.AggregatorEngine;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.hellblazer.utils.Utils;

/**
 * @author <a href="mailto:hal.hildebrand@me.com">Chiral Behaviors</a>
 *
 */
public class AggregatorConfiguration extends HardtackConfiguration {
    public static AggregatorConfiguration fromYaml(File yaml)
                                                             throws JsonParseException,
                                                             JsonMappingException,
                                                             IOException {
        return fromYaml(new FileInputStream(yaml));
    }

    public static AggregatorConfiguration fromYaml(InputStream yaml)
                                                                    throws JsonParseException,
                                                                    JsonMappingException,
                                                                    IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(yaml, AggregatorConfiguration.class);
    }

    public static final int  DEFAULT_PORT = 56999;

    public InetSocketAddress endpoint     = new InetSocketAddress(DEFAULT_PORT);

    /* (non-Javadoc)
     * @see com.chiralBehaviors.slp.hive.configuration.EngineConfiguration#construct()
     */
    @Override
    public Engine construct() throws IOException {
        NetworkInterface intf = getNetworkInterface();
        DatagramSocket socket;
        InetSocketAddress address;
        if (endpoint.getAddress().isAnyLocalAddress()) {
            address = new InetSocketAddress(Utils.getAddress(intf, ipv4),
                                            endpoint.getPort());
        } else {
            address = endpoint;
        }
        try {
            socket = new DatagramSocket(address);
        } catch (BindException e) {
            BindException bindException = new BindException(
                                                            String.format("Cannot bind to %s",
                                                                          address));
            bindException.initCause(e);
            throw bindException;
        }
        return new AggregatorEngine(
                                    socket,
                                    getMac(),
                                    Executors.newSingleThreadScheduledExecutor(),
                                    getFdFactory());
    }
}
