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
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeUnit;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.chiralBehaviors.slp.hive.Common;
import com.chiralBehaviors.slp.hive.Engine;
import com.hellblazer.utils.Base64Coder;
import com.hellblazer.utils.fd.FailureDetectorFactory;
import com.hellblazer.utils.fd.impl.AdaptiveFailureDetectorFactory;

/**
 * @author hhildebrand
 * 
 */
abstract public class EngineConfiguration {

    public FailureDetectorFactory fdFactory;
    public int                    heartbeatPeriod         = 3;
    public TimeUnit               heartbeatUnit           = TimeUnit.SECONDS;
    public String                 hmac                    = "HmacMD5";
    public String                 hmacKey                 = "I0WDrSNGg60jRYOtI0WDrQ==";
    public String                 networkInterface;
    public int                    receiveBufferMultiplier = Common.DEFAULT_RECEIVE_BUFFER_MULTIPLIER;
    public int                    sendBufferMultiplier    = Common.DEFAULT_SEND_BUFFER_MULTIPLIER;

    abstract public Engine construct() throws IOException;

    public FailureDetectorFactory getFdFactory() {
        if (fdFactory == null) {
            long heartbeatIntervalMillis = heartbeatUnit.toMillis(heartbeatPeriod);
            fdFactory = new AdaptiveFailureDetectorFactory(
                                                           0.9,
                                                           100,
                                                           0.8,
                                                           3 * heartbeatIntervalMillis,
                                                           10,
                                                           heartbeatIntervalMillis);
        }
        return fdFactory;
    }

    public Mac getMac() {
        if (hmac == null || hmacKey == null) {
            return Common.defaultMac();
        }
        Mac mac;
        try {
            mac = Mac.getInstance(hmac);
            mac.init(new SecretKeySpec(Base64Coder.decode(hmacKey), hmac));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(
                                            String.format("Unable to create mac %s",
                                                          hmac));
        } catch (InvalidKeyException e) {
            throw new IllegalStateException(
                                            String.format("Invalid key %s for mac %s",
                                                          hmacKey, hmac));
        }
        return mac;
    }
}
