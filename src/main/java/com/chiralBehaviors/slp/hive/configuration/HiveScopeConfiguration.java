/*
 * (C) Copyright 2012 Chiral Behaviors, All Rights Reserved
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

import com.chiralBehaviors.slp.hive.HiveScope;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.hellblazer.slp.ServiceScope;
import com.hellblazer.slp.config.ServiceScopeConfiguration;

/**
 * @author hhildebrand
 * 
 */
public class HiveScopeConfiguration implements ServiceScopeConfiguration {
    public static HiveScopeConfiguration fromYaml(File yaml)
                                                            throws JsonParseException,
                                                            JsonMappingException,
                                                            IOException {
        return fromYaml(new FileInputStream(yaml));
    }

    public static HiveScopeConfiguration fromYaml(InputStream yaml)
                                                                   throws JsonParseException,
                                                                   JsonMappingException,
                                                                   IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.registerModule(new EngineModule());
        return mapper.readValue(yaml, HiveScopeConfiguration.class);
    }

    public EngineConfiguration engine              = new BroadcastConfiguration();
    public int                 notificationThreads = 2;

    /* (non-Javadoc)
     * @see com.hellblazer.slp.config.ServiceScopeConfiguration#construct()
     */
    @Override
    public ServiceScope construct() throws Exception {
        return new HiveScope(engine.construct(), 2);
    }

}
