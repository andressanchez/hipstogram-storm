/**
 *  Copyright 2014 Andrés Sánchez Pascual
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.hipstogram.storm.context;

import com.hipstogram.storm.context.config.Config;
import org.apache.commons.configuration.ConfigurationException;

import java.io.File;

/**
 * HipstogramContext holds all the components that the topology needs.
 * @author Andrés Sánchez
 */
public class HipstogramContext
{
    private Config configuration;
    private static HipstogramContext instance; // Singleton

    private HipstogramContext()
    {
        String configFile = System.getProperty("config");

        if (configFile == null)
            configFile = getClass().getClassLoader().getResource("config/config.ini").getFile();

        try {
            System.out.println("Expecting config file in: " + configFile);
            configuration = new Config(new File(configFile));
        } catch (ConfigurationException e) {
            // Java, I hate you. Checked exceptions are the devil. Look, what you are forcing me to do, LOOK!
            e.printStackTrace();
        }
    }

    public Config getConfiguration() {
        return configuration;
    }

    public static HipstogramContext getInstance() {
        if (instance == null) instance = new HipstogramContext();
        return instance;
    }
}
