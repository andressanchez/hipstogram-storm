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

package com.hipstogram.storm.context.config;

import com.hipstogram.storm.context.config.sections.KafkaSection;
import com.hipstogram.storm.context.config.sections.ZookeeperSection;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalINIConfiguration;

import java.io.File;

/**
 * Config reads the configuration file and create an object for each section.
 * @author Andrés Sánchez
 */
public class Config
{
    private KafkaSection kafkaSection;
    private ZookeeperSection zookeeperSection;

    public Config(File configFile, File... otherConfigFile) throws ConfigurationException {
        HierarchicalINIConfiguration iniConfig = new HierarchicalINIConfiguration(configFile);
        this.kafkaSection = new KafkaSection(iniConfig.getSection("kafka"));
        this.zookeeperSection = new ZookeeperSection(iniConfig.getSection("zookeeper"));
    }

    public KafkaSection getKafkaSection() {
        return kafkaSection;
    }

    public ZookeeperSection getZookeeperSection() {
        return zookeeperSection;
    }
}
