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

package com.hipstogram.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.hipstogram.storm.state.IntegerCount;
import com.hipstogram.storm.state.TrackCountCassandraMapper;
import com.hipstogram.storm.context.HipstogramContext;
import com.hipstogram.storm.context.config.sections.KafkaSection;
import com.hipstogram.storm.context.config.sections.ZookeeperSection;
import com.hipstogram.storm.operator.ExtractTimeAndURI;
import com.hipstogram.storm.state.TrackCountMongoDBMapper;
import com.hmsonline.trident.cql.CassandraCqlMapState;
import com.hmsonline.trident.cql.CassandraCqlStateFactory;
import io.hipstogram.trident.mongodb.MongoDBMapState;
import io.hipstogram.trident.mongodb.MongoDBStateFactory;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;

public class HipstogramTopology
{
    public static StormTopology buildTopology()
    {
        // 1. Create a new Trident topology
        TridentTopology topology = new TridentTopology();

        //2. Configure Kafka
        KafkaSection kafkaSection = HipstogramContext.getInstance().getConfiguration().getKafkaSection();
        ZookeeperSection zookeeperSection = HipstogramContext.getInstance().getConfiguration().getZookeeperSection();
        BrokerHosts zk = new ZkHosts(zookeeperSection.host + ":" + zookeeperSection.port);
        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, kafkaSection.topic);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        //spoutConf.forceFromStart = true;

        /* 3. Configure Cassandra
        CassandraCqlMapState.Options options = new CassandraCqlMapState.Options();
        options.keyspace = "hipstogram";
        options.tableName = "transactions";*/

        // 3. Configure MongoDB
        MongoDBMapState.Options options = new MongoDBMapState.Options();
        options.db = "test";
        options.collection = "tracks";

        // 4. Create a Kafka spout
        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

        // 5. Define the data stream
        Stream inputStream = topology.newStream("str", spout);
        inputStream
                // 5.1. Extract: starting time, ending time and URI
                .each(new Fields("str"), new ExtractTimeAndURI(), new Fields("uri", "time"))
                // 5.2. Group by: URI and time. So, two events with the same URI and time
                // will be processed by the same instance
                .groupBy(new Fields("uri", "time"))
                // 5.3. Update counters in Cassandra
                //.persistentAggregate(CassandraCqlMapState.opaque(new TrackCountCassandraMapper(), options), new IntegerCount(), new Fields("count"));
                // 5.3. Update counters in MongoDB
                .persistentAggregate(MongoDBMapState.opaque(new TrackCountMongoDBMapper(), options), new IntegerCount(), new Fields("count"));

        // 6. Return Storm topology
        return topology.build();
    }

    public static void main(String[] args) throws Exception
    {
        System.setProperty("config", "config/config.ini"); // TODO: Change this!!
        Config conf = new Config();
        conf.put(CassandraCqlStateFactory.TRIDENT_CASSANDRA_CQL_HOSTS, "192.168.2.100");
        conf.put(MongoDBStateFactory.MONGODB_HOSTS, "192.168.2.100");
        conf.put(MongoDBStateFactory.MONGODB_DB, "test");
        conf.put(MongoDBStateFactory.MONGODB_COLLECTION, "tracks");
        conf.setNumWorkers(3);

        //LocalCluster cluster = new LocalCluster();
        //cluster.submitTopology("HipstogramLocal", conf, buildTopology());
        StormSubmitter.submitTopology("HipstogramTrackCounter", conf, buildTopology());

        //Thread.sleep(120000);
        //cluster.shutdown();
    }
}
