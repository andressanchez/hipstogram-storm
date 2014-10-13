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

package com.hipstogram.storm.state;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import io.hipstogram.trident.mongodb.mappers.MongoDBRowMapper;
import io.hipstogram.trident.mongodb.operation.CRUDOperation;
import io.hipstogram.trident.mongodb.operation.Query;
import io.hipstogram.trident.mongodb.operation.Upsert;
import storm.trident.state.OpaqueValue;
import storm.trident.tuple.TridentTuple;

import java.io.Serializable;
import java.util.List;

public class TrackCountMongoDBMapper implements MongoDBRowMapper<List<Object>, OpaqueValue>, Serializable
{
    private static final long serialVersionUID = 1L;
    //private static final Logger LOG = LoggerFactory.getLogger(TrackCountCassandraMapper.class);

    public static final String HISTOGRAM_NAME = "histogram";

    @Override
    public CRUDOperation map(List<Object> keys, OpaqueValue value)
    {

        Integer inc = 1;
        if (value.getPrev() != null && value.getCurr() != null )
            inc = (Integer) value.getCurr() - (Integer) value.getPrev();

        String uri = (String) keys.get(0);
        Integer time = (Integer) keys.get(1);

        BasicDBObject query = new BasicDBObject("_id", uri);
        BasicDBObject increment = new BasicDBObject("$inc", new BasicDBObject(HISTOGRAM_NAME + "." + time, inc));

        return new Upsert(query, increment);
    }

    @Override
    public Query retrieve(List<Object> keys)
    {
        String uri = (String) keys.get(0);
        Integer time = (Integer) keys.get(1);

        BasicDBObject query = new BasicDBObject("_id", uri);
        BasicDBObject projection = new BasicDBObject("_id", false).append(HISTOGRAM_NAME + "." + time, true);

        return new Query(query, projection);
    }

    @Override
    public OpaqueValue getValue(DBObject dbObject)
    {
        DBObject histogram = (DBObject) dbObject.get(HISTOGRAM_NAME);
        Object[] keys = histogram.keySet().toArray();
        Integer l = 0;

        if (keys.length != 0)
            l = Integer.parseInt(histogram.get((String) keys[0]).toString());

        return (OpaqueValue) new OpaqueValue<Number>(0l, l);
    }

    @Override
    public CRUDOperation map(TridentTuple objects) {
        return null;
    }
}