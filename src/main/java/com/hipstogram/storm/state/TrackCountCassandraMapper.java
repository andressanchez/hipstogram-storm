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

import java.io.Serializable;
import java.util.List;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.SimpleStatement;
import storm.trident.state.OpaqueValue;
import storm.trident.tuple.TridentTuple;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.hmsonline.trident.cql.mappers.CqlRowMapper;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

public class TrackCountCassandraMapper implements CqlRowMapper<List<Object>, OpaqueValue>, Serializable
{
    private static final long serialVersionUID = 1L;
    //private static final Logger LOG = LoggerFactory.getLogger(TrackCountCassandraMapper.class);

    public static final String KEYSPACE_NAME = "hipstogram";
    public static final String TABLE_NAME = "track_counts";
    public static final String URI_KEY_NAME = "uri";
    public static final String TIME_KEY_NAME = "time";
    public static final String VALUE_NAME = "count";

    @Override
    public Statement map(List<Object> keys, OpaqueValue value)
    {

        Integer increment = 1;
        if (value.getPrev() != null && value.getCurr() != null )
            increment = (Integer) value.getCurr() - (Integer) value.getPrev();

        String statement = "UPDATE " + KEYSPACE_NAME + "." + TABLE_NAME
                + " SET " + VALUE_NAME + " = " + VALUE_NAME + " + " + increment
                + " WHERE " + URI_KEY_NAME + " = '" + keys.get(0) + "' AND " + TIME_KEY_NAME + " = " + keys.get(1) + ";";

        BatchStatement batch = new BatchStatement(BatchStatement.Type.COUNTER);
        batch.add(new SimpleStatement(statement));

        return batch;
    }

    @Override
    public Statement retrieve(List<Object> keys)
    {
        // Retrieve all the columns associated with the keys
        Select statement = QueryBuilder.select().column(URI_KEY_NAME)
                .column(TIME_KEY_NAME).column(VALUE_NAME)
                .from(KEYSPACE_NAME, TABLE_NAME);
        statement.where(eq(URI_KEY_NAME, keys.get(0)));
        statement.where(eq(TIME_KEY_NAME, keys.get(1)));
        return statement;
    }

    @Override
    public OpaqueValue getValue(Row row)
    {
        return (OpaqueValue) new OpaqueValue<Number>(0l, (int) row.getLong(VALUE_NAME));
    }

    @Override
    public Statement map(TridentTuple tuple)
    {
        // TODO Auto-generated method stub
        return null;
    }


}