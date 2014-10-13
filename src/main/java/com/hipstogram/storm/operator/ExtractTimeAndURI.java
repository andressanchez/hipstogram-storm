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

package com.hipstogram.storm.operator;

import com.google.gson.Gson;
import com.hipstogram.storm.model.Event;
import com.hipstogram.storm.utils.TimeConverter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

public class ExtractTimeAndURI extends BaseFunction
{
    private static final long serialVersionUID = 1L;
    private static final int WINDOW = 5; // Seconds
    //private static final Logger LOG = LoggerFactory.getLogger(ExtractTimeAndURI.class);

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector)
    {
        // Convert incoming JSON String to an Event
        String eventStr = (String) tuple.getValue(0);
        Event event = new Gson().fromJson(eventStr, Event.class);

        // Keep a log of incoming events
        //LOG.warn(eventStr);

        // Extract and emit starting time, ending time and URI
        int start = TimeConverter.toSeconds(event.getStartTime());
        int end = TimeConverter.toSeconds(event.getEndTime());

        for (int i = nearestWindow(start); i < end; i += WINDOW)
        {
            List<Object> values = new ArrayList<Object>();
            values.add(event.getUri());
            values.add(i);
            collector.emit(values);
        }
    }

    public static int nearestWindow(int v)
    {
        return (int) (Math.floor(v / WINDOW) * WINDOW);
    }
}