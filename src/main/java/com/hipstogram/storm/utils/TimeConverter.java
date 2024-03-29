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

package com.hipstogram.storm.utils;

/**
 * Class for time conversion
 */
public class TimeConverter
{
    /**
     * Convert a time in format "mm:ss" into seconds
     * @param time Time in format "mm:ss"
     * @return Time in seconds
     */
    public static int toSeconds(String time)
    {
        String[] t = time.split(":");
        if (t.length == 2) return Integer.parseInt(t[0]) * 60 + Integer.parseInt(t[1]);
        return 0;
    }
}
