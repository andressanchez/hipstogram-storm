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

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class IntegerCount implements CombinerAggregator<Integer>
{
    private static final long serialVersionUID = 1L;

    @Override
    public Integer init(TridentTuple tuple) {
        return 1;
    }

    @Override
    public Integer combine(Integer val1, Integer val2) {
        return val1 + val2;
    }

    @Override
    public Integer zero() {
        return 0;
    }

}