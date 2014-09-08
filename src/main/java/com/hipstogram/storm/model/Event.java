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

package com.hipstogram.storm.model;

/**
 * This class represent a Hipstogram Event
 * @author Andrés Sánchez
 */
public class Event {

    private long timestamp;
    private String startTime;
    private String endTime;
    private String startEvent;
    private String endEvent;
    private String uri;
    private String title;
    private String artist;
    private String cover;

    public Event(long timestamp, String startTime, String endTime, String startEvent,
                 String endEvent, String uri, String title, String artist, String cover)
    {
        this.timestamp = timestamp;
        this.startTime = startTime;
        this.endTime = endTime;
        this.startEvent = startEvent;
        this.endEvent = endEvent;
        this.uri = uri;
        this.title = title;
        this.artist = artist;
        this.cover = cover;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public String getStartEvent() {
        return startEvent;
    }

    public void setStartEvent(String startEvent) {
        this.startEvent = startEvent;
    }

    public String getEndEvent() {
        return endEvent;
    }

    public void setEndEvent(String endEvent) {
        this.endEvent = endEvent;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getArtist() {
        return artist;
    }

    public void setArtist(String artist) {
        this.artist = artist;
    }

    public String getCover() {
        return cover;
    }

    public void setCover(String cover) {
        this.cover = cover;
    }
}

