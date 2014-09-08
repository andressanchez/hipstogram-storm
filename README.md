![Hipstogram](https://raw.githubusercontent.com/andressanchez/hipstogram-extension/master/screenshots/logo.png)

**[Hipstogram][1]** is a project to analyze the habits of people when they are listening to music. Thus, we are able to answer questions such as: Why do people like this song? Which parts of this song do people like the most? What sort of people like this artist? All these questions will help us to understand people and their behaviors.

As an ambitious but realistic goal, it is composed of different subprojects: a Google Chrome extension, a backend, a set of Storm topologies and a frontend.  At this moment, **Hipstogram** is still in its early stages. Use it under your own risk and feel free to send us as many feedback as you want.

----------


Storm Topologies
--------------------------------

This repository defines a set of Storm topologies to generate statistics from user interactions. Their main purpose is to count the number of times that each part of a track has been played.


> **Note:**
> These topologies require to have installed Zookeeper, Storm, Kafka and Cassandra.

----------

License
--------------------------------

This work is licensed under a Apache v2.0 License.

Copyright (c) 2014 by Andrés Sánchez Pascual

  [1]: http://hipstogram.io/