/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.examples.temperature;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilTimeLimit;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;


/**
 * Demonstrates, using the high-level KStream DSL, how to implement an IoT demo application
 * which ingests temperature value processing the maximum value in the latest TEMPERATURE_WINDOW_SIZE seconds (which
 * is 5 seconds) sending a new message if it exceeds the TEMPERATURE_THRESHOLD (which is 20)
 *
 * In this example, the input stream reads from a topic named "iot-temperature", where the values of messages
 * represent temperature values; using a TEMPERATURE_WINDOW_SIZE seconds "tumbling" window, the maximum value is processed and
 * sent to a topic named "iot-temperature-max" if it exceeds the TEMPERATURE_THRESHOLD.
 *
 * Before running this example you must create the input topic for temperature values in the following way :
 *
 * bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic iot-temperature
 *
 * and at same time the output topic for filtered values :
 *
 * bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic iot-temperature-max
 *
 * After that, a console consumer can be started in order to read filtered values from the "iot-temperature-max" topic :
 *
 * bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic iot-temperature-max --from-beginning
 *
 * On the other side, a console producer can be used for sending temperature values (which needs to be integers)
 * to "iot-temperature" typing them on the console :
 *
 * bin/kafka-console-producer.sh --broker-list localhost:9092 --topic iot-temperature
 * > 10
 * > 15
 * > 22
 */
public class TemperatureDemo {

    // threshold used for filtering max temperature values
    private static final int TEMPERATURE_THRESHOLD = 20;
    // window size within which the filtering is applied
    private static final int TEMPERATURE_WINDOW_SIZE = 5;

    public static void main(final String[] args) {

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-temperature");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        final Topology topology = getTopology();

        final String currentTopologyDescription = topology.describe().toString();
        System.out.println(currentTopologyDescription);

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> source = builder.stream("iot-temperature");

        final KStream<Windowed<String>, String> max = source
            // temperature values are sent without a key (null), so in order
            // to group and reduce them, a key is needed ("temp" has been chosen)
            .selectKey((key, value) -> "temp")
            .groupByKey()
            .windowedBy(TimeWindows.of(Duration.ofSeconds(TEMPERATURE_WINDOW_SIZE)))
            .reduce((value1, value2) -> {
                if (Integer.parseInt(value1) > Integer.parseInt(value2)) {
                    return value1;
                } else {
                    return value2;
                }
            })
            .toStream()
            .filter((key, value) -> Integer.parseInt(value) > TEMPERATURE_THRESHOLD);

        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class);

        // need to override key serde to Windowed<String> type
        max.to("iot-temperature-max", Produced.with(windowedSerde, Serdes.String()));

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-temperature-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    static Topology getTopology() {
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        final LogDataPredicate notNullLogData = new LogDataPredicate();
        final JsonSerializer<LogData> logDataSerializer = new JsonSerializer<>();
        final JsonDeserializer<LogData> logDataDeserialzer = new JsonDeserializer<>();

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("class", LogData.class);
        logDataDeserialzer.configure(configMap, false);

        final Serde<LogData> logDataSerde = Serdes.serdeFrom(logDataSerializer, logDataDeserialzer);


        final KeyValueMapper<Windowed<String>, Long, KeyValue<String, Long>> windowMapper = (wk, cnt) -> KeyValue.pair(wk.key(), cnt);

        final StreamsBuilder builder = new StreamsBuilder();
        final ObjectReader mapReader = new ObjectMapper().readerFor(Map.class);

        final Map<String, String> changeLogConfigs = new HashMap<>();
        changeLogConfigs.put("retention.bytes", "104857600");
        changeLogConfigs.put("retention.ms", "21600000");
        changeLogConfigs.put("cleanup.policy", "compact,delete");

        final String windowedTopic = "windowed-node-counts";

        final KStream<String, String> caasLogStream = builder.stream("logs.syslog",
            Consumed.with(stringSerde, stringSerde)
                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));



        final KStream<String, LogData> logDataStream = caasLogStream.mapValues(value -> {
            LogData returnValue = null;
            try {
                final Map<String, Object> jsonMap = mapReader.readValue(value);
                returnValue = new LogData(jsonMap);
            } catch (final IOException e) {
            }
            return returnValue;
        });

        final KStream<String, LogData> logDataByNodeName = logDataStream.filter(notNullLogData).map(
            (key, logData) -> {
                final String nodeNameKey = logData.nodeName;
                return KeyValue.pair(nodeNameKey, logData);
            }).through("node-name-repartition", Produced.with(stringSerde, logDataSerde));

        final KGroupedStream<String, LogData> logDataKGroupedStream = logDataByNodeName
            .groupByKey(Grouped.with(stringSerde, logDataSerde));

        final KTable<Windowed<String>, Long> logData10MinuteWindowCount = logDataKGroupedStream
            .windowedBy(TimeWindows.of(Duration.ofMinutes(10)).grace(Duration.ofMinutes(10)))
            .count(
                Materialized
                    .<String, Long, WindowStore<Bytes, byte[]>>with(stringSerde, longSerde)
                    .withLoggingEnabled(changeLogConfigs)
                    .withRetention(Duration.ofHours(4))
            );

        logData10MinuteWindowCount
            .toStream()
            .map(windowMapper)
            .to(windowedTopic, Produced.with(stringSerde, longSerde));

        logDataKGroupedStream
            .windowedBy(TimeWindows.of(Duration.ofMinutes(45)))
            .count(
                Materialized
                    .<String, Long, WindowStore<Bytes, byte[]>>with(stringSerde, longSerde)
                    .withLoggingEnabled(changeLogConfigs)
            )
            .toStream()
            .map(windowMapper)
            .to(windowedTopic, Produced.with(stringSerde, longSerde));

        logDataKGroupedStream
            .windowedBy(TimeWindows.of(Duration.ofMinutes(2)).advanceBy(Duration.ofSeconds(15)))
            .count(
                Materialized
                    .<String, Long, WindowStore<Bytes, byte[]>>with(stringSerde, longSerde)
                    .withLoggingEnabled(changeLogConfigs)
            )
            .toStream()
            .map(windowMapper)
            .to(windowedTopic, Produced.with(stringSerde, longSerde));

        logDataKGroupedStream
            .windowedBy(SessionWindows.with(Duration.ofMillis(5)))
            .count(
                Materialized
                    .<String, Long, SessionStore<Bytes, byte[]>>with(stringSerde, longSerde)
                    .withLoggingEnabled(changeLogConfigs)
            )
            .toStream()
            .map(windowMapper)
            .to(windowedTopic, Produced.with(stringSerde, longSerde));

        logDataKGroupedStream
            .windowedBy(SlidingWindows.withTimeDifferenceAndGrace(Duration.ofMinutes(10), Duration.ofMinutes(30)))
            .count(
                Materialized
                    .<String, Long, WindowStore<Bytes, byte[]>>with(stringSerde, longSerde)
                    .withLoggingEnabled(changeLogConfigs)
            )
            .toStream()
            .map(windowMapper)
            .to(windowedTopic, Produced.with(stringSerde, longSerde));

        final KTable<String, Long> windowedCountTable = builder.table(windowedTopic, Consumed.with(stringSerde, longSerde));

        logDataByNodeName.join(windowedCountTable,
            (ld, l) -> ld.nodeName + " : " + l,
            Joined.with(stringSerde, logDataSerde, longSerde))
            .to("joined-counts", Produced.with(stringSerde, stringSerde));

        final KStream<String, LogData> logDataByNetworkId = logDataStream.filter(notNullLogData).selectKey(
            (key, logData) -> logData.networkId).through("network-id-repartition", Produced.with(stringSerde, logDataSerde));

        logDataByNetworkId.groupByKey(Grouped.with(stringSerde, logDataSerde))
            .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>with(stringSerde, longSerde)
                .withLoggingEnabled(changeLogConfigs))
            .toStream().to("network-id-counts", Produced.with(stringSerde, longSerde));

        final KStream<String, LogData> logDataByHostId = logDataStream.filter(notNullLogData).selectKey(
            (key, logData) -> logData.k8sName).through("k8sName-id-repartition", Produced.with(stringSerde, logDataSerde));

        logDataByHostId.groupByKey(Grouped.with(stringSerde, logDataSerde))
            .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>with(stringSerde, longSerde)
                .withLoggingEnabled(changeLogConfigs))
            .toStream().to("k8sName-counts", Produced.with(stringSerde, longSerde));

        logDataStream.filter(notNullLogData).mapValues(logData -> {
            final String timestamp = logData.timestamp;
            final String source = logData.source;
            return timestamp + source;
        }).to("streams-soak-out", Produced.with(stringSerde, stringSerde));

        logData10MinuteWindowCount
            .suppress(untilWindowCloses(unbounded()).withName("logData10MinuteFinalCount"))
            .toStream()
            .map(windowMapper)
            .to(windowedTopic, Produced.with(stringSerde, longSerde));

        logData10MinuteWindowCount
            .suppress(untilTimeLimit(Duration.ofMinutes(5), unbounded()).withName("logData10MinuteSuppressedCount"))
            .toStream()
            .map(windowMapper)
            .to(windowedTopic, Produced.with(stringSerde, longSerde));

        return builder.build();
    }

    private static class LogDataPredicate implements Predicate<String, LogData> {

        @Override
        public boolean test(final String s, final LogData logData) {
            return null != logData;
        }
    }
}
