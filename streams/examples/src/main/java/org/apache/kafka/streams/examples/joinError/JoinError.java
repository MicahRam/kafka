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
package org.apache.kafka.streams.examples.joinError;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.Stores;
import org.rocksdb.CompressionType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


/**
 * Before running this example you must create the input topics in_a in_b, in_c, in_d  and the output topic "output" (e.g. via
 * bin/kafka-topics.sh --create ...), and write some data to the input topic
 */
public class JoinError {

    private static final String INPUT_TOPIC_TABLE_A = "in_a";
    private static final String INPUT_TOPIC_TABLE_B = "in_b";
    private static final String INPUT_TOPIC_TABLE_C = "in_c";
    private static final String INPUT_TOPIC_TABLE_D = "in_d";

    private static final String STORE_NAME_A = "store_a";
    private static final String STORE_NAME_B = "store_b";
    private static final String STORE_NAME_C = "store_c";
    private static final String STORE_NAME_D = "store_d";

    private static int count = 0;

    private Map<String, String> loggingconfig = new HashMap<>();

    public static void main(final String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "joinError");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder builder = new StreamsBuilder();

        KTable<String, String> tableA = builder.table(INPUT_TOPIC_TABLE_A,
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.<String, String>as(Stores.persistentKeyValueStore(STORE_NAME_A))
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
                .withCachingEnabled()
            .withLoggingEnabled(Collections.singletonMap(TopicConfig.COMPRESSION_TYPE_CONFIG, CompressionType.ZSTD_COMPRESSION.getLibraryName()))
        );
        KTable<String, String> tableB = builder.table(INPUT_TOPIC_TABLE_B,
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.<String, String>as(Stores.persistentKeyValueStore(STORE_NAME_B))
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
                .withCachingEnabled()
                .withLoggingEnabled(Collections.singletonMap(TopicConfig.COMPRESSION_TYPE_CONFIG, CompressionType.ZSTD_COMPRESSION.getLibraryName()))
        );
        KTable<String, String> tableC = builder.table(INPUT_TOPIC_TABLE_C,
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.<String, String>as(Stores.persistentKeyValueStore(STORE_NAME_C))
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
                .withCachingEnabled()
                .withLoggingEnabled(Collections.singletonMap(TopicConfig.COMPRESSION_TYPE_CONFIG, CompressionType.ZSTD_COMPRESSION.getLibraryName()))
        );
        KTable<String, String> tableD = builder.table(INPUT_TOPIC_TABLE_D,
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.<String, String>as(Stores.persistentKeyValueStore(STORE_NAME_D))
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
                .withCachingEnabled()
                .withLoggingEnabled(Collections.singletonMap(TopicConfig.COMPRESSION_TYPE_CONFIG, CompressionType.ZSTD_COMPRESSION.getLibraryName()))
        );

        tableA.leftJoin(tableB, JoinError::joinB)
            .leftJoin(tableC, JoinError::joinC)
            .leftJoin(tableD, JoinError::joinD)
            .toStream()
            .peek((k, v) -> System.out.println((count++) + "-----------------------"))
            .peek((k, v) -> System.out.println("K:" + k + "\n" + v))
            .to("output");

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("joinError-shutdown-hook") {
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

    private static String joinB(String orig, String b) {

        return "A = " + nullSafe(orig) + "\n B = " + nullSafe(b);
    }

    private static String joinC(String orig, String c) {
        return nullSafe(orig) + "\n C = " + nullSafe(c);
    }

    private static String joinD(String orig, String d) {
        return nullSafe(orig) + "\n D = " + nullSafe(d);
    }

    private static String nullSafe(String possibleNull) {
        return possibleNull == null ? "null" : possibleNull;
    }

}
