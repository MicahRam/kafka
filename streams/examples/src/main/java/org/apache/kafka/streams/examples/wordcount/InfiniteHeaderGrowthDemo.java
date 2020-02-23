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
package org.apache.kafka.streams.examples.wordcount;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public final class InfiniteHeaderGrowthDemo {


    static class AddHeaderProcessorSupplier implements ProcessorSupplier<String, String> {
        private static AtomicInteger globalCounter = new AtomicInteger();

        @Override
        public Processor<String, String> get() {
            return new Processor<String, String>() {
                private ProcessorContext context;
                private KeyValueStore<String, Integer> kvStore;

                @Override
                @SuppressWarnings("unchecked")
                public void init(final ProcessorContext context) {
                    this.context = context;
                    this.context.schedule(Duration.ofMillis(500), PunctuationType.WALL_CLOCK_TIME, timestamp -> {

                        int punctuateIteration = globalCounter.getAndIncrement();
                        String headerValue = "Added from partition " + context.taskId().partition;
                        context.headers().add("" + punctuateIteration, Serdes.String().serializer().serialize("", headerValue));

                        StringBuilder stringBuilder = new StringBuilder();
                        stringBuilder.append("----------- punctuate count " + punctuateIteration + " ----------- \n");
                        stringBuilder.append("\t partition=" + context.taskId().partition + "\n");
                        stringBuilder.append("\t header size:" + context.headers().toArray().length + "\n");
                        System.out.println(stringBuilder.toString());

                        context.forward("dummy", "dummy");

                    });
                }

                @Override
                public void process(final String dummy, final String line) {
                }

                @Override
                public void close() {
                }
            };
        }
    }

    static class ReadHeaderProcessorSupplier implements ProcessorSupplier<String, String> {

        @Override
        public Processor<String, String> get() {
            return new Processor<String, String>() {
                private ProcessorContext context;
                private KeyValueStore<String, Integer> kvStore;

                @Override
                @SuppressWarnings("unchecked")
                public void init(final ProcessorContext context) {
                    this.context = context;
                }

                @Override
                public void process(final String dummy, final String line) {

                    List<Header> headers = Arrays.asList(context.headers().toArray());

                    StringBuilder stringBuilder = new StringBuilder();
                    stringBuilder.append("----------- Received from sink topic ----------- \n");
                    stringBuilder.append("\t partition=" + context.taskId().partition + "\n");
                    stringBuilder.append("\t header size:" + headers.size() + "\n");

                    headers.forEach(header -> {
                        String key = header.key();
                        String value = Serdes.String().deserializer().deserialize("", header.value());

                        stringBuilder.append("\t\t" + key + " : " + value + "\n");
                    });

                    System.out.println(stringBuilder.toString());
                }

                @Override
                public void close() {
                }
            };
        }
    }

    public static void main(final String[] args) {

        NewTopic sourceTopic = new NewTopic("input", 1, (short) 1);
        NewTopic sinkTopic = new NewTopic("output", 1, (short) 1);

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "micahDemo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Topology builder = new Topology();

        builder.addSource("Source", sourceTopic.name());
        builder.addProcessor("ProcessWrite", new AddHeaderProcessorSupplier(), "Source");
        builder.addSink("Sink", sinkTopic.name(), "ProcessWrite");

        builder.addSource("Sink2", sinkTopic.name());
        builder.addProcessor("ProcessRead", new ReadHeaderProcessorSupplier(), "Sink2");

        final KafkaStreams streams = new KafkaStreams(builder, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        AdminClient adminClient = KafkaAdminClient.create(props);
        adminClient.createTopics(Arrays.asList(sourceTopic, sinkTopic));

        try {
            System.out.println("Starting!");
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
            System.out.println(e.getMessage());
        }
        System.exit(0);
    }
}
