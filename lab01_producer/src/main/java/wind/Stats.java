/*
 * Inspired by org.apache.kafka.tools.ProducerPerformance.Stats
 *
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
package wind;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Inspired by org.apache.kafka.tools.ProducerPerformance.Stats
 */
public class Stats {
    private long start;
    private long windowStart;
    private List<Integer> latencies;
    private long sampling;
    private long iteration;
    private int index;
    private long count;
    private long bytes;
    private int maxLatency;
    private long totalLatency;
    private long windowCount;
    private int windowMaxLatency;
    private long windowTotalLatency;
    private long windowBytes;
    private long reportingInterval;

    public Stats(int reportingInterval) {
        this.start = System.currentTimeMillis();
        this.windowStart = System.currentTimeMillis();
        this.iteration = 0;
        this.sampling = 50000;
        this.latencies = new ArrayList<>();
        this.index = 0;
        this.maxLatency = 0;
        this.windowCount = 0;
        this.windowMaxLatency = 0;
        this.windowTotalLatency = 0;
        this.windowBytes = 0;
        this.totalLatency = 0;
        this.reportingInterval = reportingInterval;
    }

    private static int[] percentiles(List<Integer> latencies, int count, double... percentiles) {
        int size = Math.min(count, latencies.size());
        Collections.sort(latencies);
        int[] values = new int[percentiles.length];
        for (int i = 0; i < percentiles.length; i++) {
            int index = (int) (percentiles[i] * size);
            values[i] = latencies.get(index);
        }
        return values;
    }

    public void record(long iter, int latency, int bytes, long time) {
        this.count++;
        this.bytes += bytes;
        this.totalLatency += latency;
        this.maxLatency = Math.max(this.maxLatency, latency);
        this.windowCount++;
        this.windowBytes += bytes;
        this.windowTotalLatency += latency;
        this.windowMaxLatency = Math.max(windowMaxLatency, latency);
        if (iter % this.sampling == 0) {
            this.latencies.add(latency);
            this.index++;
        }
        /* maybe report the recent perf */
        if (time - windowStart >= reportingInterval) {
            printWindow();
            newWindow();
        }
    }

    public Callback nextCompletion(long start, Stats stats) {
        Callback cb = new PerfCallback(this.iteration, start, stats);
        this.iteration++;
        return cb;
    }

    public void printWindow() {
        long elapsed = System.currentTimeMillis() - windowStart;
        double recsPerSec = 1000.0 * windowCount / (double) elapsed;
        double mbPerSec = 1000.0 * this.windowBytes / (double) elapsed / (1024.0 * 1024.0);
        System.out.printf("%d records sent, %.1f records/sec (%.2f MB/sec), %.1f ms avg latency, %.1f ms max latency.%n",
                windowCount,
                recsPerSec,
                mbPerSec,
                windowTotalLatency / (double) windowCount,
                (double) windowMaxLatency);
    }

    public void newWindow() {
        this.windowStart = System.currentTimeMillis();
        this.windowCount = 0;
        this.windowMaxLatency = 0;
        this.windowTotalLatency = 0;
        this.windowBytes = 0;
    }

    public void printTotal() {
        long elapsed = System.currentTimeMillis() - start;
        if(elapsed==0) {
            elapsed=1; // avoid division by zero
        }
        double recsPerSec = 1000.0 * count / (double) elapsed;
        double mbPerSec = 1000.0 * this.bytes / (double) elapsed / (1024.0 * 1024.0);
        int[] percs = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
        System.out.printf("%d records sent, %f records/sec (%.2f MB/sec), %.2f ms avg latency, %.2f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.%n",
                count,
                recsPerSec,
                mbPerSec,
                totalLatency / (double) count,
                (double) maxLatency,
                percs[0],
                percs[1],
                percs[2],
                percs[3]);
    }

    class PerfCallback implements Callback {
        private final long start;
        private final long iteration;
        private final Stats stats;

        public PerfCallback(long iter, long start, Stats stats) {
            this.start = start;
            this.stats = stats;
            this.iteration = iter;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long now = System.currentTimeMillis();
            int latency = (int) (now - start);
            this.stats.record(iteration, latency, metadata.serializedKeySize() + metadata.serializedValueSize(), now);
            if (exception != null)
                exception.printStackTrace();
        }

    }
}
