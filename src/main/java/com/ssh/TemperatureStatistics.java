package com.ssh;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.windowing.TimeWindow;

import org.joda.time.Duration;

/**
 * @author admin
 * @version 1.0.0
 * @ClassName TemperatureStatistics.java
 * @Description TODO
 * @createTime 2022年12月19日 15:09:00
 */
public class TemperatureStatistics {
    public static void main(String[] args) {
        // 创建管道选项
        PipelineOptions options = PipelineOptionsFactory.create();

        // 创建管道
        Pipeline pipeline = Pipeline.create(options);

        // 读取输入文件
        PCollection<String> input = pipeline.apply(TextIO.read().from("input.txt"));

        // 解析输入数据并提取温度和时间戳
        PCollection<KV<Long, Float>> data = input.apply(ParDo.of(new DoFn<String, KV<Long, Float>>() {
            @ProcessElement
            public void processElement(@Element String element, OutputReceiver<KV<Long, Float>> out) {
                String[] parts = element.split(",");
                long timestamp = Long.parseLong(parts[0]);
                float temperature = Float.parseFloat(parts[1]);
                out.output(KV.of(timestamp, temperature));
            }
        }));
        // 为数据分配时间戳，并将数据分成1分钟、5分钟和15分钟的窗口
        PCollection<KV<Long, Float>> dataWithTimestamps = data.apply(WithTimestamps.of((KV<Long, Float> kv) -> kv.getKey()));

        PCollection<KV<Long, Float>> dataIn1MinuteWindows = dataWithTimestamps.apply(Window.into(new TimeWindow.Unbounded<KV<Long, Float>>().withOutputTimeFn((TimeWindow w, Trigger.TriggerContext c) -> w.maxTimestamp())));
        PCollection<KV<Long, Float>> dataIn5MinuteWindows = dataWithTimestamps.apply(Window.into(new TimeWindow.Unbounded<KV<Long, Float>>().withOutputTimeFn((TimeWindow w, Trigger.TriggerContext c) -> w.maxTimestamp()).withAllowedLateness(Duration.standardMinutes(5))).triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(5)))).discardingFiredPanes());
        PCollection<KV<Long, Float>> dataIn15MinuteWindows = dataWithTimestamps.apply(Window.into(new TimeWindow.Unbounded<KV<Long, Float>>().withOutputTimeFn((TimeWindow w, Trigger.TriggerContext c) -> w.maxTimestamp()).withAllowedLateness(Duration.standardMinutes(15))).triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(15)))).discardingFiredPanes());

// 计算每个时间窗口的统计信息
        PCollection<String> statistics1Minute = dataIn1MinuteWindows.apply(Combine.perKey(new StatisticsFn())).apply(MapElements.via((KV<Long, Statistics> kv) -> String.format("1 minute, %d, %.2f, %.2f, %.2f", kv.getKey(), kv.getValue().getMax(), kv.getValue().getMin(), kv.getValue().getAvg())));
        PCollection<String> statistics5Minute = dataIn5MinuteWindows.apply(Combine.perKey(new StatisticsFn())).apply(MapElements.via((KV<Long, Statistics> kv) -> String.format("5 minutes, %d, %.2f, %.2f, %.2f", kv.getKey(), kv.getValue().getMax(), kv.getValue().getMin(), kv.getValue().getAvg())));
        PCollection<String> statistics15Minute = dataIn15MinuteWindows.apply(Combine.perKey(new StatisticsFn())).apply(MapElements.via((KV<Long, Statistics> kv) -> String.format("15 minutes, %d, %.2f, %.2f, %.2f", kv.getKey(), kv.getValue().getMax(), kv.getValue().getMin(), kv.getValue().getAvg())));

// 发现异常点
        PCollection<KV<Long, Float>> abnormalPoints = data.apply(Filter.by((KV<Long, Float> kv) -> kv.getValue() < 50.0 || kv.getValue() > 100.0));

// 将结果写入输出位置
        statistics1Minute.apply(TextIO.write().to("1_minutestatistics.txt"));
        statistics5Minute.apply(TextIO.write().to("5_minute_statistics.txt"));
        statistics15Minute.apply(TextIO.write().to("15_minute_statistics.txt"));
        abnormalPoints.apply(TextIO.write().to("abnormal_points.txt"));
        // 运行管道
        pipeline.run();
    }


}
// Combine函数用于计算统计信息

class StatisticsFn extends Combine.CombineFn<KV<Long, Float>, Statistics, Statistics> {
    @Override
    public Statistics createAccumulator() {
        return new Statistics();
    }

    @Override
    public Statistics addInput(Statistics accumulator, KV<Long, Float> input) {
        accumulator.add(input.getValue());
        return accumulator;
    }

    @Override
    public Statistics mergeAccumulators(Iterable<Statistics> accumulators) {
        Statistics merged = createAccumulator();
        for (Statistics accumulator : accumulators) {
            merged.merge(accumulator);
        }
        return merged;
    }

    @Override
    public Statistics extractOutput(Statistics accumulator) {
        return accumulator;
    }
}

// 用于存储时间窗口统计信息的类
class Statistics {
    private float min = Float.MAX_VALUE;
    private float max = Float.MIN_VALUE;
    private float sum = 0.0f;
    private long count = 0;

    public void add(float value) {
        min = Math.min(min, value);
        max = Math.max(max, value);
        sum += value;
        count++;
    }

    public void merge(Statistics other) {
        min = Math.min(min, other.min);
        max = Math.max(max, other.max);
        sum += other.sum;
        count += other.count;
    }

    public float getMin() {
        return min;
    }

    public float getMax() {
        return max;
    }

    public float getAvg() {
        return sum / count;
    }
}
