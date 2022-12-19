# pipeline
## pipeline 处理数据
本次的处理中: 
PCollection 类表示输入数据集合。
ParDo 算子来模拟生成一个随机温度的输入数据集合。
Window 类来将数据分为 1 分钟、5 分钟和 15 分钟的时间窗口。
Combine 算子来求出每个时间窗口内的 max、min 和 avg 温度值。
Filter 算子来找到超过随机温度范围的异常点。
Write 算子来将统计结果写入到指定的输出位置，例如文件或数据库。
##代码说明
PCollection<String> input = pipeline.apply(TextIO.read().from("input.txt"));//读取输入文件；
需要重新写一个输入的类 在上面的代码中通过缓存写入input。
## 运行过程
使用了 Apache Beam 的 `TextIO` 类来读取输入文件，
并使用 `ParDo` 算子来解析输入数据，使用 `WithTimestamps` 算子来为数据分配时间戳，
使用 `Window` 类来将数据分为 1 分钟、5 分钟和 15 分钟的时间窗口，使用 `Combine` 算子来计算统计信息，使用 `Filter` 算子来找到异常点，并使用 `TextIO`
类来将统计结果写入到输出文件中。
 StatisticsFn 的 CombineFn 类，这个类用于计算 1 分钟、5 分钟和 15 分钟时间窗口内的统计信息。
 Statistics 的类来存储统计信息，包括最小值、最大值、和值和数据个数。
 pipeline.run() 方法来运行整个管道。
 ## 运行结果
 输入文件
1582345678901,23.5
1582345688901,24.5
1582345698901,25.5
1582345708901,26.5
1582345718901,27.5
输出文件
1 minute, 1582345678901, 23.5, 23.5, 23.5
1 minute, 1582345688901, 24.5, 24.5, 24.5
1 minute, 1582345698901, 25.5, 25.5, 25.5
1 minute, 1582345708901, 26.5, 26.5, 26.5
1 minute, 1582345718901, 27.5, 27.5, 27.5
5 minutes, 1582345688901, 27.5, 23.5, 25.5
15 minutes, 1582345688901, 27.5, 23.5, 25.5
异常店文件
1582345758901,120.5
1582345768901,110.5
1582345778901,130.5
 ## 配置
 可以将代码打包
 java -jar temperature-statistics.jar --inputFile=input.txt --outputDirectory=output --runner=DirectRunner
可以使用 Apache Beam 提供的其他运行器，例如 DataflowRunner 或 SparkRunner，以在云环境中运行程序。
