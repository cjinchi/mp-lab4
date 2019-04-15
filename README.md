# 大数据处理实验3

## TODO

### 必做部分

- 课本中还在每个term索引的末端输出了其在全部文档中的提及次数之和

- 实验要求中的“平均提及次数”，这一点需要在上面这一点完成后再做

- 【实验报告】实验设计说明
    1. 主要设计思路
    2. 算法设计
    3. 程序和各个类设计说明 

- 【实验报告】程序运行和实验结果说明和分析
    1.程序运行和实验结果说明
    2.分析

- 【实验报告】性能、扩展性等方面可能存在的不足和可能的改进之处 

### 拓展部分

- 词干提取（第五章PPT第66页的第1点，考虑用[Lucene的PorterStemmer](https://lucene.apache.org/core/7_6_0/analyzers-common/org/tartarus/snowball/ext/PorterStemmer.html)实现）

- 去stopwords（第五章PPT第66页的第2点，课本里也有提及，考虑用课本的方法或者[Lucene的StopFilter](http://lucene.apache.org/core/7_6_0/core/org/apache/lucene/analysis/StopFilter.html)实现）

- ...

## 运行
假设已经将数据文件放在HDFS中的`/test-in`文件夹内，执行以下命令：

`bin/hadoop jar mp-lab3.jar app.InvertedIndex /test-in /test-out`

其中：
- `mp-lar3.jar` 为生成的jar文件
- `/test-in` 为HDFS中数据所在文件夹的路径
