# flume-custom-extend
## 概述
- 自定义的一些flumesource或sink模块，可配合flume1.9使用。
- merge-dfsfile-name-sink中提供的sink可将数据以文本追加的方式写进hdfs的文件，且结果文件名就是数据的原文件名，来自相同文件名的数据会被汇总到一个文件，然后按天滚动目录。
- merge-file-name-sink中提供的sink可将数据汇聚到本地文件，且结果文件名就是数据的原文件名，来自相同文件名的数据会被汇总到一个文件，然后按天滚动目录。
- tail-dir-include-child-source是在原有flume1.9的TaildirSource基础上改的，在保证原有功能和配置不变的前提下，增加了监听子目录的功能。配置中需要监听的文件正则可监听目标目录或目标目录的所有子目录，以及后续新生成的各层子目录。

## 部署
1. 需要搭建好flume1.9。
2. 然后将需要的组件maven打包出jar文件，将jar文件放入. /apache-flume-1.9.0-bin/lib/目录。
3. 配置文件中的sink或source需要指向自定义的类上。