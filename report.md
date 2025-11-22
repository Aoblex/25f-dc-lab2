# Lab2

小组成员:

-   王晨瑞, 2025104213

-   余锐琦, 2025104231

## 环境配置

### 申请节点

首先选择 `Python 3.12` 环境，并申请一个节点。

![node-selection](./figures/node-selection.png)

### 制作lab2镜像


#### 初始化

进入镜像后，首先执行初始化程序。

```bash
bash /course487/software/init.sh

source /etc/profile

bash /course487/software/init_spark.sh
```

#### python 依赖

然后安装 `pyarrow` 包:

```bash
pip install pyarrow
```

#### spark 配置

调整 `spark-defaults.conf`:

```conf
# $SPARK_HOME/conf/spark-defaults.conf
spark.eventLog.enabled true
spark.eventLog.dir hdfs:///spark-logs        # 作业提交时使用的 event log 目录
spark.history.fs.logDirectory hdfs:///spark-logs
```

使用命令行将其写入配置文件:

```bash
cat > $SPARK_HOME/conf/spark-defaults.conf <<EOF
spark.eventLog.enabled           true
spark.eventLog.dir               hdfs:///spark-logs
spark.history.fs.logDirectory    hdfs:///spark-logs
EOF
```

#### 项目仓库

下载 `lab2` 仓库:

```bash
git clone https://github.com/Aoblex/25f-dc-lab2 ~/github/lab2
```

然后下载数据:

```bash
make download-dataset
```

#### 操作汇总

```bash
# initialization
bash /course487/software/init.sh
source /etc/profile
bash /course487/software/init_spark.sh

# python dependencies
pip install pyarrow

# spark configuration
cat > $SPARK_HOME/conf/spark-defaults.conf <<EOF
spark.eventLog.enabled           true
spark.eventLog.dir               hdfs:///spark-logs
spark.history.fs.logDirectory    hdfs:///spark-logs
EOF

# git repository download and data download
git clone https://github.com/Aoblex/25f-dc-lab2 ~/github/lab2
cd ~/github/lab2/ && make download-dataset

```

#### 保存

操作完成之后将镜像保存到个人环境中:

![save-env](./figures/save-env.png)

### 配置集群

使用刚才制作好的镜像申请三个节点，然后将其添加到集群中，并设置 `master node`。

![cluster](./figures/cluster.png)

设置主节点的 `/etc/hosts/`:

```
127.0.0.1       localhost
::1     localhost ip6-localhost ip6-loopback
fe00::0 ip6-localnet
ff00::0 ip6-mcastprefix
ff02::1 ip6-allnodes
ff02::2 ip6-allrouters
172.17.0.3      P4gn2j

192.168.1.30 P4gn2j
192.168.1.31 wJvpz0
192.168.1.32 qj76ar

192.168.1.30 node1
192.168.1.31 node2
192.168.1.32 node3
```

修改 `~/.ssh/config` 文件，把第一行注释掉:

```
# Host 192.168.1.*
IdentityFile /root/.ssh/id_rsa_6ea19af88bcc
```

然后将 `/etc/hosts` 复制到另外两个节点:

```bash
scp /etc/hosts node2:/etc/hosts
scp /etc/hosts node3:/etc/hosts
```

接下来可以初始化 hadoop 并添加 spark log 目录:

```bash
hdfs namenode -format
hdfs dfs -mkdir /sprak-logs
```

然后启动所有节点:

```bash
start-all.sh
```

并启动 `jobhistory server`:

```bash
$SPARK_HOME/sbin/start-history-server.sh
```

汇总(ssh配置好后可运行):

```bash
scp /etc/hosts node2:/etc/hosts
scp /etc/hosts node3:/etc/hosts

# spark configuration
cat > $SPARK_HOME/conf/spark-defaults.conf <<EOF
spark.eventLog.enabled           true
spark.eventLog.dir               hdfs:///spark-logs
spark.history.fs.logDirectory    hdfs:///spark-logs
EOF

hdfs namenode -format
hdfs dfs -mkdir /spark-logs

start-all.sh
$SPARK_HOME/sbin/start-history-server.sh
```

另外，在本地使用web ui查看job history:

```bash
ssh -N -L 18080:localhost:18080 -p 28521 root@hz-4.matpool.com
```

然后就可以在[本地查看](http://localhost:18080)任务记录。

## 实验运行

进入实验文件夹:

```bash
cd ~/github/lab2
```

运行测试任务:

```bash
make test
```

然后可以[查看](http://localhost:18080)刚才执行完成的任务:

![test-result](./figures/test-result.png)