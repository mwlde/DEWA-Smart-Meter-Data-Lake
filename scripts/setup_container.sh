#!/bin/bash
#one-time setup of the project tools inside the hadoop container:
#hive 3.1.3, spark 3.5, kafka 3.9 and hbase 2.5, all on java 8.
#
#before running this, install java 8 as root. in the mac terminal:
#  docker exec -u root hadoop bash -c "apt-get update && apt-get install -y openjdk-8-jdk-headless curl python3"
#(not needed with the Dockerfile image from SETUP.md: it already has java 8)
#
#then inside the container (as user hadoop):
#  bash /home/hadoop/dewa/scripts/setup_container.sh
#
#safe to run again: anything already installed is skipped.
#if the container is ever lost, recreate it and run both steps again.
set -eE
trap 'echo; echo "FAILED in setup_container.sh at line $LINENO: $BASH_COMMAND"' ERR

#versions (change here if a download is not found)
HIVE_VERSION=3.1.3
SPARK_VERSION=3.5.6
KAFKA_VERSION=3.9.1
SCALA_VERSION=2.13
HBASE_VERSION=2.5.10
MIRROR=https://archive.apache.org/dist

say() { echo; echo "=== $1"; }

#---------------------------------------------------------------- java 8
J8=$(ls -d /usr/lib/jvm/java-8-openjdk-* 2>/dev/null | head -1)
if [ -z "$J8" ]; then
  echo "Java 8 is not installed. Run the root command at the top of this script first."
  exit 1
fi
say "Java 8 found at $J8"

#point hadoop at java 8 (hive 3 fails on java 11)
HENV=$HADOOP_HOME/etc/hadoop/hadoop-env.sh
if [ -z "$HADOOP_HOME" ]; then HADOOP_HOME=/home/hadoop/hadoop; HENV=$HADOOP_HOME/etc/hadoop/hadoop-env.sh; fi
sed -i '/^export JAVA_HOME=/d' "$HENV"
echo "export JAVA_HOME=$J8" >> "$HENV"
export JAVA_HOME=$J8

#---------------------------------------------------------------- downloads
mkdir -p ~/downloads
fetch() {  #fetch <url> <install-dir-name>
  local url=$1 name=$2 file=~/downloads/$(basename "$1")
  if [ -d ~/"$name" ]; then echo "$name already installed, skipping"; return; fi
  if [ ! -f "$file" ]; then
    echo "Downloading $(basename "$url") (can take a few minutes)..."
    if ! curl -fL --retry 3 -o "$file.part" "$url"; then
      rm -f "$file.part"
      echo
      echo "Download failed: $url"
      echo "The version may have moved. Check $MIRROR for the folder and change the version at the top of this script."
      exit 1
    fi
    mv "$file.part" "$file"
  fi
  mkdir -p ~/"$name"
  tar -xzf "$file" -C ~/"$name" --strip-components=1
  echo "$name installed"
}

say "Hive $HIVE_VERSION"
fetch $MIRROR/hive/hive-$HIVE_VERSION/apache-hive-$HIVE_VERSION-bin.tar.gz hive
say "Spark $SPARK_VERSION"
fetch $MIRROR/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz spark
say "Kafka $KAFKA_VERSION"
fetch $MIRROR/kafka/$KAFKA_VERSION/kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz kafka
say "HBase $HBASE_VERSION"
fetch $MIRROR/hbase/$HBASE_VERSION/hbase-$HBASE_VERSION-hadoop3-bin.tar.gz hbase

#---------------------------------------------------------------- environment
if ! grep -q "DEWA project tools" ~/.bashrc; then
  cat >> ~/.bashrc <<EOF

# DEWA project tools (added by setup_container.sh)
export JAVA_HOME=$J8
export HADOOP_HOME=$HADOOP_HOME
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
export HIVE_HOME=/home/hadoop/hive
export HIVE_CONF_DIR=\$HIVE_HOME/conf
export SPARK_HOME=/home/hadoop/spark
export KAFKA_HOME=/home/hadoop/kafka
export HBASE_HOME=/home/hadoop/hbase
export PATH=\$JAVA_HOME/bin:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$HIVE_HOME/bin:\$SPARK_HOME/bin:\$KAFKA_HOME/bin:\$HBASE_HOME/bin:\$PATH
EOF
fi
export HIVE_HOME=/home/hadoop/hive SPARK_HOME=/home/hadoop/spark
export KAFKA_HOME=/home/hadoop/kafka HBASE_HOME=/home/hadoop/hbase
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HIVE_HOME/bin:$SPARK_HOME/bin:$KAFKA_HOME/bin:$HBASE_HOME/bin:$PATH

#---------------------------------------------------------------- hive config
say "Configuring Hive"
#guava clash between hive 3 and hadoop 3 (nosuchmethoderror on start-up)
rm -f $HIVE_HOME/lib/guava-19.0.jar
cp -n $HADOOP_HOME/share/hadoop/common/lib/guava-*.jar $HIVE_HOME/lib/ 2>/dev/null || true

cat > $HIVE_HOME/conf/hive-site.xml <<'EOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:derby:;databaseName=/home/hadoop/hive/metastore_db;create=true</value>
  </property>
  <property><name>hive.metastore.warehouse.dir</name><value>/user/hive/warehouse</value></property>
  <property><name>hive.server2.enable.doAs</name><value>false</value></property>
  <property><name>hive.metastore.event.db.notification.api.auth</name><value>false</value></property>
  <property><name>hive.stats.autogather</name><value>false</value></property>
  <property><name>hive.stats.column.autogather</name><value>false</value></property>
  <property><name>hive.cli.print.header</name><value>true</value></property>
</configuration>
EOF

#---------------------------------------------------------------- spark config
say "Configuring Spark"
cat > $SPARK_HOME/conf/spark-env.sh <<EOF
export JAVA_HOME=$J8
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
EOF
cp -n $SPARK_HOME/conf/log4j2.properties.template $SPARK_HOME/conf/log4j2.properties 2>/dev/null || true
sed -i 's/^rootLogger.level = info/rootLogger.level = warn/' $SPARK_HOME/conf/log4j2.properties 2>/dev/null || true

#---------------------------------------------------------------- kafka config
say "Configuring Kafka (single node, KRaft mode, no ZooKeeper)"
if [ ! -f /home/hadoop/kafka-data/meta.properties ]; then
  sed -i 's#^log.dirs=.*#log.dirs=/home/hadoop/kafka-data#' $KAFKA_HOME/config/kraft/server.properties
  KID=$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)
  $KAFKA_HOME/bin/kafka-storage.sh format -t "$KID" -c $KAFKA_HOME/config/kraft/server.properties
else
  echo "Kafka storage already formatted, skipping"
fi

#---------------------------------------------------------------- hbase config
say "Configuring HBase (standalone mode)"
#added once only (re-runs used to append the same line again each time)
grep -q "^export JAVA_HOME=$J8" $HBASE_HOME/conf/hbase-env.sh || echo "export JAVA_HOME=$J8" >> $HBASE_HOME/conf/hbase-env.sh
cat > $HBASE_HOME/conf/hbase-site.xml <<'EOF'
<?xml version="1.0"?>
<configuration>
  <property><name>hbase.cluster.distributed</name><value>false</value></property>
  <property><name>hbase.rootdir</name><value>file:///home/hadoop/hbase-data</value></property>
  <property><name>hbase.zookeeper.property.dataDir</name><value>/home/hadoop/hbase-zk</value></property>
  <property><name>hbase.unsafe.stream.capability.enforce</name><value>false</value></property>
</configuration>
EOF

#---------------------------------------------------------------- hive metastore
say "Creating Hive folders in HDFS and the metastore"
hdfs dfs -mkdir -p /tmp /user/hive/warehouse
hdfs dfs -chmod g+w /tmp /user/hive/warehouse
if [ ! -d /home/hadoop/hive/metastore_db ]; then
  (cd ~ && schematool -dbType derby -initSchema)
else
  echo "Metastore already exists, skipping"
fi

say "Setup finished"
echo "Next:"
echo "  1. exit, then in the Mac terminal: docker restart hadoop   (Hadoop restarts on Java 8)"
echo "  2. docker exec -it hadoop bash"
echo "  3. bash /home/hadoop/dewa/scripts/check_tools.sh"
