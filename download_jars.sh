#!/bin/bash

# Cria o diretório de destino
mkdir -p pipe/layers/config/spark/jars/

# Baixa cada JAR
echo "⬇️ Baixando JARs necessários..."

curl -L -o pipe/layers/config/spark/jars/aws-java-sdk-bundle-1.12.426.jar \
  https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/1.12.426/bundle-1.12.426.jar

curl -L -o pipe/layers/config/spark/jars/delta-core_2.12-2.2.0.jar \
  https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.2.0/delta-core_2.12-2.2.0.jar

curl -L -o pipe/layers/config/spark/jars/delta-spark_2.12-3.2.0.jar \
  https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar

curl -L -o pipe/layers/config/spark/jars/hadoop-aws-3.3.4.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

curl -L -o pipe/layers/config/spark/jars/delta-storage-3.2.0.jar \
  https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar

echo "✅ Todos os JARs foram baixados com sucesso."
