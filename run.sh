#!/bin/bash
# Script de execucao do middleware PubSub (sem Gradle instalado)
# Usa groovyc + java diretamente

set -e
GROOVY_CP=$(find /usr/share/groovy -name "*.jar" 2>/dev/null | tr '\n' ':')
if [ -z "$GROOVY_CP" ]; then
    echo "Groovy nao encontrado. Instale com: sudo apt install groovy"
    exit 1
fi

echo "=== Compilando modulos ==="
mkdir -p /tmp/pubsub-run/{broker,client,examples}

groovyc -d /tmp/pubsub-run/broker \
  broker/src/main/groovy/br/edu/ifpb/pubsub/broker/*.groovy

groovyc -d /tmp/pubsub-run/client \
  client-lib/src/main/groovy/br/edu/ifpb/pubsub/client/*.groovy

groovyc -cp /tmp/pubsub-run/broker:/tmp/pubsub-run/client \
  -d /tmp/pubsub-run/examples \
  example-apps/src/main/groovy/br/edu/ifpb/pubsub/examples/*.groovy

echo "=== Compilacao concluida. Iniciando demo ==="
CP="/tmp/pubsub-run/broker:/tmp/pubsub-run/client:/tmp/pubsub-run/examples:$GROOVY_CP"
java -cp "$CP" br.edu.ifpb.pubsub.examples.RunAllExamples
