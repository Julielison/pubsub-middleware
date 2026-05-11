package br.edu.ifpb.pubsub.examples

import br.edu.ifpb.pubsub.client.PubSubClient

/**
 * PUBLICADOR 1: Estação Meteorológica
 * Publica dados dos tópicos:
 *   - "temperatura"  → leituras de temperatura
 *   - "umidade"      → leituras de umidade
 */
class SensorPublisher {

    static void run(List<String> brokers) {
        println "\n[SensorPublisher] Iniciando estação meteorológica..."
        PubSubClient client = new PubSubClient(brokers)
        client.connect()
        sleep(500)

        Random rnd = new Random()
        int ciclos = 5

        ciclos.times { i ->
            double temp = 20.0 + rnd.nextDouble() * 15.0
            double umid = 40.0 + rnd.nextDouble() * 50.0

            client.publish("temperatura", [
                valor   : Math.round(temp * 10) / 10.0,
                unidade : "°C",
                estacao : "Estação-01",
                timestamp: new Date().toString()
            ])

            client.publish("umidade", [
                valor   : Math.round(umid * 10) / 10.0,
                unidade : "%",
                estacao : "Estação-01",
                timestamp: new Date().toString()
            ])

            sleep(1000)
        }

        // Testa publicar em tópico sem inscritos
        client.publish("sem_inscritos_teste", [dados: "isso será descartado"])
        sleep(500)

        client.disconnect()
        println "[SensorPublisher] Encerrado."
    }
}
