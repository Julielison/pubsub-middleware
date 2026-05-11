
import groovy.json.JsonOutput
import groovy.json.JsonSlurper

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * Biblioteca cliente para o middleware Pub/Sub.
 *
 * Provê transparência de balanceamento de carga:
 * o cliente não sabe com quantos brokers está se comunicando.
 *
 * Estratégia de balanceamento: Round-Robin por tópico.
 * Cada tópico é mapeado de forma determinística a um broker,
 * garantindo que mensagens de um mesmo tópico vão sempre ao mesmo broker.
 */
class PubSubClient {

    private final List<BrokerConnection> connections = []
    private final AtomicInteger roundRobinIndex = new AtomicInteger(0)

    // Mapeamento topic -> índice do broker (para consistência por tópico)
    private final Map<String, Integer> topicBrokerMap = new ConcurrentHashMap<>()

    // Callbacks de mensagem recebida: topic -> Closure
    private final Map<String, Closure> messageHandlers = new ConcurrentHashMap<>()

    /**
     * @param brokerAddresses lista de "host:porta" dos brokers disponíveis
     */
    PubSubClient(List<String> brokerAddresses) {
        brokerAddresses.each { address ->
            def (host, port) = address.split(':')
            connections.add(new BrokerConnection(host, port.toInteger(), this))
        }
        if (connections.isEmpty()) {
            throw new IllegalArgumentException("Nenhum endereço de broker fornecido.")
        }
    }

    /** Construtor de conveniência para um único broker */
    PubSubClient(String host, int port) {
        this(["${host}:${port}"])
    }

    /**
     * Conecta a todos os brokers disponíveis.
     */
    void connect() {
        connections.each { conn ->
            try {
                conn.connect()
                println "[PubSubClient] Conectado ao broker ${conn.address}"
            } catch (Exception e) {
                println "[PubSubClient] Não foi possível conectar a ${conn.address}: ${e.message}"
            }
        }
        ensureConnected()
    }

    private void ensureConnected() {
        boolean anyConnected = connections.any { it.connected }
        if (!anyConnected) {
            throw new RuntimeException("Nenhum broker disponível.")
        }
    }

    /**
     * Seleciona o broker para um tópico via hash (consistência por tópico).
     * Garante que subscribe e publish do mesmo tópico vão ao mesmo broker.
     */
    private BrokerConnection brokerForTopic(String topic) {
        int idx = topicBrokerMap.computeIfAbsent(topic) {
            Math.abs(topic.hashCode()) % connections.size()
        }
        BrokerConnection conn = connections[idx]
        if (!conn.connected) {
            // Fallback: Round-Robin entre os conectados
            conn = connections.find { it.connected }
            if (!conn) throw new RuntimeException("Todos os brokers estão offline.")
        }
        return conn
    }

    /**
     * Publica uma mensagem em um tópico.
     * @param topic  nome do tópico
     * @param data   mapa de chave-valor que será serializado em JSON
     */
    void publish(String topic, Map data) {
        String payload = JsonOutput.toJson(data)
        BrokerConnection conn = brokerForTopic(topic)
        conn.send(buildPublish(topic, payload))
        println "[PubSubClient] Publicado em '${topic}' via ${conn.address}: ${payload}"
    }

    /**
     * Inscreve o cliente em um tópico e registra o handler de mensagens.
     * @param topic   nome do tópico
     * @param handler closure chamada ao receber mensagem: { topic, Map payload -> }
     */
    void subscribe(String topic, Closure handler) {
        messageHandlers[topic] = handler
        BrokerConnection conn = brokerForTopic(topic)
        conn.send(buildSubscribe(topic))
        println "[PubSubClient] Inscrito em '${topic}' via ${conn.address}"
    }

    /**
     * Remove a inscrição em um tópico.
     */
    void unsubscribe(String topic) {
        messageHandlers.remove(topic)
        topicBrokerMap.remove(topic)
        BrokerConnection conn = brokerForTopic(topic)
        conn.send(buildUnsubscribe(topic))
        println "[PubSubClient] Inscrição removida de '${topic}'"
    }

    /**
     * Chamado internamente pelas conexões ao receber uma mensagem entregue pelo broker.
     */
    void onMessageReceived(String topic, String payloadJson) {
        Closure handler = messageHandlers[topic]
        if (handler) {
            try {
                Map data = new JsonSlurper().parseText(payloadJson) as Map
                handler.call(topic, data)
            } catch (Exception e) {
                println "[PubSubClient] Erro ao processar mensagem do tópico '${topic}': ${e.message}"
            }
        }
    }

    void disconnect() {
        connections.each { it.disconnect() }
        println "[PubSubClient] Desconectado de todos os brokers."
    }

    // ── helpers de protocolo ──────────────────────────────────────────────────

    private static String buildSubscribe(String topic) {
        JsonOutput.toJson([action: 'SUBSCRIBE', topic: topic])
    }

    private static String buildUnsubscribe(String topic) {
        JsonOutput.toJson([action: 'UNSUBSCRIBE', topic: topic])
    }

    private static String buildPublish(String topic, String payload) {
        JsonOutput.toJson([action: 'PUBLISH', topic: topic, payload: payload])
    }
}
