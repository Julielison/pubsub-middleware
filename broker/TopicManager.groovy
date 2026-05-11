package br.edu.ifpb.pubsub.broker

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArraySet

/**
 * Gerencia os tópicos e os clientes inscritos em cada tópico.
 * Thread-safe: usa estruturas concorrentes.
 */
class TopicManager {

    // topic -> conjunto de ClientHandler inscritos
    private final Map<String, Set<ClientHandler>> topics = new ConcurrentHashMap<>()

    /**
     * Inscreve um cliente em um tópico, criando-o se necessário.
     */
    synchronized void subscribe(String topic, ClientHandler client) {
        topics.computeIfAbsent(topic) { new CopyOnWriteArraySet<>() }.add(client)
        println "[TopicManager] Cliente ${client.id} inscrito no tópico '${topic}'. " +
                "Total de inscritos: ${topics[topic].size()}"
    }

    /**
     * Remove a inscrição de um cliente em um tópico.
     * Se não houver mais inscritos, o tópico é removido.
     */
    synchronized void unsubscribe(String topic, ClientHandler client) {
        Set<ClientHandler> subscribers = topics.get(topic)
        if (subscribers) {
            subscribers.remove(client)
            if (subscribers.isEmpty()) {
                topics.remove(topic)
                println "[TopicManager] Tópico '${topic}' removido (sem inscritos)."
            }
        }
    }

    /**
     * Remove todas as inscrições de um cliente (usado ao desconectar).
     */
    synchronized void removeClient(ClientHandler client) {
        topics.each { topic, subscribers ->
            subscribers.remove(client)
        }
        topics.entrySet().removeIf { it.value.isEmpty() }
    }

    /**
     * Retorna os inscritos de um tópico (cópia para evitar ConcurrentModification).
     */
    Set<ClientHandler> getSubscribers(String topic) {
        Set<ClientHandler> subs = topics.get(topic)
        return subs ? new HashSet<>(subs) : Collections.emptySet()
    }

    boolean hasSubscribers(String topic) {
        Set<ClientHandler> subs = topics.get(topic)
        return subs && !subs.isEmpty()
    }

    Set<String> getTopics() {
        return Collections.unmodifiableSet(topics.keySet())
    }

    void printStatus() {
        println "=== Tópicos ativos ==="
        topics.each { topic, subs ->
            println "  ${topic}: ${subs.size()} inscrito(s)"
        }
        println "====================="
    }
}
