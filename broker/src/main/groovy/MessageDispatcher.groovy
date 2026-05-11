
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * Responsável por buferizar mensagens e despachá-las de forma assíncrona.
 * O recebimento de mensagens e o encaminhamento são independentes,
 * garantindo que o broker não fique bloqueado ao encaminhar mensagens.
 */
class MessageDispatcher {

    private static class Envelope {
        final String topic
        final String payload
        final ClientHandler publisher

        Envelope(String topic, String payload, ClientHandler publisher) {
            this.topic = topic
            this.payload = payload
            this.publisher = publisher
        }
    }

    private final TopicManager topicManager
    private final LinkedBlockingQueue<Envelope> queue = new LinkedBlockingQueue<>()
    private final ExecutorService dispatchPool
    private volatile boolean running = true

    MessageDispatcher(TopicManager topicManager, int dispatchThreads = 4) {
        this.topicManager = topicManager
        this.dispatchPool = Executors.newFixedThreadPool(dispatchThreads)
        startDispatcher()
    }

    /**
     * Coloca a mensagem no buffer. Retorna rapidamente sem bloquear.
     */
    void enqueue(String topic, String payload, ClientHandler publisher) {
        queue.put(new Envelope(topic, payload, publisher))
    }

    /**
     * Thread de despacho: consome o buffer e encaminha para os inscritos.
     */
    private void startDispatcher() {
        Thread.start("dispatcher") {
            while (running) {
                try {
                    Envelope env = queue.poll(200, java.util.concurrent.TimeUnit.MILLISECONDS)
                    if (env) {
                        dispatchPool.submit { dispatch(env) }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt()
                }
            }
        }
    }

    private void dispatch(Envelope env) {
        Set<ClientHandler> subscribers = topicManager.getSubscribers(env.topic)

        if (subscribers.isEmpty()) {
            // Informa ao publicador que não há inscritos
            env.publisher?.send(Protocol.buildDiscarded(env.topic))
            println "[Dispatcher] Mensagem descartada no tópico '${env.topic}' (sem inscritos)."
            return
        }

        String delivery = Protocol.buildDelivery(env.topic, env.payload)
        int sent = 0
        subscribers.each { ClientHandler sub ->
            try {
                sub.send(delivery)
                sent++
            } catch (Exception e) {
                println "[Dispatcher] Falha ao entregar para cliente ${sub.id}: ${e.message}"
            }
        }
        println "[Dispatcher] Mensagem no tópico '${env.topic}' entregue para ${sent} inscrito(s)."
    }

    void shutdown() {
        running = false
        dispatchPool.shutdown()
    }
}
