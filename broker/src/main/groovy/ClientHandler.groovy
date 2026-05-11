
import java.util.concurrent.atomic.AtomicInteger

/**
 * Gerencia a conexão com um único cliente.
 * Cada cliente possui sua própria thread de leitura.
 */
class ClientHandler implements Runnable {

    private static final AtomicInteger counter = new AtomicInteger(0)

    final int id
    final String brokerId
    private final Socket socket
    private final TopicManager topicManager
    private final MessageDispatcher dispatcher

    private BufferedReader reader
    private PrintWriter writer
    private volatile boolean connected = true

    ClientHandler(Socket socket, TopicManager topicManager, MessageDispatcher dispatcher, String brokerId) {
        this.id = counter.incrementAndGet()
        this.socket = socket
        this.topicManager = topicManager
        this.dispatcher = dispatcher
        this.brokerId = brokerId
    }

    @Override
    void run() {
        try {
            reader = new BufferedReader(new InputStreamReader(socket.inputStream, 'UTF-8'))
            writer = new PrintWriter(new OutputStreamWriter(socket.outputStream, 'UTF-8'), true)

            println "[Broker-${brokerId}] Cliente #${id} conectado: ${socket.remoteSocketAddress}"
            send(Protocol.buildOk("Conectado ao Broker ${brokerId}. ID do cliente: ${id}"))

            String line
            while (connected && (line = reader.readLine()) != null) {
                handleMessage(line.trim())
            }
        } catch (Exception e) {
            if (connected) {
                println "[Broker-${brokerId}] Erro no cliente #${id}: ${e.message}"
            }
        } finally {
            disconnect()
        }
    }

    private void handleMessage(String raw) {
        try {
            Map msg = Protocol.parse(raw)
            String action = msg.action as String

            switch (action) {
                case Protocol.ACTION_SUBSCRIBE:
                    String topic = msg.topic as String
                    topicManager.subscribe(topic, this)
                    send(Protocol.buildOk("Inscrito no tópico '${topic}'"))
                    break

                case Protocol.ACTION_UNSUBSCRIBE:
                    String topic = msg.topic as String
                    topicManager.unsubscribe(topic, this)
                    send(Protocol.buildOk("Inscrição removida do tópico '${topic}'"))
                    break

                case Protocol.ACTION_PUBLISH:
                    String topic   = msg.topic as String
                    String payload = msg.payload as String
                    dispatcher.enqueue(topic, payload, this)
                    if (topicManager.hasSubscribers(topic)) {
                        send(Protocol.buildOk("Mensagem publicada no tópico '${topic}'"))
                    }
                    break

                default:
                    send(Protocol.buildError("Ação desconhecida: ${action}"))
            }
        } catch (Exception e) {
            send(Protocol.buildError("Formato de mensagem inválido: ${e.message}"))
        }
    }

    synchronized void send(String message) {
        try {
            writer?.println(message)
        } catch (Exception e) {
            println "[Broker-${brokerId}] Falha ao enviar para cliente #${id}: ${e.message}"
        }
    }

    void disconnect() {
        if (!connected) return
        connected = false
        topicManager.removeClient(this)
        try { socket.close() } catch (ignored) {}
        println "[Broker-${brokerId}] Cliente #${id} desconectado."
    }
}
