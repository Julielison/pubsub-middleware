package br.edu.ifpb.pubsub.broker

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * Instância do Broker Pub/Sub.
 * Aceita conexões TCP e delega cada cliente a uma thread do pool.
 */
class Broker {

    final int port
    final String id

    private final TopicManager topicManager = new TopicManager()
    private final MessageDispatcher dispatcher
    private final ExecutorService clientPool
    private ServerSocket serverSocket
    private volatile boolean running = false

    Broker(int port, String id = "B-${port}", int dispatchThreads = 4, int clientThreads = 50) {
        this.port = port
        this.id = id
        this.dispatcher = new MessageDispatcher(topicManager, dispatchThreads)
        this.clientPool = Executors.newFixedThreadPool(clientThreads)
    }

    void start() {
        serverSocket = new ServerSocket(port)
        running = true
        println "╔══════════════════════════════════════╗"
        println "║  Broker [${id}] iniciado na porta ${port}  ║"
        println "╚══════════════════════════════════════╝"

        // Thread de monitoramento (status a cada 30s)
        Thread.startDaemon("monitor") {
            while (running) {
                sleep(30_000)
                topicManager.printStatus()
            }
        }

        while (running) {
            try {
                Socket client = serverSocket.accept()
                ClientHandler handler = new ClientHandler(client, topicManager, dispatcher, id)
                clientPool.submit(handler)
            } catch (SocketException e) {
                if (running) println "[Broker-${id}] Erro ao aceitar conexão: ${e.message}"
            }
        }
    }

    void stop() {
        running = false
        dispatcher.shutdown()
        clientPool.shutdown()
        try { serverSocket?.close() } catch (ignored) {}
        println "[Broker-${id}] Encerrado."
    }
}
