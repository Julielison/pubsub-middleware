package br.edu.ifpb.pubsub.client

import groovy.json.JsonSlurper

/**
 * Gerencia a conexão TCP com um único broker.
 * Possui thread própria de leitura assíncrona de mensagens.
 */
class BrokerConnection {

    final String host
    final int port
    final String address

    private final PubSubClient client
    private Socket socket
    private PrintWriter writer
    private BufferedReader reader
    volatile boolean connected = false

    private static final JsonSlurper slurper = new JsonSlurper()

    BrokerConnection(String host, int port, PubSubClient client) {
        this.host = host
        this.port = port
        this.address = "${host}:${port}"
        this.client = client
    }

    void connect() {
        socket = new Socket(host, port)
        socket.soTimeout = 0 // sem timeout — conexão persistente
        writer = new PrintWriter(new OutputStreamWriter(socket.outputStream, 'UTF-8'), true)
        reader = new BufferedReader(new InputStreamReader(socket.inputStream, 'UTF-8'))
        connected = true
        startReader()
    }

    /**
     * Thread de leitura assíncrona: processa mensagens recebidas do broker.
     */
    private void startReader() {
        Thread.startDaemon("reader-${address}") {
            try {
                String line
                while (connected && (line = reader.readLine()) != null) {
                    processIncoming(line.trim())
                }
            } catch (Exception e) {
                if (connected) {
                    println "[BrokerConnection] Conexão perdida com ${address}: ${e.message}"
                }
            } finally {
                connected = false
            }
        }
    }

    private void processIncoming(String raw) {
        try {
            Map msg = slurper.parseText(raw) as Map
            String status = msg.status as String

            switch (status) {
                case 'OK':
                    println "[BrokerConnection][${address}] ✓ ${msg.message}"
                    break
                case 'MESSAGE':
                    client.onMessageReceived(msg.topic as String, msg.payload as String)
                    break
                case 'DISCARDED':
                    println "[BrokerConnection][${address}] ⚠ ${msg.message}"
                    break
                case 'ERROR':
                    println "[BrokerConnection][${address}] ✗ Erro: ${msg.message}"
                    break
                default:
                    println "[BrokerConnection][${address}] Resposta desconhecida: ${raw}"
            }
        } catch (Exception e) {
            println "[BrokerConnection] Erro ao processar resposta: ${e.message}"
        }
    }

    synchronized void send(String message) {
        if (!connected) throw new RuntimeException("Não conectado a ${address}")
        writer.println(message)
    }

    void disconnect() {
        connected = false
        try { socket?.close() } catch (ignored) {}
    }
}
