
/**
 * Ponto de entrada do Broker.
 * Uso: groovy BrokerMain.groovy [porta1] [porta2] ...
 * Padrão: inicia 2 brokers nas portas 9001 e 9002.
 */
class BrokerMain {

    static void main(String[] args) {
        List<Integer> ports = args ? args.collect { it.toInteger() } : [9001, 9002]

        println "Iniciando ${ports.size()} instância(s) de Broker..."

        List<Broker> brokers = ports.collect { port ->
            new Broker(port, "B${port}")
        }

        // Inicia cada broker em sua própria thread
        brokers.each { broker ->
            Thread.start("broker-${broker.port}") {
                broker.start()
            }
        }

        // Aguarda encerramento via Ctrl+C
        addShutdownHook {
            println "\nEncerrando brokers..."
            brokers.each { it.stop() }
        }

        // Mantém a JVM viva
        Thread.currentThread().join()
    }
}
