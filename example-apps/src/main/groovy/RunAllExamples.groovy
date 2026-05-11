

class RunAllExamples {

    static void main(String[] args) {
        println '========================================================'
        println '  MIDDLEWARE PUB/SUB -- DEMONSTRACAO COMPLETA'
        println '  IFPB - Programacao Distribuida - Prof. Ruan Delgado'
        println '========================================================'

        List<String> brokerAddresses = ['localhost:9001', 'localhost:9002']

        println '>>> Iniciando 2 brokers (portas 9001 e 9002)...'
        Broker broker1 = new Broker(9001, 'B9001')
        Broker broker2 = new Broker(9002, 'B9002')

        Thread.start { broker1.start() }
        Thread.start { broker2.start() }
        sleep(800)

        println '>>> Conectando consumidores...'
        Thread dashThread  = Thread.start { DashboardSubscriber.run(brokerAddresses, 14_000) }
        Thread alertThread = Thread.start { AlertSubscriber.run(brokerAddresses, 14_000) }
        sleep(800)

        println '>>> Iniciando publicadores...'
        Thread sensorThread = Thread.start { SensorPublisher.run(brokerAddresses) }
        Thread orderThread  = Thread.start { OrderPublisher.run(brokerAddresses) }

        sensorThread.join()
        orderThread.join()
        dashThread.join()
        alertThread.join()

        println '========================================================'
        println '  DEMONSTRACAO CONCLUIDA COM SUCESSO!'
        println '========================================================'

        broker1.stop()
        broker2.stop()
        System.exit(0)
    }
}
