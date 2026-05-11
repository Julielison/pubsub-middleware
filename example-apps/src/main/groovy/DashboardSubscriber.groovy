
class DashboardSubscriber {

    static void run(List<String> brokers, int duration = 12000) {
        println '[DashboardSubscriber] Iniciando dashboard de monitoramento...'
        PubSubClient client = new PubSubClient(brokers)
        client.connect()
        sleep(300)

        client.subscribe('temperatura') { String topic, Map data ->
            println '[Dashboard] TEMPERATURA | Estacao: ' + data.estacao + ' | Valor: ' + data.valor + ' ' + data.unidade + ' | Hora: ' + data.timestamp
        }

        client.subscribe('novo_pedido') { String topic, Map data ->
            println '[Dashboard] PEDIDO | Id: ' + data.pedido_id + ' | Produto: ' + data.produto + ' (x' + data.quantidade + ') | Preco: R$ ' + data.preco + ' | Status: ' + data.status
        }

        sleep(duration)

        client.unsubscribe('temperatura')
        client.unsubscribe('novo_pedido')
        client.disconnect()
        println '[DashboardSubscriber] Encerrado.'
    }
}
