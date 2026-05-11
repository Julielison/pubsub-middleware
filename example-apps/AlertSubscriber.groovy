package br.edu.ifpb.pubsub.examples

import br.edu.ifpb.pubsub.client.PubSubClient

class AlertSubscriber {

    static void run(List<String> brokers, int duration = 12000) {
        println '[AlertSubscriber] Iniciando sistema de alertas...'
        PubSubClient client = new PubSubClient(brokers)
        client.connect()
        sleep(300)

        client.subscribe('umidade') { String topic, Map data ->
            double valor = data.valor as double
            String alerta = valor > 80 ? 'UMIDADE ALTA!' : (valor < 45 ? 'UMIDADE BAIXA!' : 'Normal')
            println '[Alertas] UMIDADE | Estacao: ' + data.estacao + ' | Valor: ' + data.valor + ' ' + data.unidade + ' | Alerta: ' + alerta
        }

        client.subscribe('atualizacao_estoque') { String topic, Map data ->
            int estoque = data.estoque_atual as int
            String alerta = estoque < 10 ? 'ESTOQUE CRITICO!' : 'OK'
            println '[Alertas] ESTOQUE | Produto: ' + data.produto + ' | Op: ' + data.operacao + ' (' + data.quantidade + ' unid.) | Estoque: ' + data.estoque_atual + ' [' + alerta + ']'
        }

        sleep(duration)

        client.unsubscribe('umidade')
        client.unsubscribe('atualizacao_estoque')
        client.disconnect()
        println '[AlertSubscriber] Encerrado.'
    }
}
