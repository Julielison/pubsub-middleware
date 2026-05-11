package br.edu.ifpb.pubsub.examples

import br.edu.ifpb.pubsub.client.PubSubClient

/**
 * PUBLICADOR 2: Sistema de E-commerce
 * Publica dados dos tópicos:
 *   - "novo_pedido"      → pedidos realizados
 *   - "atualizacao_estoque" → mudanças no estoque
 */
class OrderPublisher {

    static void run(List<String> brokers) {
        println "\n[OrderPublisher] Iniciando sistema de e-commerce..."
        PubSubClient client = new PubSubClient(brokers)
        client.connect()
        sleep(500)

        String[] produtos = ["Notebook", "Mouse", "Teclado", "Monitor", "Headset"]
        String[] status   = ["PENDENTE", "CONFIRMADO", "EM_SEPARACAO"]
        Random rnd = new Random()

        5.times { i ->
            String produto = produtos[rnd.nextInt(produtos.length)]
            int quantidade = rnd.nextInt(5) + 1
            double preco   = 50.0 + rnd.nextDouble() * 950.0

            client.publish("novo_pedido", [
                pedido_id : "PED-${1000 + i}",
                produto   : produto,
                quantidade: quantidade,
                preco     : Math.round(preco * 100) / 100.0,
                status    : status[rnd.nextInt(status.length)],
                timestamp : new Date().toString()
            ])

            client.publish("atualizacao_estoque", [
                produto   : produto,
                operacao  : "SAIDA",
                quantidade: quantidade,
                estoque_atual: rnd.nextInt(100),
                timestamp : new Date().toString()
            ])

            sleep(1200)
        }

        client.disconnect()
        println "[OrderPublisher] Encerrado."
    }
}
