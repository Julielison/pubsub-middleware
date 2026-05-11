# Módulo Broker

Este módulo representa o servidor do sistema de mensagens Publish/Subscribe. Ele é responsável por aceitar conexões TCP, gerenciar tópicos, manter a lista de clientes inscritos e fazer o roteamento das mensagens publicadas para os respectivos interessados.

## Principais Classes

* **`BrokerMain`**: O ponto de entrada da aplicação. Lê a porta passada por argumento (ou usa uma padrão) e inicializa a instância do `Broker`.
* **`Broker`**: Inicia o servidor TCP (`ServerSocket`) e fica em um loop aguardando novas conexões. Para cada nova conexão, delega o tratamento para o `ClientHandler` dentro de um pool de threads (ExecutorService).
* **`ClientHandler`**: Gerencia o ciclo de vida de uma conexão TCP individual de um cliente. Fica em loop lendo mensagens (em JSON separadas por quebra de linha), faz o parse via `Protocol` e aciona as ações corretas (inscrever, cancelar inscrição ou publicar) no `TopicManager`.
* **`TopicManager`**: Classe thread-safe responsável por manter o dicionário que mapeia o nome de cada tópico aos `ClientHandler`s dos clientes inscritos nele.
* **`MessageDispatcher`**: Fornece uma fila/buffer assíncrono onde mensagens publicadas são colocadas. Utiliza threads próprias para ler essa fila, descobrir quem são os inscritos no `TopicManager` e fazer o despacho (enviar via socket) para cada destino, não bloqueando a etapa de recebimento.
* **`Protocol`**: Define e valida o formato das mensagens JSON trocadas entre cliente e broker, garantindo a padronização e estruturação simples do protocolo.

## Fluxo de Execução

1. **Inicialização**: Ao executar `./gradlew :broker:run --args="9001"`, o `BrokerMain` é invocado, chamando `new Broker(9001).start()`.
2. **Conexões**: O socket passa a escutar. Quando um cliente conecta, uma nova thread com `ClientHandler` é gerada.
3. **Inscrição**: O cliente manda `{"action":"SUBSCRIBE", "topic": "xy"}`. O `ClientHandler` interpreta, associa este cliente ao tópico "xy" lá no `TopicManager` e devolve um `"status": "OK"`.
4. **Publicação & Despacho**:
   * O cliente manda `{"action":"PUBLISH", "topic":"xy", "payload":"..."}`.
   * O `ClientHandler` coloca essa publicação na fila de envio do `MessageDispatcher`.
   * As threads de background do `MessageDispatcher` repassam a mensagem para o socket de todos os clientes inscritos naquele tópico buscando as referências no `TopicManager`.