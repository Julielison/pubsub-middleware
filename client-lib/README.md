# Módulo Client-Lib

Este módulo é a biblioteca (lib) que as aplicações cliente importam e utilizam para se conectar de maneira fácil e transparente aos brokers no sistema de mensagens.

## Principais Classes

* **`PubSubClient`**: A interface pública de alto nível usada pelas aplicações. Seu principal papel, além de abstrair os envios, é centralizar o **balanceamento de carga (Load Balancing)** e conectar-se simultaneamente a diferentes nós de *brokers*. Usa um algoritmo de determinismo (hash do tópico % total de brokers) para definir para qual broker enviará as mensagens de determinado tópico.
* **`BrokerConnection`**: Lida com a minúcia de abrir um Socket (TCP) com um Broker específico. Possui o buffer de escrita para enviar requisições (`SUBSCRIBE`, `PUBLISH`) e mantém uma "thread de escuta" dedicada que fica permanentemente lendo respostas e mensagens que chegam da rede disparando o callback de notificação para a aplicação cliente.

## Fluxo de Execução

1. **Inicialização**: A aplicação cliente cria e configura o `PubSubClient`, informando o IP/porta dos Brokers disponíveis e passando uma função de callback (que será chamada ao receber mensagens).
2. **Conexões Simultâneas**: Internamente, `PubSubClient` pede que a classe `BrokerConnection` abra sockets e threads de leitura de conexão com todos os brokers informados.
3. **Ações do Usuário (Aplicação)**:
   * **Subscribe**: A aplicação invoca `cliente.subscribe("tema-a")`. O `PubSubClient` realiza o hash do texto `"tema-a"`, descobre o Broker responsável por aquele tópico e invoca o _subscribe_ apenas nele.
   * **Publish**: Semelhantemente, ao fazer `cliente.publish("tema-a", "{...}")`, o cálculo por hash é refeito enviando a mensagem sempre para a mesma conexão tcp (broker) responsável pela distribuição daquele tópico.
4. **Tratamento de Mensagens Recebidas**: Aquele socket sendo administrado pelo `BrokerConnection` que estiver escutando a mensagem JSON chegando com `"status": "MESSAGE"`, converte a mensagem e aciona o _callback_ registrado pela aplicação na etapa (1).