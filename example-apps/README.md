# Módulo Example-Apps

Este módulo concentra scripts que simulam aplicações reais (Publicadores e Consumidores) que dependem e incluem nativamente o módulo `client-lib` para se comunicar com o `broker`.

## Principais Classes

* **`RunAllExamples`**: É a classe de conveniência usada como ponto principal deste módulo. O papel dela é disparar internamente as aplicações de forma concorrente provando que a topologia está funcionando.
* **`SensorPublisher`**: Atua gerando e publicando dados randômicos em tópicos como `temperatura` e `umidade`.
* **`OrderPublisher`**: Atua como outro serviço independente que gera e publica mensagens nos tópicos `novo_pedido` e `atualizacao_estoque`.
* **`DashboardSubscriber`**: Aplicação que consome informações de interesse global (ex: `temperatura` e `novo_pedido`) e printaria em um painel do usuário.
* **`AlertSubscriber`**: Aplicação de backoffice inscrita em métricas de alerta como (ex: `umidade`, `atualizacao_estoque`).

## Fluxo de Execução

Ao rodar a task do gradle correspondente ao exemplo (`./gradlew :example-apps:run`):

1. **Orquestração**: A classe `RunAllExamples` é chamada pelo Gradle. Ela instanciará múltiplas threads simultâneas para simular diferentes processos existindo no mesmo espaço de tempo físico.
2. **Setup dos Clientes**: Em ambas as instâncias (consumidores e publicadores), uma conexão com portas pré-acordadas dos Brokers (9001, 9002) é instanciada chamando a lib do projeto via construtor principal de `PubSubClient`.
3. **Consumidores Registram Desejo**: Inicialmente as threads ativas do `DashboardSubscriber` e do `AlertSubscriber` avisam aos brokers (via cliente): "inscreva-me em X e y".
4. **Publicadores Geram Tráfego**: Instantes curtos de tempo depois, os *Publishers* entram em de loops contínuos enviando mensagens baseadas em JSON formatado com dados para os canais corretos.
5. **Observabilidade (Output)**: Os consumidores recebem isso e emitem um _print_ interativo em real-time no log/console ilustrando para o usuário que foi recebido do Broker X, Tópico Y, Mensagem Z.