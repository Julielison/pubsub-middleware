# Módulo gRPC Version

Este módulo é uma implementação alternativa e isolada do sistema de mensagens Publish/Subscribe utilizando tecnologias mais modernas e robustas: **gRPC** e **Protocol Buffers**.

Diferente dos outros módulos do repositório, que implementam o controle de conexões TCP e parsing de JSON na "unha", essa versão delega o "trabalho pesado" para o ecossistema gRPC.

## Principais Arquivos e Componentes

* **`src/main/proto/pubsub.proto`**: É a definição dos contratos (Interface Definition Language). Ele diz que nossos Brokers disponibilizam os serviços de "Subscribe" e "Publish", e o tipo de bytes que fluem neles (Strings e Arrays).
* **`GrpcBroker`**: Equivalente ao antigo `BrokerMain`. Ele usa as classes geradas automaticamente via compilação do arquivo proto, levantando a instância do servidor atrelado à classe de implementação `BrokerServiceImpl`.
* **`BrokerServiceImpl`**: Responde às requisições dos clientes gRPC. O serviço `subscribe` tira vantagem do *Server Streaming* (stream contínuo bidirecional de ponta), deixando uma via permanentemente em aberto via `StreamObserver`. 
* **`GrpcClient`**: Representa um cliente (semelhante ao nosso antigo script `RunAllExamples` combinado ao `PubSubClient`). Ele faz duas requisições diferentes pelo mesmo *Channel*: uma não bloqueante para escutar eventos; outra síncrona para postar (dar `publish`).

## Por que migrar do modelo Socket em loop?

* **Eficiência e Velocidade**: Mensagens não trafegam mais num enorme bloco JSON. Elas vão convertidas para representação binária e tipadas em gRPC em cima do mais eficiente HTTP/2.
* **Complexidade Zero**: O servidor não precisa abrir threads gigantes só pra ler um SocketInputStream até achar um char de \n (newline). O protocolo binário garante começo, meio e fim de requisição nativamente.
* **Streaming Nativo**: Subscribes reativos baseados em evento.

## Como Executar

Para executar o servidor Broker gRPC:

```bash
./gradlew :grpc-version:run --args="broker" 
# (assumindo que seja necessário mudar a mainClass, senão apenas criar a task de run específica)
```

No momento, a execução direta vai exigir que vocẽ atualize as referências da classe `main` em sua IDE ou invoque ela usando plugins customizados caso queira acionar via linha de comando do servidor de mock e o cliente separadamente.