# Middleware Publish/Subscribe — Groovy + Gradle

**IFPB · Programação Distribuída · Prof. Ruan Delgado Gomes**

---

## Arquitetura

```
pubsub-middleware/
├── broker/            # Módulo do Broker (servidor)
├── client-lib/        # Biblioteca cliente (API para aplicações)
├── example-apps/      # Aplicações de exemplo
├── build.gradle       # Build raiz
├── settings.gradle
└── run.sh             # Script de execução rápida
```

### Componentes

| Classe | Responsabilidade |
|---|---|
| `Broker` | Servidor TCP que aceita conexões |
| `TopicManager` | Gerencia tópicos e inscritos (thread-safe) |
| `MessageDispatcher` | Bufferiza e despacha mensagens assincronamente |
| `ClientHandler` | Gerencia cada conexão de cliente individualmente |
| `Protocol` | Define o protocolo JSON de comunicação |
| `PubSubClient` | Biblioteca cliente com balanceamento de carga |
| `BrokerConnection` | Conexão TCP persistente com leitura assíncrona |

---

## Protocolo de Comunicação

Mensagens trocadas via TCP como JSON por linha (`\n`).

### Cliente → Broker

```json
{ "action": "SUBSCRIBE",   "topic": "temperatura" }
{ "action": "UNSUBSCRIBE", "topic": "temperatura" }
{ "action": "PUBLISH",     "topic": "temperatura", "payload": "{\"valor\": 25.3}" }
```

### Broker → Cliente

```json
{ "status": "OK",        "message": "Inscrito no tópico 'temperatura'" }
{ "status": "ERROR",     "message": "..." }
{ "status": "MESSAGE",   "topic": "temperatura", "payload": "{...}" }
{ "status": "DISCARDED", "message": "Mensagem descartada: nenhum inscrito no tópico 'x'" }
```

---

## Balanceamento de Carga

A `PubSubClient` conecta-se a **múltiplos brokers simultaneamente** e usa
**Round-Robin por hash de tópico**: cada tópico é mapeado deterministicamente
a um broker (`Math.abs(topic.hashCode()) % numBrokers`), garantindo que
subscribe e publish do mesmo tópico sempre vão ao mesmo broker.
O cliente não precisa saber quantos brokers existem.

---

## Cenário de Teste

- **2 Brokers**: portas `9001` e `9002`
- **2 Publicadores**:
  - `SensorPublisher` → tópicos `temperatura`, `umidade`
  - `OrderPublisher`  → tópicos `novo_pedido`, `atualizacao_estoque`
- **2 Consumidores**:
  - `DashboardSubscriber` → consome `temperatura` e `novo_pedido`
  - `AlertSubscriber`     → consome `umidade` e `atualizacao_estoque`
- **4 Tópicos**: `temperatura`, `umidade`, `novo_pedido`, `atualizacao_estoque`
- **Teste extra**: publicação em tópico sem inscritos → mensagem descartada

---

## Como Executar

### Com o script (sem Gradle instalado)

```bash
# Instalar Groovy se necessário
sudo apt install groovy

cd pubsub-middleware
chmod +x run.sh
./run.sh
```

### Com Gradle (requer Gradle 8.5+)

```bash
cd pubsub-middleware
./gradlew :example-apps:run
```

### Brokers separados (terminais diferentes)

```bash
# Terminal 1 — Broker na porta 9001
./gradlew :broker:run --args="9001"

# Terminal 2 — Broker na porta 9002
./gradlew :broker:run --args="9002"

# Terminal 3 — Aplicações de exemplo
./gradlew :example-apps:run
```

---

## Requisitos Atendidos

- [x] Broker aceita múltiplas conexões simultâneas (thread pool)
- [x] Clientes podem publicar, se inscrever e cancelar inscrição
- [x] Tópicos criados em runtime; removidos quando sem inscritos
- [x] Mensagens descartadas (com aviso) quando não há inscritos
- [x] Buffer assíncrono: recebimento e despacho independentes
- [x] Balanceamento de carga entre múltiplas instâncias do broker
- [x] Transparência: cliente não sabe quantos brokers existem
- [x] 2 publicadores, 2 consumidores, 4+ tópicos de exemplo
