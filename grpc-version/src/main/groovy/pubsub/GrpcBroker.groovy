package pubsub

import com.pubsub.grpc.*
import io.grpc.Server
import io.grpc.ServerBuilder
import io.grpc.stub.ServerCallStreamObserver
import io.grpc.stub.StreamObserver

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

class BrokerServiceImpl extends BrokerServiceGrpc.BrokerServiceImplBase {

    // Mantém quem está escutando cada tópico: Map<Topico, List<StreamObserver>>
    private final ConcurrentHashMap<String, List<StreamObserver<MessageEvent>>> topics = new ConcurrentHashMap<>()

    @Override
    void subscribe(SubscribeRequest request, StreamObserver<MessageEvent> responseObserver) {
        String topic = request.topic
        
        // Inicializa a lista do tópico se não existir
        def observers = topics.computeIfAbsent(topic, { k -> new CopyOnWriteArrayList<>() })
        observers.add(responseObserver)

        println("[Broker] Novo inscrito no tópico: ${topic}")

        // gRPC permite interceptar quando o cliente cancela o stream/desconecta
        if (responseObserver instanceof ServerCallStreamObserver) {
            ((ServerCallStreamObserver) responseObserver).setOnCancelHandler({
                println("[Broker] Cliente desconectado do tópico: ${topic}")
                topics.get(topic)?.remove(responseObserver)
            })
        }
        
        // Nós não chamamos responseObserver.onCompleted() aqui, pois o stream 
        // deve ficar aberto para enviarmos as mensagens do tópico de forma assíncrona!
    }

    @Override
    void publish(PublishRequest request, StreamObserver<PublishResponse> responseObserver) {
        String topic = request.topic
        String payload = request.payload

        def observers = topics.get(topic)
        if (observers != null && !observers.isEmpty()) {
            // Prepara a msg para envio
            MessageEvent event = MessageEvent.newBuilder()
                    .setTopic(topic)
                    .setPayload(payload)
                    .build()

            // Despacha a mensagem para todos os streams abertos neste tópico
            observers.each { obs ->
                try {
                    obs.onNext(event)
                } catch (Exception e) {
                    println("[Broker] Falha ao enviar para um cliente (Removendo-o): ${e.message}")
                    observers.remove(obs)
                }
            }

            // Responde via unary call (Request/Response) indicando sucesso a quem publicou
            PublishResponse res = PublishResponse.newBuilder()
                    .setStatus("OK")
                    .setMessage("Mensagem entregue a ${observers.size()} inscritos.")
                    .build()
            responseObserver.onNext(res)
        } else {
            // Ninguém mapeado para este tópico
            PublishResponse res = PublishResponse.newBuilder()
                    .setStatus("DISCARDED")
                    .setMessage("Nenhum inscrito no tópico '${topic}'")
                    .build()
            responseObserver.onNext(res)
        }
        responseObserver.onCompleted()
    }
}

class GrpcBroker {
    static void main(String[] args) {
        int port = 9005
        Server server = ServerBuilder.forPort(port)
                .addService(new BrokerServiceImpl())
                .build()
                .start()
        
        println("gRPC Broker iniciado na porta ${port}")
        server.awaitTermination()
    }
}