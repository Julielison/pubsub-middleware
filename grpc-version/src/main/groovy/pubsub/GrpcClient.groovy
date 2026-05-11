package pubsub

import com.pubsub.grpc.*
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.stub.StreamObserver

class GrpcClient {
    static void main(String[] args) {
        // Inicializa o gRPC channel
        ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", 9005)
                .usePlaintext() // desabilita TLS/SSL para rodar em localhost
                .build()

        // Para subscribe usamos o stub assíncrono (não-bloqueante) porque é um Stream
        BrokerServiceGrpc.BrokerServiceStub asyncStub = BrokerServiceGrpc.newStub(channel)
        
        // Para publish usamos o stub bloqueante (unary call normal)
        BrokerServiceGrpc.BrokerServiceBlockingStub blockingStub = BrokerServiceGrpc.newBlockingStub(channel)

        String topicTarget = "temperatura"

        // 1. INCREVENDO-SE (Subscribe)
        println("Inscrevendo-se em: ${topicTarget}...")
        SubscribeRequest subReq = SubscribeRequest.newBuilder().setTopic(topicTarget).build()
        
        asyncStub.subscribe(subReq, new StreamObserver<MessageEvent>() {
            @Override
            void onNext(MessageEvent value) {
                println(">>> [Mensagem Recebida] Tópico: ${value.topic} | Payload: ${value.payload}")
            }

            @Override
            void onError(Throwable t) {
                println(">>> [Erro no Stream]: ${t.message}")
            }

            @Override
            void onCompleted() {
                println(">>> [Stream Finalizado pelo Servidor]")
            }
        })

        // 2. PUBLICANDO (Publish)
        // Dá um pequeno atraso apenas para a stream async conectar antes de publicarmos
        Thread.sleep(500) 

        println("Publicando duas mensagens em: ${topicTarget}...")
        
        // Primeira publicação
        PublishRequest pubReq1 = PublishRequest.newBuilder()
                .setTopic(topicTarget)
                .setPayload("{ \"valor\": 25.5 }")
                .build()
                
        PublishResponse resp1 = blockingStub.publish(pubReq1)
        println("[Publish Result 1]: ${resp1.status} - ${resp1.message}")

        // Segunda publicação
        PublishRequest pubReq2 = PublishRequest.newBuilder()
                .setTopic(topicTarget)
                .setPayload("{ \"valor\": 26.1 }")
                .build()
                
        PublishResponse resp2 = blockingStub.publish(pubReq2)
        println("[Publish Result 2]: ${resp2.status} - ${resp2.message}")

        // Impede que a thread termine para continuarmos ouvindo novos eventos do stream
        Thread.sleep(3000)
        channel.shutdown()
    }
}