package br.edu.ifpb.pubsub.broker

import groovy.json.JsonOutput
import groovy.json.JsonSlurper

/**
 * Protocolo de comunicação entre cliente e broker.
 *
 * Formato das mensagens (JSON por linha):
 *   { "action": "SUBSCRIBE",   "topic": "topico" }
 *   { "action": "UNSUBSCRIBE", "topic": "topico" }
 *   { "action": "PUBLISH",     "topic": "topico", "payload": "{...}" }
 *
 * Respostas do broker:
 *   { "status": "OK",      "message": "..." }
 *   { "status": "ERROR",   "message": "..." }
 *   { "status": "MESSAGE", "topic": "topico", "payload": "{...}" }
 *   { "status": "DISCARDED","message": "..." }
 */
class Protocol {

    static final String ACTION_SUBSCRIBE   = 'SUBSCRIBE'
    static final String ACTION_UNSUBSCRIBE = 'UNSUBSCRIBE'
    static final String ACTION_PUBLISH     = 'PUBLISH'

    static final String STATUS_OK        = 'OK'
    static final String STATUS_ERROR     = 'ERROR'
    static final String STATUS_MESSAGE   = 'MESSAGE'
    static final String STATUS_DISCARDED = 'DISCARDED'

    private static final JsonSlurper slurper = new JsonSlurper()

    static Map parse(String line) {
        slurper.parseText(line) as Map
    }

    static String buildSubscribe(String topic) {
        JsonOutput.toJson([action: ACTION_SUBSCRIBE, topic: topic])
    }

    static String buildUnsubscribe(String topic) {
        JsonOutput.toJson([action: ACTION_UNSUBSCRIBE, topic: topic])
    }

    static String buildPublish(String topic, String payload) {
        JsonOutput.toJson([action: ACTION_PUBLISH, topic: topic, payload: payload])
    }

    static String buildOk(String message) {
        JsonOutput.toJson([status: STATUS_OK, message: message])
    }

    static String buildError(String message) {
        JsonOutput.toJson([status: STATUS_ERROR, message: message])
    }

    static String buildDelivery(String topic, String payload) {
        JsonOutput.toJson([status: STATUS_MESSAGE, topic: topic, payload: payload])
    }

    static String buildDiscarded(String topic) {
        JsonOutput.toJson([status: STATUS_DISCARDED, message: "Mensagem descartada: nenhum inscrito no tópico '${topic}'"])
    }
}
