package dev.dmco.test.kafka.messages.response;

public interface ResponseMessage {

    short NO_ERROR = 0;

    ResponseHeader header();

    ResponseMessage withHeader(ResponseHeader header);
}
