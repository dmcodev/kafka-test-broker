package dev.dmcode.test.kafka.messages.response;

public interface ResponseMessage {

    ResponseHeader header();

    ResponseMessage withHeader(ResponseHeader header);
}
