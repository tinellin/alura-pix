package com.alura.pix.streams;


import com.alura.pix.dto.PixDTO;
import com.alura.pix.serdes.PixSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class PixAggregator {

    /* Nesse caso, a anotação autowired indica que é um método que será executado assim que a aplicação subir */
    @Autowired
    public void aggregator(StreamsBuilder streamsBuilder) {
        KTable<String, Double> messageStream = streamsBuilder
                .stream("pix-topic", Consumed.with(Serdes.String(), new PixSerdes()))
                .peek((key, value) -> System.out.println("Pix recebido de: " + value.getChaveOrigem()))
                .groupBy((key, value) -> value.getChaveOrigem())
                .aggregate(
                    () -> 0.0,
                    (key, value, aggregate) -> (aggregate + value.getValor()),
                    Materialized.with(Serdes.String(), Serdes.Double())
                );

        messageStream.toStream().print(Printed.toSysOut());
        messageStream.toStream().to("pix-aggregation-topic", Produced.with(Serdes.String(), Serdes.Double()));
    }
}
