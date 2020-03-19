package com.avborg.finalproject.straming.generator.service;

import com.avborg.finalproject.straming.generator.model.Event;
import com.google.common.net.InetAddresses;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class EventGeneratorImpl implements EventGenerator<Event> {

    private final boolean isBot;
    private String ip;
    private Long time;

    public EventGeneratorImpl(boolean isBot) {
        this.isBot = isBot;
        if (isBot) {
            ip = generateRandomIp();
            time = new Date().getTime();
        }
    }


    public Event generateOne() {
        if (isBot) {
            return new Event("click", ip, time, "url");
        } else {
            return new Event("click", generateRandomIp(), new Date().getTime(), "url");
        }
    }

    @Override
    public List<Event> generateList(long count) {
        List<Event> result = new ArrayList<>();

        for (int i = 0; i < count; i++)
            result.add(generateOne());

        return result;
    }

    private String generateRandomIp() {
        Random r = new Random();
        return InetAddresses.fromInteger(r.nextInt()).getHostAddress();
    }
}
