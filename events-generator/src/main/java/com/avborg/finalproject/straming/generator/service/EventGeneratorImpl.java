package com.avborg.finalproject.straming.generator.service;

import com.avborg.finalproject.straming.generator.model.Event;
import com.google.common.net.InetAddresses;
import org.apache.commons.lang3.RandomStringUtils;

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
            time = currentTimeInSeconds();
        }
    }
    private Event generateOne(Long time) {
        if (isBot) {
            return new Event("click", ip, time, genRandomUrl());
        } else {
            return new Event("click", generateRandomIp(), currentTimeInSeconds(), genRandomUrl());
        }
    }

    public String genRandomUrl() {
        return "https://clck.ru/" + RandomStringUtils.random(6, true, true);
    }
    public Event generateOne() {
        return generateOne(time);
    }

    @Override
    public List<Event> generateList(long count) {
        List<Event> result = new ArrayList<>();

        for (int i = 0; i < count; i++)
            result.add(generateOne(currentTimeInSeconds()));

        return result;
    }

    private String generateRandomIp() {
        Random r = new Random();
        return InetAddresses.fromInteger(r.nextInt()).getHostAddress();
    }

    private Long currentTimeInSeconds() {
        return System.currentTimeMillis()/1000;
    }
}
