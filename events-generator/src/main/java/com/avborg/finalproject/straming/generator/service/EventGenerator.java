package com.avborg.finalproject.straming.generator.service;

import java.util.List;

public interface EventGenerator<T> {

    T generateOne();
    List<T> generateList(long count);
}
