/*
 * Copyright 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rbmhtechnology.eventuate;

import akka.japi.pf.FI;
import akka.japi.pf.PFBuilder;

/**
 * Java API for building a {@link AbstractEventsourcedProcessor.Process} behavior that matches arbitrary [[Object]]s to {@link Iterable}s.
 *
 * Used to define processing behaviour in {@link AbstractEventsourcedProcessor#createOnProcessEvent}.
 */
public final class ProcessBuilder {

    private final PFBuilder<Object, Iterable<Object>> underlying;

    private ProcessBuilder(PFBuilder<Object, Iterable<Object>> underlying) {
        this.underlying = underlying;
    }

    /**
     * Returns a new {@link ProcessBuilder} instance.
     *
     * @return a {@link ProcessBuilder}
     */
    public static ProcessBuilder create() {
        return new ProcessBuilder(new PFBuilder<>());
    }

    /**
     * Add a new case statement to this builder.
     *
     * @param type the type to match the argument against
     * @param apply the function to apply for the given argument - must return {@link Iterable}
     * @param <P> the type of the argument
     * @return the {@link ProcessBuilder} with the case statement added
     */
    public <P> ProcessBuilder match(final Class<P> type, IterableApply<P> apply) {
        underlying.match(type, apply);
        return this;
    }

    /**
     * Add a new case statement to this builder.
     *
     * @param type the type to match the argument against
     * @param predicate the predicate to match the argument against
     * @param apply the function to apply for the given argument - must return {@link Iterable}
     * @param <P> the type of the argument
     * @return the {@link ProcessBuilder} with the case statement added
     */
    public <P> ProcessBuilder match(final Class<P> type, final FI.TypedPredicate<P> predicate, final IterableApply<P> apply) {
        underlying.match(type, predicate, apply);
        return this;
    }

    /**
     * Add a new case statement to this builder without compile time type check of the parameters.
     * Should normally not be used, but when matching on class with generic type.
     *
     * @param type the type to match the argument against
     * @param apply the function to apply for the given argument - must return {@link Iterable}
     * @return the {@link ProcessBuilder} with the case statement added
     */
    public ProcessBuilder matchUnchecked(final Class<?> type, IterableApply<?> apply) {
        underlying.matchUnchecked(type, apply);
        return this;
    }

    /**
     * Add a new case statement to this builder without compile time type check of the parameters.
     * Should normally not be used, but when matching on class with generic type.
     *
     * @param type the type to match the argument against
     * @param predicate a predicate that will be evaluated on the argument if the type matches
     * @param apply the function to apply for the given argument - must return {@link Iterable}
     * @return the {@link ProcessBuilder} with the case statement added
     */
    public ProcessBuilder matchUnchecked(final Class<?> type, final FI.TypedPredicate<?> predicate, final IterableApply<?> apply) {
        underlying.matchUnchecked(type, predicate, apply);
        return this;
    }

    /**
     * Add a new case statement to this builder.
     *
     * @param object the object to match the argument against
     * @param apply the function to apply for the given argument - must return an {@link Iterable}
     * @param <P> the type of the argument
     * @return the {@link ProcessBuilder} with the case statement added
     */
    public <P> ProcessBuilder matchEquals(final P object, final IterableApply<P> apply) {
        underlying.matchEquals(object, apply);
        return this;
    }

    /**
     * Add a new case statement to this builder, that matches any argument.
     *
     * @param apply the function to apply for the given argument - must return an {@link Iterable}
     * @return the {@link ProcessBuilder} with the case statement added
     */
    public ProcessBuilder matchAny(final IterableApply<Object> apply) {
        underlying.matchAny(apply);
        return this;
    }

    /**
     * Builds the resulting processing behavior as an instance of {@link AbstractEventsourcedProcessor.Process}.
     *
     * @return the configured {@link AbstractEventsourcedProcessor.Process}
     */
    public AbstractEventsourcedProcessor.Process build() {
        return new AbstractEventsourcedProcessor.Process(underlying.build());
    }

    public interface IterableApply<T> extends FI.Apply<T, Iterable<Object>> {
    }
}
