/*
 * Copyright (C) 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rbmhtechnology.eventuate;

import akka.japi.pf.FI;
import akka.japi.pf.Match;
import akka.japi.pf.PFBuilder;

/**
 * Java API for building a PartialFunction that matches arbitrary [[Object]]s to {@link Iterable}s.
 *
 * Can be used to define processing behaviour in {@link AbstractEventsourcedProcessor#setOnProcessEvent}
 * or {@link AbstractEventsourcedProcessor#onProcessEvent}.
 */
public final class ProcessBuilder {
    private ProcessBuilder() {
    }

    /**
     * Returns a new {@link PFBuilder} of {@link java.lang.Iterable} with a case statement added.
     *
     * @param type the type to match the argument against
     * @param apply the function to apply for the given argument - must return {@link java.lang.Iterable}
     * @param <P> the type of the argument
     * @return a builder with the case statement added
     */
    public static <P> PFBuilder<Object, Iterable<Object>> match(final Class<? extends P> type, final IterableApply<? extends P> apply) {
        return Match.match(type, apply);
    }

    /**
     * Returns a new {@link PFBuilder} of {@link java.lang.Iterable} with a case statement added.
     *
     * @param type the type to match the argument against
     * @param predicate the predicate to match the argument against
     * @param apply the function to apply for the given argument - must return {@link java.lang.Iterable}
     * @param <P> the type of the argument
     * @return a builder with the case statement added
     */
    public static <P> PFBuilder<Object, Iterable<Object>> match(final Class<? extends P> type,
                                                                final FI.TypedPredicate<? extends P> predicate,
                                                                final IterableApply<? extends P> apply) {
        return Match.match(type, predicate, apply);
    }

    /**
     * Returns a new {@link PFBuilder} of {@link java.lang.Iterable} with a case statement added.
     *
     * @param object the object to match the argument against
     * @param apply the function to apply for the given argument - must return an {@link java.lang.Iterable}
     * @param <P> the type of the argument
     * @return a builder with the case statement added
     */
    public static <P> PFBuilder<Object, Iterable<Object>> matchEquals(final P object, final IterableApply<P> apply) {
        return Match.matchEquals(object, apply);
    }

    /**
     * Returns a new {@link PFBuilder} of {@link java.lang.Iterable} with a default case statement added.
     *
     * @param apply the function to apply for the given argument - must return an {@link java.lang.Iterable}
     * @return a builder with the case statement added
     */
    public static PFBuilder<Object, Iterable<Object>> matchAny(final IterableApply<Object> apply) {
        return Match.matchAny(apply);
    }

    public interface IterableApply<T> extends FI.Apply<T, Iterable<Object>> {
    }
}
