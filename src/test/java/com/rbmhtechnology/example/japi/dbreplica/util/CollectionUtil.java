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

package com.rbmhtechnology.example.japi.dbreplica.util;

import javaslang.control.Option;
import scala.collection.JavaConversions;
import scala.collection.immutable.Seq;

import java.util.Collection;

public final class CollectionUtil {

    private CollectionUtil() {
    }

    public static <T> Option<T> headOption(final Collection<T> collection) {
        return Option.when(!collection.isEmpty(), () -> collection.iterator().next());
    }

    public static <T> Seq<T> asScala(final Iterable<T> i) {
        return JavaConversions.iterableAsScalaIterable(i).toList();
    }

    public static <T> Collection<T> asJava(final Seq<T> seq) {
        return JavaConversions.seqAsJavaList(seq);
    }
}
