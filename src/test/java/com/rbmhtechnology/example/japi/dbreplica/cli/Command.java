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

package com.rbmhtechnology.example.japi.dbreplica.cli;

import javaslang.Tuple;
import javaslang.Tuple2;
import javaslang.collection.HashMap;
import javaslang.collection.List;
import javaslang.collection.Map;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

enum Command {
    CREATE("^create (?<id>\\d+) (?<subject>\\w+) (?<content>\\w+)$", Parameters.ID, Parameters.SUBJECT, Parameters.CONTENT),
    SUBJECT("^subject (?<id>\\d+) (?<subject>\\w+)$", Parameters.ID, Parameters.SUBJECT),
    CONTENT("^content (?<id>\\d+) (?<content>\\w+)$", Parameters.ID, Parameters.CONTENT),
    LIST("^list$"),
    UNKNOWN("$");

    private final Pattern pattern;
    private final List<String> paramNames;

    Command(final String regex, final String... paramNames) {
        this.pattern = Pattern.compile(regex);
        this.paramNames = List.ofAll(Arrays.asList(paramNames));
    }

    public static Tuple2<Command, Map<String, String>> fromString(final String str) {
        for (Command command : Command.values()) {
            final Matcher m = command.pattern.matcher(str);

            if (m.matches()) {
                return Tuple.of(command, getValues(m, command.paramNames));
            }
        }
        return Tuple.of(UNKNOWN, HashMap.empty());
    }

    private static Map<String, String> getValues(final Matcher matcher, final List<String> paramNames) {
        return paramNames.toMap(param -> Tuple.of(param, matcher.group(param)));
    }

    public class Parameters {
        public static final String ID = "id";
        public static final String SUBJECT = "subject";
        public static final String CONTENT = "content";
    }
}
