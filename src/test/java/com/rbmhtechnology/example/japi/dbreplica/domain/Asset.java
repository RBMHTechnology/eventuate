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

package com.rbmhtechnology.example.japi.dbreplica.domain;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class Asset {

    private final String id;
    private final String subject;
    private final String content;

    public Asset(final String id, final String subject, final String content) {
        this.id = id;
        this.subject = subject;
        this.content = content;
    }

    public String getId() {
        return id;
    }

    public String getSubject() {
        return subject;
    }

    public String getContent() {
        return content;
    }

    public Asset setSubject(final String subject) {
        return new Asset(this.id, subject, this.content);
    }

    public Asset setContent(final String content) {
        return new Asset(this.id, this.subject, content);
    }

    @Override
    public boolean equals(final Object that) {
        return EqualsBuilder.reflectionEquals(this, that);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this, false);
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.SIMPLE_STYLE);
    }
}
