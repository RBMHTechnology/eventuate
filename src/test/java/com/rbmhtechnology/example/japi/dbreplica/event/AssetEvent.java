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

package com.rbmhtechnology.example.japi.dbreplica.event;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import java.io.Serializable;
import java.util.UUID;

public abstract class AssetEvent implements Serializable {
    public final String assetId;

    protected AssetEvent(final String assetId) {
        this.assetId = assetId;
    }

    @Override
    public boolean equals(final Object other) {
        return EqualsBuilder.reflectionEquals(this, other);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this, false);
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

    public static class AssetCreated extends AssetEvent {
        public final String subject;
        public final String content;

        public AssetCreated(final String id, final String subject, final String content) {
            super(id);
            this.subject = subject;
            this.content = content;
        }

        public static AssetCreated create(final String subject, final String content) {
            return new AssetCreated(UUID.randomUUID().toString(), subject, content);
        }
    }

    public static class AssetSubjectUpdated extends AssetEvent {
        public final String subject;

        public AssetSubjectUpdated(final String id, final String subject) {
            super(id);
            this.subject = subject;
        }
    }

    public static class AssetContentUpdated extends AssetEvent {
        public final String content;

        public AssetContentUpdated(final String id, final String content) {
            super(id);
            this.content = content;
        }
    }
}
