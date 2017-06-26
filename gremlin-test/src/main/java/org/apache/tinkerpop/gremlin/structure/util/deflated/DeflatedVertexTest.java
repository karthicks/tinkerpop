/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.structure.util.deflated;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.FeatureRequirementSet;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DeflatedVertexTest extends AbstractGremlinTest {

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldIteratePropertiesOnDeflate() {
        final Vertex v = graph.addVertex("name", "daniel", "favoriteColor", "red", "state", "happy");
        // ALL PROPERTIES
        Vertex deflated = DeflatedFactory.detach(v, new HashMap<String, Set<String>>() {{
            put("properties", new HashSet<>(Arrays.asList("name", "favoriteColor", "state")));
        }});
        final AtomicInteger counter = new AtomicInteger(0);
        assertTrue(deflated.properties().hasNext());
        deflated.properties().forEachRemaining(p -> {
            counter.incrementAndGet();
            if (p.key().equals("name"))
                assertEquals("daniel", p.value());
            else if (p.key().equals("favoriteColor"))
                assertEquals("red", p.value());
            else if (p.key().equals("state"))
                assertEquals("happy", p.value());
            else
                fail("Should be one of the expected keys");
        });
        assertEquals(3, counter.get());
        // SOME PROPERTIES
        counter.set(0);
        deflated = DeflatedFactory.detach(v, new HashMap<String, Set<String>>() {{
            put("properties", new HashSet<>(Arrays.asList("name", "favoriteColor")));
        }});
        assertTrue(deflated.properties().hasNext());
        deflated.properties().forEachRemaining(p -> {
            counter.incrementAndGet();
            if (p.key().equals("name"))
                assertEquals("daniel", p.value());
            else if (p.key().equals("favoriteColor"))
                assertEquals("red", p.value());
            else
                fail("Should be one of the expected keys");
        });
        assertEquals(2, counter.get());
        // NO PROPERTIES
        deflated = DeflatedFactory.detach(v, Collections.emptyMap());
        assertFalse(deflated.properties().hasNext());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldHashAndEqualCorrectly() {
        final Vertex v = graph.addVertex("name","blah");
        final Set<Vertex> set = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            set.add(DeflatedFactory.detach(v, Collections.emptyMap()));
            set.add(DeflatedFactory.detach(v, new HashMap<String, Set<String>>() {{
                put("properties", new HashSet<>(Arrays.asList("name")));
            }}));
            set.add(v);
        }
        assertEquals(1, set.size());
    }
}