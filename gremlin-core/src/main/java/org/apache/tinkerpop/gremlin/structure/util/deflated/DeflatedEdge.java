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

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DeflatedEdge extends DeflatedElement<Edge> implements Edge {

    private ReferenceVertex outVertex;
    private ReferenceVertex inVertex;


    protected DeflatedEdge(final Edge edge, final Map<String, Set<String>> components) {
        super(edge);
        this.outVertex = ReferenceFactory.detach(edge.outVertex());
        this.inVertex = ReferenceFactory.detach(edge.inVertex());
        if (components.containsKey(PROPERTIES)) {
            final Set<String> propertiesSet = components.get(PROPERTIES);
            final Iterator<Property<Object>> itty = propertiesSet.isEmpty() ?
                    edge.properties() :
                    edge.properties(propertiesSet.toArray(new String[propertiesSet.size()]));
            if (itty.hasNext() && null == this.properties) this.properties = new HashMap<>();
            while (itty.hasNext()) {
                final Property<Object> property = itty.next();
                if (!this.properties.containsKey(property.key()))
                    this.properties.put(property.key(), new ArrayList<>());
                this.properties.get(property.key()).add(new DeflatedProperty<>(property));
            }
        }
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    public Vertex inVertex() {
        return this.inVertex;
    }

    @Override
    public Vertex outVertex() {
        return this.outVertex;
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction) {
        switch (direction) {
            case OUT:
                return IteratorUtils.of(this.outVertex);
            case IN:
                return IteratorUtils.of(this.inVertex);
            default:
                return IteratorUtils.of(this.outVertex, this.inVertex);
        }
    }

    @Override
    public void remove() {
        throw Edge.Exceptions.edgeRemovalNotSupported();
    }

    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        return (Iterator) super.properties(propertyKeys);
    }
}