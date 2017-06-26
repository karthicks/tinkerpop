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
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DeflatedVertex extends DeflatedElement<Vertex> implements Vertex {

    public DeflatedVertex(final Vertex vertex, final Map<String, Set<String>> components) {
        super(vertex);
        if (components.containsKey(PROPERTIES)) {
            final Set<String> propertiesSet = components.get(PROPERTIES);
            final Iterator<VertexProperty<Object>> itty = propertiesSet.isEmpty() ?
                    vertex.properties() :
                    vertex.properties(propertiesSet.toArray(new String[propertiesSet.size()]));
            if(itty.hasNext() && null == this.properties) this.properties = new HashMap<>();
            while (itty.hasNext()) {
                final VertexProperty<Object> vertexProperty = itty.next();
                if (!this.properties.containsKey(vertexProperty.key()))
                    this.properties.put(vertexProperty.key(), new ArrayList<>());
                this.properties.get(vertexProperty.key()).add(new DeflatedVertexProperty<>(vertexProperty, components));
            }
        }
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        throw Element.Exceptions.propertyAdditionNotSupported();
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value, final Object... keyValues) {
        throw Element.Exceptions.propertyAdditionNotSupported();
    }

    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        throw Element.Exceptions.propertyAdditionNotSupported();
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        if (null != this.properties && this.properties.containsKey(key)) {
            final List<VertexProperty> list = (List) this.properties.get(key);
            if (list.size() > 1)
                throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
            else
                return list.get(0);
        } else
            return VertexProperty.<V>empty();
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        throw Vertex.Exceptions.edgeAdditionsNotSupported();
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        return (Iterator) super.properties(propertyKeys);
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        return Collections.emptyIterator();
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... labels) {
        return Collections.emptyIterator();
    }

    @Override
    public void remove() {
        throw Vertex.Exceptions.vertexRemovalNotSupported();
    }

}
