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

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DeflatedVertexProperty<V> extends DeflatedElement<VertexProperty<V>> implements VertexProperty<V> {

    protected V value;
    protected transient ReferenceVertex vertex;

    public DeflatedVertexProperty(final VertexProperty<V> vertexProperty, final Map<String, Set<String>> components) {
        super(vertexProperty);
        this.value = vertexProperty.value();
        this.vertex = ReferenceFactory.detach(vertexProperty.element());
        if (components.containsKey(PROPERTIES)) {
            final Set<String> propertiesSet = components.get(PROPERTIES);
            final Iterator<Property<Object>> itty = propertiesSet.isEmpty() ?
                    vertexProperty.properties() :
                    vertexProperty.properties(propertiesSet.toArray(new String[propertiesSet.size()]));
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
    public boolean isPresent() {
        return true;
    }

    @Override
    public String key() {
        return this.label;
    }

    @Override
    public V value() {
        return this.value;
    }

    @Override
    public Vertex element() {
        return this.vertex;
    }

    @Override
    public void remove() {
        throw Property.Exceptions.propertyRemovalNotSupported();
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
        return (Iterator) super.properties(propertyKeys);
    }
}
