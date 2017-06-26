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

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DeflatedFactory {

    private DeflatedFactory() {
    }

    public static DeflatedVertex detach(final Vertex vertex, final Map<String, Set<String>> components) {
        return new DeflatedVertex(vertex, components);
    }

    public static DeflatedEdge detach(final Edge edge, final Map<String, Set<String>> components) {
        return new DeflatedEdge(edge, components);
    }

    public static <V> DeflatedVertexProperty detach(final VertexProperty<V> vertexProperty, final Map<String, Set<String>> components) {
        return new DeflatedVertexProperty<>(vertexProperty, components);
    }

    public static <V> DeflatedProperty<V> detach(final Property<V> property) {
        return new DeflatedProperty<V>(property);
    }

    public static DeflatedPath detach(final Path path, final Map<String, Set<String>> components) {
        return new DeflatedPath(path, components);
    }

    public static DeflatedElement detach(final Element element, final Map<String, Set<String>> components) {
        if (element instanceof Vertex)
            return detach((Vertex) element, components);
        else if (element instanceof Edge)
            return detach((Edge) element, components);
        else if (element instanceof VertexProperty)
            return detach((VertexProperty) element, components);
        else
            throw new IllegalArgumentException("The provided argument is an unknown element: " + element + ':' + element.getClass());
    }

    public static <D> D detach(final Object object, final Map<String, Set<String>> components) {
        if (object instanceof Element) {
            return (D) DeflatedFactory.detach((Element) object, components);
        } else if (object instanceof Property) {
            return (D) DeflatedFactory.detach((Property) object);
        } else if (object instanceof Path) {
            return (D) DeflatedFactory.detach((Path) object, components);
        } else {
            return (D) object;
        }
    }
}
