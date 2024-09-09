/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.maps.common.projection;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * An immutable replacement of {@link java.awt.geom.Point2D.java.awt.geom.Point2D.Double} to contain X and Y as doubles
 * and named in a way that will play nicely with all the other Point classes.
 */
@Data
@AllArgsConstructor
public class Double2D implements Serializable {
  private static final long serialVersionUID = 4586026386756756496L;

  private final double x;
  private final double y;
}
