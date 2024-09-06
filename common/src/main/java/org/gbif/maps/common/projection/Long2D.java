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
 * An immutable replace of {@link java.awt.Point} to contain X and Y but increaed to use longs and named in a way that
 * will play nicely with all the other Point classes.
 */
@Data
@AllArgsConstructor
public class Long2D implements Serializable {
  private static final long serialVersionUID = 5180346999387460314L;

  private final long x;
  private final long y;
}
