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
package org.gbif.maps.resource;


import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.MethodParameter;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.http.server.ServletServerHttpResponse;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyAdvice;

/**
 * Sets a 204 for any response that is a NULL object but indicating a 200.
 */
@ControllerAdvice
public class NoContentResponseFilter implements ResponseBodyAdvice<Object> {

  @Override
  public boolean supports(
    MethodParameter returnType, Class<? extends HttpMessageConverter<?>> converterType
  ) {
    return returnType.getParameterType().isAssignableFrom(byte[].class);
  }

  @Override
  public Object beforeBodyWrite(
    Object body,
    MethodParameter returnType,
    MediaType selectedContentType,
    Class<? extends HttpMessageConverter<?>> selectedConverterType,
    ServerHttpRequest request,
    ServerHttpResponse response
  ) {
    if (((ServletServerHttpResponse)response).getServletResponse().getStatus() == HttpServletResponse.SC_OK &&
        body != null && ((byte[])body).length == 0) {
      response.setStatusCode(HttpStatus.NO_CONTENT);
    }
    return body;
  }

}
