FROM node:8 AS builder

COPY src/main/node /usr/src/mapnik-server
WORKDIR /usr/src/mapnik-server
RUN npm install --registry https://repository.gbif.org/content/repositories/npmjs/

FROM node:8
LABEL maintainers="Matthew Blissett <mblissett@gbif.org>"

COPY --from=builder /usr/src/mapnik-server /usr/local/gbif/mapnik-server
RUN mkdir /usr/local/gbif/conf
VOLUME /usr/local/gbif/conf
COPY conf/server.yml /usr/local/gbif/conf

EXPOSE 8080
WORKDIR /usr/local/gbif/mapnik-server
CMD ["/usr/local/bin/node", "server.js", "/usr/local/gbif/conf/server.yml", "8080", "mapnik-server"]
