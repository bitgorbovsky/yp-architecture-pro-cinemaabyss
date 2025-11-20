FROM kong:3.9.1

ENV KONG_DATABASE=off
ENV KONG_DECLARATIVE_CONFIG=/etc/kong/kong.yml
ENV KONG_NGINX_PROXY_INCLUDE=/etc/kong/healthcheck.conf

COPY entrypoint.sh /entrypoint.sh
COPY genkong.lua /genkong.lua
COPY healthcheck.conf /etc/kong/healthcheck.conf

USER root
RUN chmod +x /entrypoint.sh

USER kong
ENTRYPOINT ["/entrypoint.sh"]
CMD ["kong", "docker-start"]
