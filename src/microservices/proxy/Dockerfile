FROM kong:3.9.1

ENV KONG_DATABASE=off
ENV KONG_DECLARATIVE_CONFIG=/etc/kong/kong.yml

COPY entrypoint.sh /entrypoint.sh
COPY genkong.lua /genkong.lua

USER root
RUN chmod +x /entrypoint.sh

USER kong
ENTRYPOINT ["/entrypoint.sh"]
CMD ["kong", "docker-start"]
