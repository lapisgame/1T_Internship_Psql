FROM grafana/grafana

USER root

# Install dependencies required by grafana-image-renderer plugin
RUN apk update && apk add chromium

USER grafana

# Install grafana-image-renderer plugin
RUN grafana cli plugins install grafana-image-renderer
