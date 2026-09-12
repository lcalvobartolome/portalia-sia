FROM docker.io/library/solr:9.1.1@sha256:8e97c5151e169763c4f786dec78e48063fa74245dcab2650a446214553adb89f

# bash + pipefail so a failed download is not masked by `sha256sum -c` (DL4006)
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

USER root
ARG NP_PLUGIN_REF=974a1b7f56c2a639a2b8994e728e0fdee8acc5c3
ARG NP_PLUGIN_SHA256=8dec250adbdcd69d689f5539de5db9713717f3c9bb44fd5c04d7aba5db934a3f
RUN mkdir -p /opt/solr/dist/plugins && \
    wget -q -O /opt/solr/dist/plugins/NP-solr-dist-plugin.jar \
      "https://github.com/nextprocurement/NP-solr-dist-plugin/raw/${NP_PLUGIN_REF}/NP-solr-dist-plugin.jar" && \
    echo "${NP_PLUGIN_SHA256}  /opt/solr/dist/plugins/NP-solr-dist-plugin.jar" | sha256sum -c -
USER 8983
