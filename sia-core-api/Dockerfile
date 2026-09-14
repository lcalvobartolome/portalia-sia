FROM nvidia/cuda:12.1.1-base-ubuntu22.04@sha256:457a4076c56025f51217bff647ca631c7880ad3dbf546b03728ba98297ebbc22
ARG TARGETARCH

# Unprivileged runtime user
ARG APP_UID=1000
ARG APP_GID=1000

# Pin the pipeline dependency to an immutable commit SHA in deployment 
# Defaults to "main" for local dev; production builds MUST pass a SHA.
ARG PIPELINE_REF=main

# APT packages are resolved against a frozen Ubuntu archive snapshot for
# reproducible builds
ARG APT_SNAPSHOT=20260901T000000Z
# Python build front-end, pinned for reproducibility 
ARG PIP_VERSION=26.2.1
ARG SETUPTOOLS_VERSION=84.0.0
ARG WHEEL_VERSION=0.48.0
ENV TZ=Europe/Madrid
ENV DEBIAN_FRONTEND=noninteractive
ENV MALLET_VERSION=202108
ENV MALLET_URL="https://github.com/mimno/Mallet/releases/download/v${MALLET_VERSION}/Mallet-${MALLET_VERSION}-bin.tar.gz"
ENV MALLET_HOME=/opt/mallet
ENV PATH="$MALLET_HOME/bin:$PATH"

# Use bash with pipefail
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Install system dependencies.
RUN { echo "deb [check-valid-until=no] http://snapshot.ubuntu.com/ubuntu/${APT_SNAPSHOT} jammy main restricted universe multiverse"; \
      echo "deb [check-valid-until=no] http://snapshot.ubuntu.com/ubuntu/${APT_SNAPSHOT} jammy-updates main restricted universe multiverse"; \
      echo "deb [check-valid-until=no] http://snapshot.ubuntu.com/ubuntu/${APT_SNAPSHOT} jammy-security main restricted universe multiverse"; \
    } > /etc/apt/sources.list && \
    apt-get -o Acquire::Check-Valid-Until=false update && \
    apt-get install -y --no-install-recommends \
    openjdk-21-jdk \
    poppler-utils \
    python3.10 python3.10-dev python3.10-venv python3-pip \
    git ant wget curl \
    libbz2-dev libffi-dev libgdbm-dev libncurses5-dev \
    libnss3-dev libreadline-dev libsqlite3-dev libssl-dev \
    tzdata zlib1g-dev libpython3-dev \
    gcc g++ build-essential cmake \
    ca-certificates openssl && \
    update-ca-certificates && \
    rm -f /usr/lib/aarch64-linux-gnu/libGLX_indirect.so.0 && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /root/.cache/*

# Set JAVA_HOME dynamically based on architecture
RUN if [ "$TARGETARCH" = "arm64" ]; then \
        JAVA_PATH=/usr/lib/jvm/java-21-openjdk-arm64; \
    else \
        JAVA_PATH=/usr/lib/jvm/java-21-openjdk-amd64; \
    fi && \
    update-alternatives --set java ${JAVA_PATH}/bin/java && \
    echo "JAVA_HOME=${JAVA_PATH}" >> /etc/environment && \
    echo "export JAVA_HOME=${JAVA_PATH}" >> /etc/profile.d/java.sh && \
    ln -sf ${JAVA_PATH} /usr/lib/jvm/java-21-openjdk-current

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-current

# Verify Java version
RUN java -version

# Upgrade the build front-end to pinned versions (IDX-006)
RUN pip install --no-cache-dir --upgrade \
    "pip==${PIP_VERSION}" "setuptools==${SETUPTOOLS_VERSION}" "wheel==${WHEEL_VERSION}" && \
    pip cache purge

# Set working directory
WORKDIR /app

# Copy only requirements file first to optimize caching
COPY requirements.txt .

# Ensure pip uses updated certificates
RUN pip config set global.cert /etc/ssl/certs/ca-certificates.crt

# spaCy model wheel: download and verify its checksum before install (IDX-006),
# instead of letting pip fetch an unverified artifact from a GitHub release URL.
ARG ES_CORE_NEWS_LG_URL="https://github.com/explosion/spacy-models/releases/download/es_core_news_lg-3.8.0/es_core_news_lg-3.8.0-py3-none-any.whl"
ARG ES_CORE_NEWS_LG_SHA256="7c6c212715a12f31aacde3361754436945ff7376fb24cde57d0c277c9c9b050b"

# GITHUB_TOKEN is supplied only as a BuildKit secret
RUN --mount=type=secret,id=github_token,required=true \
    GITHUB_TOKEN="$(cat /run/secrets/github_token)" && \
    git clone "https://x-access-token:${GITHUB_TOKEN}@github.com/lcalvobartolome/portalia-pipeline.git" /opt/alia-pipeline && \
    git -C /opt/alia-pipeline checkout "${PIPELINE_REF}" && \
    git -C /opt/alia-pipeline rev-parse HEAD && \
    wget -q -O /tmp/es_core_news_lg-3.8.0-py3-none-any.whl "${ES_CORE_NEWS_LG_URL}" && \
    echo "${ES_CORE_NEWS_LG_SHA256}  /tmp/es_core_news_lg-3.8.0-py3-none-any.whl" | sha256sum -c - && \
    pip install --no-cache-dir --ignore-installed -e /opt/alia-pipeline && \
    pip install --no-cache-dir --ignore-installed -r requirements.txt && \
    pip install --no-cache-dir --ignore-installed /tmp/es_core_news_lg-3.8.0-py3-none-any.whl && \
    rm -f /tmp/es_core_news_lg-3.8.0-py3-none-any.whl && \
    rm -rf /opt/alia-pipeline/.git && \
    pip cache purge

# Mallet: verify the release tarball checksum before extracting (IDX-006).
ENV MALLET_SHA256=d64c77b00e3f1afbc48ed775f772ce7eccaaca77da4b9b581fb21dfe4a7f8a26
RUN wget -q $MALLET_URL -O /tmp/mallet.tar.gz && \
    echo "${MALLET_SHA256}  /tmp/mallet.tar.gz" | sha256sum -c - && \
    tar -xvzf /tmp/mallet.tar.gz -C /opt && \
    mv /opt/Mallet-${MALLET_VERSION} /opt/mallet && \
    rm /tmp/mallet.tar.gz && \
    rm -rf /opt/mallet/sample-data  # Remove sample data to reduce image size

RUN mkdir -p /mnt/data_place /data/inference

# Persistent Hugging Face cache baked into the image itself
ENV HF_HOME=/opt/hf_cache
ENV TRANSFORMERS_CACHE=/opt/hf_cache
ENV SENTENCE_TRANSFORMERS_HOME=/opt/hf_cache

# Pre-download the embeddings model into that cache, pinned to the audited
# commit. huggingface_hub only writes a local refs/<revision> pointer file
# when the requested revision is NOT already a full commit sha, so pinning
# an exact sha here means no refs/main gets written. But the runtime
# EmbeddingsManager (in alia_pipeline) loads the model by name only, with
# no revision pin, which defaults to "main"; with HF_HUB_OFFLINE=1 below
# that lookup needs a local refs/main to resolve without network. So after
# downloading the pinned commit we write refs/main ourselves next to
# wherever its snapshot actually landed (searched for, not assumed, since
# TRANSFORMERS_CACHE/SENTENCE_TRANSFORMERS_HOME above can route different
# loaders to different cache subpaths under /opt/hf_cache).
ARG MODEL_NAME=hiiamsid/sentence_similarity_spanish_es
ARG MODEL_REVISION=66ab46adac3910bb6ea6085b962a25e49513b981
RUN python3 -c "from pathlib import Path; from sentence_transformers import SentenceTransformer; model_name='${MODEL_NAME}'; revision='${MODEL_REVISION}'; SentenceTransformer(model_name, revision=revision); snapshot_dirs = list(Path('/opt/hf_cache').rglob('snapshots/' + revision)); assert snapshot_dirs, f'no snapshot for {model_name}@{revision} found under /opt/hf_cache'; [((d.parent.parent / 'refs').mkdir(parents=True, exist_ok=True), (d.parent.parent / 'refs' / 'main').write_text(revision), print(f'Wrote {d.parent.parent}/refs/main -> {revision}')) for d in snapshot_dirs]"

# Fail fast instead of silently trying (and hanging) to hit the network if the cache is ever missing something once the container is moved/offline.
ENV HF_HUB_OFFLINE=1

COPY . .

# Drop root: create a service user/group and hand it the paths written or
# cached at runtime
RUN (getent group "${APP_GID}" || groupadd -g "${APP_GID}" app) && \
    (getent passwd "${APP_UID}" || useradd -l --no-log-init -u "${APP_UID}" -g "${APP_GID}" -m -s /usr/sbin/nologin app) && \
    mkdir -p /mnt/data_place /data/inference /config && \
    chown -R "${APP_UID}:${APP_GID}" /app /mnt/data_place /data /config /opt/hf_cache /opt/mallet

USER ${APP_UID}:${APP_GID}

EXPOSE 10083
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "10083", "--workers", "2"]