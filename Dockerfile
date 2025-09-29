FROM debian:trixie-slim

ENV VIRTUAL_ENV=/opt/venv
ENV UV_PROJECT_ENVIRONMENT=$VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:/root/.local/bin/:$PATH"
ENV PYTHONUNBUFFERED=1

RUN apt -y update \
    && apt -y install curl git gdal-bin postgresql-client \
    && curl -LsSf https://astral.sh/uv/install.sh | sh \
    && uv venv $VIRTUAL_ENV \ 
    && apt clean \
    && apt autoremove -y \
    && rm -rf /var/lib/apt/lists/*