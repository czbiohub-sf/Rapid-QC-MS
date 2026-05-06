FROM pytorch/pytorch:1.9.1-cuda11.1-cudnn8-runtime

LABEL maintainer="Anthony Goering"
LABEL description="MIST-CF molecular formula prediction with SIRIUS decomp"

# System deps for SIRIUS (Java) and building torch extensions
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-11-jre-headless \
        git \
    && rm -rf /var/lib/apt/lists/*

# Python deps for MIST-CF
RUN pip install --no-cache-dir \
    pytorch-lightning==1.6.5 \
    torch-geometric==2.0.4 \
    torch-sparse==0.6.18 \
    torch-spline-conv==1.2.2 \
    torchmetrics==1.2.1 \
    pandas==2.0.3 \
    scipy==1.10.1 \
    rdkit-pypi

# Install MIST-CF
COPY mist-cf/ /opt/mist-cf/
RUN cd /opt/mist-cf && pip install --no-cache-dir -e src/

# Install SIRIUS (decomp only — no login required)
COPY sirius/ /opt/sirius/
RUN chmod +x /opt/sirius/bin/sirius

ENV SIRIUS_PATH="/opt/sirius/bin/sirius"
ENV PATH="/opt/mist-cf:$PATH"
