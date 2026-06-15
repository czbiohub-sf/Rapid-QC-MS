FROM mcr.microsoft.com/dotnet/runtime:8.0-noble

LABEL maintainer="Anthony Goering"
LABEL description="MS-DIAL Console 4.92 for LC-MS metabolomics feature detection"

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 && \
    rm -rf /var/lib/apt/lists/*

COPY MsdialConsoleApp_linux-x64/ /opt/msdial/
RUN chmod +x /opt/msdial/MsdialConsoleApp

ENV PATH="/opt/msdial:$PATH"

ENTRYPOINT ["MsdialConsoleApp"]
