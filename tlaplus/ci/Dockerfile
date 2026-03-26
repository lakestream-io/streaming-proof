FROM talex5/tla:latest

WORKDIR /tlaplus

COPY algorithms/ algorithms/
COPY protocol/ protocol/
COPY models/ models/
COPY ci/run-tlc.sh ci/run-tlc.sh

ENTRYPOINT ["bash", "ci/run-tlc.sh"]
