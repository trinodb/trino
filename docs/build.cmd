docker run -e TRINO_VERSION -v %CD%:/docs ghcr.io/trinodb/build/sphinx:3 sphinx-build -j auto -b html -W -d target/doctrees src/main/sphinx target/html
