Janus
=====

Janus converts XML to text-with-annotations, e.g.::

    <p>Hello, <b>world</b>!</p>

Becomes "Hello, world!" with annotations (p, 0, 12), (b, 7, 11).


Usage
-----

Compile and run::

    mvn clean package
    ./target/appassembler/bin/janus server

(or use the Dockerfile, ``docker run $(docker build -q .)``). Then POST
an XML file::

    curl -d '<p>Hello, <b>world</b>!</p>' http://localhost:8080
