FROM fedora:28

RUN dnf install -y libgo

COPY elasticsearch /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/elasticsearch"]
CMD ["/queue/input"]


