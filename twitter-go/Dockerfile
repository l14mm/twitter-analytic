# Dev Dockerfile for live reloading
FROM golang:latest 

RUN mkdir /app 
ADD . /app/ 
WORKDIR /app 

ENV GOPATH /
ENV GOBIN $GOPATH/bin
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

RUN go get
RUN go install

RUN go build -o main .

# CMD ["/app/main"]
RUN go get github.com/cespare/reflex
COPY reflex.conf /
ENTRYPOINT ["reflex", "-c", "/reflex.conf"]