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

RUN go get github.com/oxequa/realize
CMD ["realize", "start"]