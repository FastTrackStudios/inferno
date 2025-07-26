FROM python:3.13-alpine3.22

# pip needs to compile dependencies of netaudio
RUN apk add build-base linux-headers sox

RUN CFLAGS="-Wno-int-conversion" pip install netaudio
ADD control_and_test.sh /
CMD [ "/control_and_test.sh" ]
