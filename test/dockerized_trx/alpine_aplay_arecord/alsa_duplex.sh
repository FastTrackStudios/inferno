#!/bin/sh -e

# TMPDIR change is needed as a workaround for datagram sockets not working correctly across containers
mkdir -p /shared/tmp-$INFERNO_NAME
export TMPDIR=/shared/tmp-$INFERNO_NAME

rm -rf $HOME/.local/state/inferno_aoip || true
head -c$((INFERNO_SAMPLE_RATE * 2 * 2 * DURATION)) < /dev/urandom > /shared/play-$INFERNO_NAME.raw

sox -t raw -c 2 -r $INFERNO_SAMPLE_RATE -e signed-integer -b 16 /shared/play-$INFERNO_NAME.raw -e signed-integer -b 32 /shared/play-native-$INFERNO_NAME.wav

aplay -D inferno /shared/play-native-$INFERNO_NAME.wav &
arecord -D inferno -d $DURATION -c 2 -r $INFERNO_SAMPLE_RATE -f S32_LE /shared/rec-native-$INFERNO_NAME.wav

sox --no-dither /shared/rec-native-$INFERNO_NAME.wav -t raw -c 2 -r $INFERNO_SAMPLE_RATE -e signed-integer -b 16 /shared/rec-$INFERNO_NAME.raw


echo done > /shared/done-$INFERNO_NAME
