#!/bin/sh -e

while ! netaudio device list | grep inferno1 ; do sleep 1; done
while ! netaudio device list | grep inferno2 ; do sleep 1; done

echo 'device list test passed'

netaudio subscription add --rx-device-name inferno1 --rx-channel-name 'RX 1' --tx-device-name inferno2 --tx-channel-name 'TX 1'
netaudio subscription add --rx-device-name inferno1 --rx-channel-name 'RX 2' --tx-device-name inferno2 --tx-channel-name 'TX 2'
netaudio subscription add --rx-device-name inferno2 --rx-channel-name 'RX 1' --tx-device-name inferno1 --tx-channel-name 'TX 1'
netaudio subscription add --rx-device-name inferno2 --rx-channel-name 'RX 2' --tx-device-name inferno1 --tx-channel-name 'TX 2'

echo 'after subscribe'

test `netaudio subscription list | grep inferno | wc -l` -eq 4

echo 'subscription test passed'

sleep $DURATION

samples_expected=$((DURATION*INFERNO_SAMPLE_RATE))
tolerance=$INFERNO_SAMPLE_RATE
function test_recording {
    infile="/shared/rec-$1.raw"
    echo "Testing file $infile"
    samples=$((`stat -c %s $infile`/4))
    test $samples -gt $((samples_expected-tolerance))
    test $samples -lt $((samples_expected+tolerance))
    echo '  recording length test passed'
    seconds_nonsilent="$(sox -t raw -e signed-integer -b 16 -c 2 -r 48000 $infile -t null null silence 1 0.1 1% stat 2>&1 | sed -E 's/^Length \(seconds\):\s+([0-9]+).*$/\1/; t; d')"
    echo "  seconds non-silent: $seconds_nonsilent"
    test $seconds_nonsilent -gt $((DURATION/2))
    echo '  signal presence test passed'
}


test_recording inferno1
test_recording inferno2