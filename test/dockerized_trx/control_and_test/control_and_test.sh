#!/bin/sh -e

while ! test `netaudio device list | grep jack | wc -l` -eq 2 ; do sleep 1; done
while ! test `netaudio device list | grep aplayrec | wc -l` -eq 2 ; do sleep 1; done

echo '✅ device list test passed'

netaudio subscription add --rx-device-name jack1 --rx-channel-name 'RX 1' --tx-device-name jack2 --tx-channel-name 'TX 1' &
netaudio subscription add --rx-device-name jack2 --rx-channel-name 'RX 1' --tx-device-name jack1 --tx-channel-name 'TX 1' &
netaudio subscription add --rx-device-name aplayrec1 --rx-channel-name 'RX 1' --tx-device-name aplayrec2 --tx-channel-name 'TX 1' &
netaudio subscription add --rx-device-name aplayrec2 --rx-channel-name 'RX 1' --tx-device-name aplayrec1 --tx-channel-name 'TX 1'

netaudio subscription add --rx-device-name jack1 --rx-channel-name 'RX 2' --tx-device-name jack2 --tx-channel-name 'TX 2' &
netaudio subscription add --rx-device-name jack2 --rx-channel-name 'RX 2' --tx-device-name jack1 --tx-channel-name 'TX 2' &
netaudio subscription add --rx-device-name aplayrec1 --rx-channel-name 'RX 2' --tx-device-name aplayrec2 --tx-channel-name 'TX 2' &
netaudio subscription add --rx-device-name aplayrec2 --rx-channel-name 'RX 2' --tx-device-name aplayrec1 --tx-channel-name 'TX 2'

wait

    echo '✅ after subscribe'

test `netaudio subscription list | grep jack | wc -l` -eq 4
test `netaudio subscription list | grep aplayrec | wc -l` -eq 4

echo '✅ subscription test passed, waiting for recorded audio'

cd /shared

while ! test \( -e done-jack1 \) -a \( -e done-jack2 \) -a \( -e done-aplayrec2 \) -a \( -e done-aplayrec2 \); do sleep 1; done

samples_expected=$((DURATION*INFERNO_SAMPLE_RATE))
tolerance=$INFERNO_SAMPLE_RATE
function test_recording {
    infile="rec-$1.raw"
    echo "Testing file $infile"

    samples=$((`stat -c %s $infile`/4))
    test $samples -gt $((samples_expected-tolerance))
    test $samples -lt $((samples_expected+tolerance))
    echo '  ✅ recording length test passed'

    seconds_nonsilent="$(sox -t raw -e signed-integer -b 16 -c 2 -r 48000 $infile -t raw -e signed-integer -b 16 -c 2 -r 48000 recns-$1.raw silence 1 0.1 1% stat 2>&1 | sed -E 's/^Length \(seconds\):\s+([0-9]+).*$/\1/; t; d')"
    echo "  seconds non-silent: $seconds_nonsilent"
    test $seconds_nonsilent -gt $((DURATION/2))
    echo '  ✅ signal presence test passed'

    seconds_both_channels="$(sox -t raw -e signed-integer -b 16 -c 2 -r 48000 $infile -t null null remix 2 silence 1 0.1 1% stat 2>&1 | sed -E 's/^Length \(seconds\):\s+([0-9]+).*$/\1/; t; d')"
    echo "  seconds non-silent both channels: $seconds_both_channels"
    test $seconds_both_channels -gt $((DURATION/2))
    echo '  ✅ signal presence test passed (both channels)'

    (/diff_tools/wd -align$samples_expected -ll play-$2.raw recns-$1.raw || true) | tee report-$1.txt
    grep '|MATCH' report-$1.txt
    echo '  ✅ longer channel recording matching'
    test `sed -E 's/^Compared:\s+([0-9]+).*$/\1/; t; d' < report-$1.txt` -gt $((DURATION*INFERNO_SAMPLE_RATE/2))
    echo '  ✅ longer channel recording long enough'

    sox -t raw -e signed-integer -b 16 -c 2 -r 48000 $infile -t raw -e signed-integer -b 16 -c 2 -r 48000 recnsbc-$1.raw trim $((samples-seconds_both_channels*INFERNO_SAMPLE_RATE))s
    /diff_tools/wd -align$samples_expected -ll play-$2.raw recnsbc-$1.raw | tee reportbc-$1.txt
    echo '  ✅ shorter channel recording matching'
    test `sed -E 's/^Compared:\s+([0-9]+).*$/\1/; t; d' < reportbc-$1.txt` -ge $((seconds_both_channels*INFERNO_SAMPLE_RATE*95/100))
    echo '  ✅ shorter channel recording long enough'

    dd if=/dev/zero of=recnsbc-$1.raw bs=1 seek=100000 count=2 conv=notrunc
    ! /diff_tools/wd -align$samples_expected -ll play-$2.raw recnsbc-$1.raw
    echo '  ✅ wd tool correctly detects damaged data'

    echo "✅ $infile all OK"
}

test_recording aplayrec1 aplayrec2
test_recording aplayrec2 aplayrec1

test_recording jack1 jack2
test_recording jack2 jack1

rm -f /shared/rec* /shared/play*
