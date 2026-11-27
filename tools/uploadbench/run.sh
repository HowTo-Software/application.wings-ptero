#!/usr/bin/env bash
# Runs the upload benchmark matrix for ONE network profile and prints JSON lines.
#
#   tools/uploadbench/run.sh <profile-name> "<netem args or 'none'>" <size-mib> <window-seconds> <runs> [client-netem-extra]
#
# Topology: the Wings router runs in the root network namespace, the client in namespace
# "bclient", joined by a veth pair (real, separate TCP stacks; MTU 1500; TSO/GSO/GRO off so
# netem sees real packets). The netem profile is applied to the egress of BOTH ends, so the
# round-trip time is twice the configured delay and loss applies in each direction.
# client-netem-extra (e.g. "rate 100mbit") is applied to the client->server direction only.
#
# Every number printed is a measurement of this emulated path on this machine. Nothing here
# predicts the behaviour of the real internet path between a user and a node.
set -euo pipefail

PROFILE=${1:?profile}
NETEM=${2:?netem args or none}
SIZE=${3:-192}
WINDOW=${4:-60}
RUNS=${5:-2}
CLIENT_EXTRA=${6:-}

BIN=${UPLOADBENCH_BIN:-/tmp/uploadbench}
WORK=$(mktemp -d)
SECRET=bench-secret
ADDR=10.200.0.1:18443
NS=bclient

cleanup() {
  set +e
  [ -n "${SERVER_PID:-}" ] && kill "$SERVER_PID" 2>/dev/null
  sudo ip netns del $NS 2>/dev/null
  sudo ip link del vs 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT

sudo ip netns add $NS
sudo ip link add vs type veth peer name vc
sudo ip link set vc netns $NS
sudo ip addr add 10.200.0.1/24 dev vs
sudo ip link set vs up
sudo ip netns exec $NS ip addr add 10.200.0.2/24 dev vc
sudo ip netns exec $NS ip link set vc up
sudo ip netns exec $NS ip link set lo up
sudo ethtool -K vs tso off gso off gro off >/dev/null 2>&1 || true
sudo ip netns exec $NS ethtool -K vc tso off gso off gro off >/dev/null 2>&1 || true

# netem releases packets that were delayed together in one burst; the default per-CPU backlog (1000 packets) of
# the receiving veth then drops them, which would show up as retransmissions that the profile never asked for.
sudo sysctl -qw net.core.netdev_max_backlog=500000 2>/dev/null || true
# Optional: do not collapse the congestion window after an idle period (affects ONE connection sending
# consecutive requests, like a browser sending chunk after chunk).
if [ -n "${SSAI:-}" ]; then sudo ip netns exec $NS sysctl -qw net.ipv4.tcp_slow_start_after_idle="$SSAI" 2>/dev/null || true; fi

NETEM_OK=true
if [ "$NETEM" != "none" ]; then
  sudo modprobe sch_netem 2>/dev/null || true
  # A large queue so the emulated delay (a big bandwidth-delay product) does not itself drop packets.
  if ! sudo tc qdisc add dev vs root netem $NETEM limit 200000 2>/dev/null; then NETEM_OK=false; fi
  if ! sudo ip netns exec $NS tc qdisc add dev vc root netem $NETEM $CLIENT_EXTRA limit 200000 2>/dev/null; then NETEM_OK=false; fi
fi
if [ "$NETEM_OK" != true ]; then
  echo "{\"profile\":\"$PROFILE\",\"skipped\":\"tc netem is not available on this runner\"}"
  exit 0
fi

"$BIN" serve --dir "$WORK/wings" --listen "$ADDR" --token "$SECRET" --h2-window-mib "${H2_WINDOW_MIB:-0}" >"$WORK/server.log" 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 100); do grep -q READY "$WORK/server.log" 2>/dev/null && break; sleep 0.2; done
grep -q READY "$WORK/server.log" || { echo "server did not start"; cat "$WORK/server.log"; exit 1; }

CLK=$(getconf CLK_TCK)
cpu_ticks() { awk '{print $14+$15}' /proc/$SERVER_PID/stat; }
vm_hwm_kb() { awk '/VmHWM/ {print $2}' /proc/$SERVER_PID/status; }
write_bytes() { awk '/^write_bytes/ {print $2}' /proc/$SERVER_PID/io; }
retrans() { sudo ip netns exec $NS awk '/^Tcp:/ && !h {for(i=1;i<=NF;i++) c[$i]=i; h=1; next} /^Tcp:/ {print $c["OutSegs"], $c["RetransSegs"]}' /proc/net/snmp; }

KERNEL=$(uname -r); CPU=$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs); NPROC=$(nproc)
VOL=$(find "$WORK/wings/volumes" -mindepth 1 -maxdepth 1 -type d | head -1)

run_one() { # mode proto conns
  local mode=$1 proto=$2 conns=$3
  for run in $(seq 1 "$RUNS"); do
    local t0 w0 r0 o0
    t0=$(cpu_ticks); w0=$(write_bytes); read -r o0 r0 < <(retrans)
    local tf="$WORK/time.$mode.$proto.$conns.$run"
    local out
    out=$(sudo ip netns exec $NS /usr/bin/time -f "%U %S %M" -o "$tf" "$BIN" run --url "https://$ADDR" --token "$SECRET" \
          --mode "$mode" --proto "$proto" --conns "$conns" --size-mib "$SIZE" --max-seconds "$WINDOW" 2>"$WORK/client.err" || true)
    local t1 w1 r1 o1
    t1=$(cpu_ticks); w1=$(write_bytes); read -r o1 r1 < <(retrans)
    read -r cu cs crss < "$tf" || { cu=0; cs=0; crss=0; }
    python3 - "$PROFILE" "$out" "$t0" "$t1" "$CLK" "$w0" "$w1" "$o0" "$o1" "$r0" "$r1" "$(vm_hwm_kb)" "$cu" "$cs" "$crss" "$KERNEL" "$CPU" "$NPROC" "$NETEM $CLIENT_EXTRA" "${H2_WINDOW_MIB:-0}" "${SSAI:-default}" <<'PY'
import json, sys
a = sys.argv
try:
    r = json.loads(a[2])
except Exception:
    r = {"error": "client produced no result: " + a[2][:200]}
clk = float(a[5])
sent = r.get("sent_bytes", 0) or 0
out_segs, retr = int(a[9]) - int(a[8]), int(a[11]) - int(a[10])
r.update({
    "profile": a[1],
    "server_cpu_s": round((int(a[4]) - int(a[3])) / clk, 3),
    "server_cpu_ms_per_mb": round(1000 * ((int(a[4]) - int(a[3])) / clk) / (sent / 1e6), 3) if sent else None,
    "server_peak_rss_mib": round(int(a[12]) / 1024, 1),
    "server_disk_write_mib": round((int(a[7]) - int(a[6])) / 2**20, 1),
    "client_cpu_s": round(float(a[13]) + float(a[14]), 3),
    "client_peak_rss_mib": round(int(a[15]) / 1024, 1),
    "tcp_out_segs": out_segs, "tcp_retrans_segs": retr,
    "retrans_pct": round(100 * retr / out_segs, 3) if out_segs else None,
    "env": {"kernel": a[16], "cpu": a[17], "nproc": int(a[18]), "netem": a[19].strip(), "h2_window_mib": a[20], "slow_start_after_idle": a[21]},
})
print(json.dumps(r))
PY
    # keep the volume small between runs
    find "$VOL" -mindepth 1 -delete 2>/dev/null || true
    sudo sync || true
  done
}

if [ "${ONLY:-all}" = "h1one" ]; then
  run_one chunked h1 1
elif [ "${ONLY:-all}" = "h2" ]; then
  run_one legacy h2 1
  run_one chunked h2 1
  run_one chunked h2 8
else
  run_one legacy h1 1
  run_one legacy h2 1
  for n in 1 2 4 8; do run_one chunked h1 "$n"; done
  run_one chunked h2 8
fi
