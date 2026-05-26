#!/usr/bin/env bash
# Install (or re-install) the irene-soak systemd-user timer.
#
# Idempotent: symlinks the in-repo unit files into ~/.config/systemd/user/,
# reloads systemd, enables and starts the timer. Re-running picks up any
# edits to the unit files in this directory.
#
# After install:
#   systemctl --user list-timers | grep irene-soak
#   journalctl --user -u irene-soak -n 50

set -euo pipefail

SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEST_DIR="${HOME}/.config/systemd/user"

mkdir -p "${DEST_DIR}"

for unit in irene-soak.service irene-soak.timer; do
    ln -sf "${SRC_DIR}/${unit}" "${DEST_DIR}/${unit}"
    echo "linked ${DEST_DIR}/${unit} -> ${SRC_DIR}/${unit}"
done

systemctl --user daemon-reload
systemctl --user enable --now irene-soak.timer

echo
echo "--- next scheduled fire ---"
systemctl --user list-timers irene-soak.timer --no-pager
