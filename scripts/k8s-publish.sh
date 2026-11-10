#!/usr/bin/env bash
# Publish the local working tree (committed or not) as branch main of the in-cluster git server
# that Argo CD reads. Run it after editing anything under deploy/.
set -euo pipefail
source "$(dirname "$0")/k8s-lib.sh"
need git; need kubectl

tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
mkdir "$tmp/work"
# Tracked files plus new files that are not ignored; skip files deleted in the working tree.
git ls-files -co --exclude-standard | while IFS= read -r f; do
  [ -f "$f" ] && { mkdir -p "$tmp/work/$(dirname "$f")"; cp "$f" "$tmp/work/$f"; }
done
(
  cd "$tmp/work"
  git init -q -b main
  git -c user.name=local -c user.email=local@localhost add -A
  git -c user.name=local -c user.email=local@localhost commit -q -m "working tree $(date -u +%FT%TZ)"
  git clone -q --bare . "$tmp/mini-exchange.git"
)

pod="$(kubectl -n gitserver get pod -l app=gitserver -o jsonpath='{.items[0].metadata.name}')"
kubectl -n gitserver exec "$pod" -- sh -c 'rm -rf /srv/mini-exchange.git'
# COPYFILE_DISABLE: no macOS "._" metadata files in the archive.
COPYFILE_DISABLE=1 tar -C "$tmp" -cf - mini-exchange.git | kubectl -n gitserver exec -i "$pod" -- sh -c 'tar -C /srv -xf - && chown -R 0:0 /srv/mini-exchange.git'
kubectl -n gitserver exec "$pod" -- git ls-remote git://localhost/mini-exchange.git >/dev/null
echo "published the working tree to the in-cluster git server"
