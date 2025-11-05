#!/usr/bin/env sh

# Portable TLS certificate checker for running directly on a server.
# - Reads targets from a file (default: targets.txt)
# - For each host:port, fetches the leaf certificate via openssl
# - Computes days left and sends an email alert if below threshold
#
# Requirements: openssl, date (GNU coreutils recommended), awk, sed
# Optional: mailx or mail command for sending emails

set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"

# Defaults (can be overridden by env or CLI)
TARGETS_FILE="${TARGETS_FILE:-$SCRIPT_DIR/targets.txt}"
ALERT_DAYS_DEFAULT="${ALERT_DAYS_DEFAULT:-30}"
# Local mailbox delivery by default: deliver to system user mailbox (readable via `mailx`)
MAILBOX_USER_DEFAULT="$(id -un 2>/dev/null || echo root)"
MAIL_TO="${MAIL_TO:-$MAILBOX_USER_DEFAULT}"
MAIL_FROM="${MAIL_FROM:-cert-checker@$(hostname -f 2>/dev/null || hostname)}"
MAIL_SUBJECT_PREFIX="${MAIL_SUBJECT_PREFIX:-[TLS-CERT]}"
# If true, force local delivery via sendmail and address without domain
MAIL_LOCAL_ONLY="${MAIL_LOCAL_ONLY:-true}"
QUIET="false"

usage() {
  cat <<EOF
Usage: $0 [-f targets_file] [-d alert_days] [-q]

Options:
  -f FILE   Path to targets file (default: $TARGETS_FILE)
  -d DAYS   Alert threshold in days (default: $ALERT_DAYS_DEFAULT)
  -q        Quiet mode (only errors/alerts)

Environment overrides:
  TARGETS_FILE, ALERT_DAYS_DEFAULT, MAIL_TO, MAIL_FROM, MAIL_SUBJECT_PREFIX, MAIL_LOCAL_ONLY

Targets file format:
  - One entry per line, comments start with '#', blank lines ignored
  - Format: host[:port][,sni=example.com][,days=NN]
    Examples:
      example.com
      example.com:443
      10.0.0.5:8443,sni=service.example.com
      api.example.com:443,days=15

Exit codes:
  0 on success, 1 if any checks failed or errors occurred
EOF
}

log() {
  if [ "$QUIET" != "true" ]; then
    echo "$@"
  fi
}

warn() {
  printf "%s\n" "$@" 1>&2
}

have_cmd() { command -v "$1" >/dev/null 2>&1; }

send_mail() {
  subject="$1"
  to_addr="$2"
  from_addr="$3"
  body="$4"

  # Prefer local delivery via sendmail if MAIL_LOCAL_ONLY=true
  if [ "$MAIL_LOCAL_ONLY" = "true" ] && [ -x /usr/sbin/sendmail ]; then
    {
      printf "From: %s\n" "$from_addr"
      printf "To: %s\n" "$to_addr"
      printf "Subject: %s\n" "$subject"
      printf "Content-Type: text/plain; charset=UTF-8\n\n"
      printf "%s\n" "$body"
    } | /usr/sbin/sendmail -t || return 1
    return 0
  fi

  # Otherwise try mailx, then mail, then sendmail
  if have_cmd mailx; then
    printf "%s" "$body" | mailx -a "From: $from_addr" -s "$subject" "$to_addr" || return 1
    return 0
  fi
  if have_cmd mail; then
    printf "%s" "$body" | mail -a "From: $from_addr" -s "$subject" "$to_addr" || return 1
    return 0
  fi
  if [ -x /usr/sbin/sendmail ]; then
    {
      printf "From: %s\n" "$from_addr"
      printf "To: %s\n" "$to_addr"
      printf "Subject: %s\n" "$subject"
      printf "Content-Type: text/plain; charset=UTF-8\n\n"
      printf "%s\n" "$body"
    } | /usr/sbin/sendmail -t || return 1
    return 0
  fi
  warn "No mailer (sendmail/mailx/mail) available to send alerts"
  return 1
}

parse_kv() {
  key="$1"; shift
  echo "$@" | awk -v k="$key" -F',' '{
    for (i=1;i<=NF;i++) {
      split($i,a,"=")
      if (a[1]==k) { print a[2]; exit }
    }
  }'
}

parse_hostport() {
  # input like: host:port or host
  in="$1"
  host=$(printf "%s" "$in" | awk -F',' '{print $1}' | awk -F':' '{print $1}')
  port=$(printf "%s" "$in" | awk -F',' '{print $1}' | awk -F':' '{print $2}')
  if [ -z "$port" ]; then port=443; fi
  printf "%s %s\n" "$host" "$port"
}

openssl_fetch_cert() {
  host="$1"; port="$2"; sni="$3"
  # We request the leaf cert and pipe to x509
  # shellcheck disable=SC3037
  if [ -n "$sni" ]; then
    echo | openssl s_client -servername "$sni" -connect "$host:$port" -showcerts 2>/dev/null |
      awk 'BEGIN{incrt=0} /BEGIN CERTIFICATE/{incrt=1} incrt{print} /END CERTIFICATE/{exit}'
  else
    echo | openssl s_client -connect "$host:$port" -showcerts 2>/dev/null |
      awk 'BEGIN{incrt=0} /BEGIN CERTIFICATE/{incrt=1} incrt{print} /END CERTIFICATE/{exit}'
  fi
}

cert_enddate_epoch() {
  # Reads a PEM cert on stdin, outputs notAfter epoch seconds
  openssl x509 -noout -enddate 2>/dev/null | sed 's/notAfter=//' |
    while IFS= read -r line; do
      if date -d "$line" +%s >/dev/null 2>&1; then
        date -d "$line" +%s
      else
        # macOS/BSD fallback: try -j -f
        if date -j -f "%b %e %T %Y %Z" "$line" +%s 2>/dev/null; then
          date -j -f "%b %e %T %Y %Z" "$line" +%s
        else
          echo ""; exit 1
        fi
      fi
    done
}

calc_days_left() {
  end_epoch="$1"
  now_epoch=$(date +%s)
  if [ -z "$end_epoch" ]; then
    echo ""; return 1
  fi
  if [ "$end_epoch" -lt "$now_epoch" ]; then
    echo 0
    return 0
  fi
  seconds_left=$(( end_epoch - now_epoch ))
  days_left=$(( (seconds_left + 86399) / 86400 ))
  echo "$days_left"
}

alert_block=""
had_error=0

while getopts ":f:d:qh" opt; do
  case "$opt" in
    f) TARGETS_FILE="$OPTARG" ;;
    d) ALERT_DAYS_DEFAULT="$OPTARG" ;;
    q) QUIET="true" ;;
    h) usage; exit 0 ;;
    :) warn "Option -$OPTARG requires an argument"; usage; exit 1 ;;
    \?) warn "Unknown option: -$OPTARG"; usage; exit 1 ;;
  esac
done

if [ ! -r "$TARGETS_FILE" ]; then
  warn "Targets file not readable: $TARGETS_FILE"
  exit 1
fi

log "Using targets file: $TARGETS_FILE"
log "Default alert threshold: $ALERT_DAYS_DEFAULT days"

line_no=0
while IFS= read -r raw || [ -n "$raw" ]; do
  line_no=$(( line_no + 1 ))
  # Trim
  line=$(echo "$raw" | sed 's/^\s\+//;s/\s\+$//')
  # Skip comments/blank
  case "$line" in
    "#"*|"") continue ;;
  esac

  base=$(echo "$line" | awk -F',' '{print $1}')
  kvs=$(echo "$line" | cut -s -d',' -f2-)
  set -- $(parse_hostport "$base")
  host="$1"; port="$2"
  sni="$(parse_kv sni "$kvs")"
  override_days="$(parse_kv days "$kvs")"
  threshold_days="${override_days:-$ALERT_DAYS_DEFAULT}"

  if [ -z "$sni" ]; then sni="$host"; fi

  log "Checking $host:$port (SNI=$sni, threshold=${threshold_days}d)"

  pem=$(openssl_fetch_cert "$host" "$port" "$sni" || true)
  if [ -z "$pem" ]; then
    had_error=1
    msg="ERROR: $host:$port (SNI=$sni) — failed to fetch certificate"
    warn "$msg"
    alert_block="$alert_block\n$msg"
    continue
  fi

  end_epoch=$(printf "%s" "$pem" | cert_enddate_epoch || true)
  if [ -z "$end_epoch" ]; then
    had_error=1
    msg="ERROR: $host:$port — could not parse cert end date"
    warn "$msg"
    alert_block="$alert_block\n$msg"
    continue
  fi

  days_left=$(calc_days_left "$end_epoch" || echo "")
  if [ -z "$days_left" ]; then
    had_error=1
    msg="ERROR: $host:$port — failed to compute days left"
    warn "$msg"
    alert_block="$alert_block\n$msg"
    continue
  fi

  not_after_readable=$(printf "%s" "$pem" | openssl x509 -noout -enddate 2>/dev/null | sed 's/notAfter=//')
  subject=$(printf "%s" "$pem" | openssl x509 -noout -subject 2>/dev/null | sed 's/^subject=//')
  issuer=$(printf "%s" "$pem" | openssl x509 -noout -issuer 2>/dev/null | sed 's/^issuer=//')

  if [ "$days_left" -le "$threshold_days" ]; then
    had_error=1
    msg="ALERT: $host:$port — $days_left day(s) left (notAfter: $not_after_readable)\n  Subject: $subject\n  Issuer:  $issuer"
    warn "$msg"
    alert_block="$alert_block\n$msg"
  else
    log "OK: $host:$port — $days_left day(s) left (notAfter: $not_after_readable)"
  fi
done < "$TARGETS_FILE"

if [ -n "$alert_block" ]; then
  subject="$MAIL_SUBJECT_PREFIX Cert alerts on $(hostname)"
  body="Certificate status from $(hostname) at $(date -Is)\n$alert_block\n"
  # Try to send; do not fail hard if mailer missing
  send_mail "$subject" "$MAIL_TO" "$MAIL_FROM" "$body" || warn "Failed to send alert email"
fi

exit "$had_error"


