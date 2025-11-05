### TLS certificate checker (on-host)

Runs directly on a server, checks TLS certificates for given `host:port` pairs, and emails alerts when expiration is within threshold.

#### Requirements
- `openssl`, `awk`, `sed`
- `date` from GNU coreutils (recommended). macOS/BSD compatible formatting attempted as fallback.
- Email sender: one of `mailx`, `mail`, or `/usr/sbin/sendmail`

#### Files
- `check_certs.sh`: main script
- `targets.txt`: your endpoints (create from example)
- `targets.example.txt`: sample targets (copy to `targets.txt`)
- `systemd/check-certs.service` and `.timer`: optional units to run periodically

#### Targets format
- One per line; comments start with `#`; blank lines ignored
- `host[:port][,sni=example.com][,days=NN]`

Examples:
```
example.com
example.com:443
10.0.0.5:8443,sni=service.example.com
api.example.com:443,days=15
```

To start, copy the example:
```
cp cert_check/targets.example.txt cert_check/targets.txt
```

#### Environment variables
- `TARGETS_FILE`: path to targets file (default: `./targets.txt`)
- `ALERT_DAYS_DEFAULT`: default threshold in days (default: `30`)
- `MAIL_TO`: recipient (default: current system user name, local mailbox)
- `MAIL_FROM`: sender (default: `cert-checker@$(hostname -f)`) 
- `MAIL_SUBJECT_PREFIX`: subject prefix (default: `[TLS-CERT]`)
- `MAIL_LOCAL_ONLY`: `true|false` force local mailbox via sendmail (default: `true`)

#### Usage
```
./check_certs.sh -f ./targets.txt -d 30
```

- Exit code `0`: all ok (no alerts/errors)
- Exit code `1`: at least one alert or error occurred

#### systemd setup (optional)
Place units and enable a timer (local mailbox by default):
```
sudo cp cert_check/systemd/check-certs.service /etc/systemd/system/
sudo cp cert_check/systemd/check-certs.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now check-certs.timer
```

Manual run:
```
sudo systemctl start check-certs.service
sudo journalctl -u check-certs.service -n 100 -f
```

Edit paths inside the unit to point to the repo location.

#### Local mailbox (mailx)
By default, messages are delivered to the local system mailbox for the user and can be read with `mailx`:
```
mailx
```
For `root` mailbox:
```
sudo su -
mailx
```
To force local delivery, keep `MAIL_LOCAL_ONLY=true` and set `MAIL_TO` to a local username (no domain), e.g. `root`.

#### Push to GitHub (repo is empty)
Initialize repo locally and push main branch to `one6way/certmonk`:
```
cd /path/to/your/checkout
git init -b main
git add cert_check
git commit -m "certmonk: on-host TLS cert monitor (script + systemd)"
git remote add origin https://github.com/one6way/certmonk.git
git push -u origin main
```

If you prefer SSH remote:
```
git remote remove origin 2>/dev/null || true
git remote add origin git@github.com:one6way/certmonk.git
git push -u origin main
```


