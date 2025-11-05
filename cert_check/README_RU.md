### Проверка TLS-сертификатов на сервере

Скрипт запускается прямо на сервере, проверяет TLS-сертификаты у заданных `host:port` и отправляет уведомления, если срок действия подходит к порогу.

#### Требования
- `openssl`, `awk`, `sed`
- `date` из GNU coreutils (желательно). Для BSD/macOS предусмотрен фолбэк парсинга дат.
- Почтовая утилита: `sendmail` (локальная доставка), либо `mailx`/`mail`

#### Состав
- `check_certs.sh`: основной скрипт
- `targets.txt`: ваши цели (создайте из примера)
- `targets.example.txt`: примеры целей (скопируйте в `targets.txt`)
- `systemd/check-certs.service` и `.timer`: опциональные юниты для периодического запуска

#### Формат целей
- По одной записи на строку; строки с `#` — комментарии, пустые строки игнорируются
- `host[:port][,sni=example.com][,days=NN]`

Примеры:
```
example.com
example.com:443
10.0.0.5:8443,sni=service.example.com
api.example.com:443,days=15
```

Старт с примера:
```
cp cert_check/targets.example.txt cert_check/targets.txt
```

#### Переменные окружения
- `TARGETS_FILE`: путь к файлу целей (по умолчанию: `./targets.txt`)
- `ALERT_DAYS_DEFAULT`: порог в днях по умолчанию (по умолчанию: `30`)
- `MAIL_TO`: получатель (по умолчанию: локальный пользователь, его почтовый ящик на сервере)
- `MAIL_FROM`: отправитель (по умолчанию: `cert-checker@$(hostname -f)`) 
- `MAIL_SUBJECT_PREFIX`: префикс темы (по умолчанию: `[TLS-CERT]`)
- `MAIL_LOCAL_ONLY`: `true|false` — принудительная локальная доставка через `sendmail` (по умолчанию: `true`)

#### Использование
```
./check_certs.sh -f ./targets.txt -d 30
```

- Код выхода `0`: всё ок (нет алертов/ошибок)
- Код выхода `1`: есть алерты или ошибки

#### Установка через systemd (опционально)
Разместите юниты и включите таймер (по умолчанию — локальная почта):
```
sudo cp cert_check/systemd/check-certs.service /etc/systemd/system/
sudo cp cert_check/systemd/check-certs.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now check-certs.timer
```

Ручной запуск:
```
sudo systemctl start check-certs.service
sudo journalctl -u check-certs.service -n 100 -f
```

Отредактируйте пути внутри юнитов под ваше расположение.

#### Локальная почта (mailx)
По умолчанию письма доставляются в локальный почтовый ящик пользователя и читаются через `mailx`:
```
mailx
```
Для ящика `root`:
```
sudo su -
mailx
```
Чтобы принудить локальную доставку, оставьте `MAIL_LOCAL_ONLY=true` и укажите `MAIL_TO` как имя локального пользователя (без домена), например `root`.


