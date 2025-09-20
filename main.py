"""
CEIDG -> PostgreSQL -> wzbogacenie danymi z Aleo (MVP)
Autor: (Twoje imię)
Wymagania: Python 3.10+, requests, psycopg2-binary
Konfiguracja przez zmienne środowiskowe:
  - DATABASE_URL=postgresql://user:pass@host:port/dbname
  - CEIDG_BASE_URL=https://api.ceidg.gov.pl/  (przykład; wstaw właściwy)
  - CEIDG_API_KEY=... (jeśli wymagany)
  - ALEO_API_URL=https://twoj-serwer-aleo.local/lookup?nip={nip} (Twój wewnętrzny serwis)

Logika:
  1) Pobierz stronę "firmy" (25 na stronę), iteruj po rekordach, zapisuj podstawy.
  2) Dla każdego rekordu pobierz szczegóły z "firma" przez pełny link i zapisz pola szczegółowe.
  3) Wzbogacenie: wywołaj API scraper Aleo po NIP i zapisz JSON w kolumnie aleo.
  4) Kontynuuj aż do dodania 20 NOWYCH rekordów (ON CONFLICT DO NOTHING liczymy tylko nowe).
  5) Resp. limit: CEIDG 50 zapytań / 180 s => ~1/3.6 s. Używamy stałej przerwy 3.7 s + prosty backoff.
"""
from __future__ import annotations
import os
import pprint
import time
import json
import logging
from typing import Any, Dict, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras as pgx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

RATE_SLEEP_SECONDS = 3.7  # zachowaj margines bezpieczeństwa dla CEIDG
NEW_RECORDS_TARGET = 20
PAGE_SIZE = 25


def get_env(name: str, default: Optional[str] = None) -> str:
    from dotenv import load_dotenv
    load_dotenv(override=True)

    v = os.getenv(name, default)
    if v is None:
        raise RuntimeError(f"Brak wymaganej zmiennej środowiskowej: {name}")
    return v


def connect_db():
    dsn = get_env("DATABASE_URL")
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    return conn


DDL = r"""
      CREATE TABLE IF NOT EXISTS ceidg_companies (
                                               ceidg_id               text PRIMARY KEY,
                                               name                   text,
                                               date_start             date,
                                               status                 text,
                                               nip                    text UNIQUE,
                                               link                   text,

                                               owner                  jsonb,
                                               address_business       jsonb,
                                               address_correspondence jsonb,
                                               citizenships           jsonb,
                                               pkd_year               integer,
                                               pkd                    jsonb,
                                               phone                  text,
                                               email                  text,
                                               www                    text,
                                               edoreczenia            text,
                                               aleo                   jsonb,

                                               created_at             timestamptz NOT NULL DEFAULT now(),
          updated_at             timestamptz NOT NULL DEFAULT now()
          );

      CREATE INDEX IF NOT EXISTS idx_companies_updated_at ON ceidg_companies(updated_at);
      CREATE INDEX IF NOT EXISTS idx_companies_email ON ceidg_companies(email);
      CREATE INDEX IF NOT EXISTS idx_companies_phone ON ceidg_companies(phone);
      CREATE INDEX IF NOT EXISTS idx_companies_pkd_year ON ceidg_companies(pkd_year);

-- Prosty stan crawl'a (opcjonalnie)
      CREATE TABLE IF NOT EXISTS ceidg_crawl_state (
                                                 id          smallint PRIMARY KEY DEFAULT 1,
                                                 last_page   integer NOT NULL DEFAULT 0,
                                                 last_run_at timestamptz NOT NULL DEFAULT now()
          );
      INSERT INTO ceidg_crawl_state(id) VALUES (1) ON CONFLICT DO NOTHING;
      """


UPSERT_BASE = (
    "INSERT INTO ceidg_companies (ceidg_id, name, date_start, status, nip, link)\n"
    "VALUES (%(ceidg_id)s, %(name)s, %(date_start)s, %(status)s, %(nip)s, %(link)s)\n"
    "ON CONFLICT (ceidg_id) DO NOTHING"
)

UPDATE_DETAILS = (
    "UPDATE ceidg_companies SET\n"
    "  owner=%(owner)s,\n"
    "  address_business=%(address_business)s,\n"
    "  address_correspondence=%(address_correspondence)s,\n"
    "  citizenships=%(citizenships)s,\n"
    "  pkd_year=%(pkd_year)s,\n"
    "  pkd=%(pkd)s,\n"
    "  phone=%(phone)s,\n"
    "  email=%(email)s,\n"
    "  www=%(www)s,\n"
    "  edoreczenia=%(edoreczenia)s,\n"
    "  updated_at=now()\n"
    "WHERE ceidg_id=%(ceidg_id)s"
)

UPDATE_ALEO = (
    "UPDATE ceidg_companies SET aleo=%(aleo)s, updated_at=now() WHERE ceidg_id=%(ceidg_id)s"
)


class CEIDGClient:
    def __init__(self, base_url: str, api_key: Optional[str] = None):
        self.base = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        if api_key:
            self.session.headers.update({"Authorization": f"Bearer {api_key}"})

    def _sleep(self):
        time.sleep(RATE_SLEEP_SECONDS)

    def companies(self, page: int) -> Dict[str, Any]:
        url = f"{self.base}/firmy?page={page}&miasto=Wrocław"
        for attempt in range(3):
            r = self.session.get(url, timeout=30)
            if r.status_code == 429:
                logging.warning("429 Too Many Requests – czekam i ponawiam")
                time.sleep(RATE_SLEEP_SECONDS * (attempt + 2))
                continue
            r.raise_for_status()

            self._sleep()
            return r.json()
        raise RuntimeError("CEIDG /firmy: zbyt wiele nieudanych prób")

    def company_detail_by_link(self, link: str) -> Dict[str, Any]:
        for attempt in range(3):
            r = self.session.get(link, timeout=30)
            if r.status_code == 429:
                logging.warning("429 Too Many Requests – czekam i ponawiam")
                time.sleep(RATE_SLEEP_SECONDS * (attempt + 2))
                continue
            r.raise_for_status()
            self._sleep()
            return r.json()
        raise RuntimeError("CEIDG /firma: zbyt wiele nieudanych prób")


def fetch_aleo_json(nip: str) -> Optional[dict]:
    # url_tpl = get_env("ALEO_API_URL")
    # url = url_tpl.format(nip=nip)
    # for attempt in range(2):
    #     try:
    #         r = requests.get(url, timeout=30)
    #         if r.status_code == 404:
    #             return None
    #         r.raise_for_status()
    #         return r.json()
    #     except Exception as e:
    #         logging.warning("Aleo lookup błąd (%s), próba %d", e, attempt + 1)
    #         time.sleep(1.0 * (attempt + 1))
    return None


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()


def insert_base(conn, row: Dict[str, Any]) -> bool:
    """Zwraca True, jeśli wstawiono NOWY rekord."""
    with conn.cursor() as cur:
        cur.execute(UPSERT_BASE, row)
        inserted = cur.rowcount  # 1 jeśli insert, 0 jeśli konflikt
    return inserted == 1


def update_details(conn, details: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute(UPDATE_DETAILS, details)


def update_aleo(conn, ceidg_id: str, aleo_json: Optional[dict]):
    with conn.cursor() as cur:
        cur.execute(UPDATE_ALEO, {"ceidg_id": ceidg_id, "aleo": pgx.Json(aleo_json)})


def map_base_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "ceidg_id": rec.get("id"),
        "name": rec.get("nazwa"),
        "date_start": rec.get("dataRozpoczecia"),
        "status": rec.get("status"),
        "nip": rec.get("wlasciciel").get("nip"),
        "link": rec.get("link"),  # pełny URL do szczegółów
    }


def map_detail_record(detail: Dict[str, Any]) -> Dict[str, Any]:
    # Zakładamy struktury wg opisu użytkownika. Dopasuj klucze do realnego API CEIDG.
    def pick_addr(addr: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not addr:
            return None
        keys = ["kraj", "kod", "miasto", "ulica", "budynek", "lokal"]
        return {k: addr.get(k) for k in keys}

    return {
        "owner": pgx.Json(detail.get("wlasciciel")),
        "address_business": pgx.Json(pick_addr(detail.get("adresDzialalnosci"))),
        "address_correspondence": pgx.Json(pick_addr(detail.get("adresKorespondencyjny"))),
        "citizenships": pgx.Json(detail.get("obywatelstwa")),
        "pkd_year": detail.get("rokPkd"),
        "pkd": pgx.Json(detail.get("pkd")),
        "phone": detail.get("telefon"),
        "email": detail.get("email"),
        "www": detail.get("www"),
        "edoreczenia": detail.get("adresDoreczenElektronicznych"),
    }


def run_etl():
    conn = connect_db()
    try:
        ensure_schema(conn)

        base_url = get_env("CEIDG_BASE_URL")
        api_key = os.getenv("CEIDG_API_KEY")
        ceidg = CEIDGClient(base_url, api_key)

        # Opcjonalnie odczytaj ostatnią stronę
        with conn.cursor(cursor_factory=pgx.DictCursor) as cur:
            cur.execute("SELECT last_page FROM ceidg_crawl_state WHERE id=1")
            row = cur.fetchone()
            current_page = int(row[0]) if row else 0

        total_inserted = 0
        while total_inserted < NEW_RECORDS_TARGET:
            logging.info("Pobieram /firmy, strona=%s", current_page)
            data = ceidg.companies(page=current_page)
            count_total = data.get("count")
            items = data.get("firmy") or data.get("items") or data.get("data") or []  # dopasuj do faktycznego pola listy
            if not items:
                logging.info("Brak elementów na stronie %s – przerywam", current_page)
                break

            for rec in items:
                base = map_base_record(rec)
                try:
                    inserted = insert_base(conn, base)
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    logging.exception("Błąd insertu podstawy dla %s: %s", base.get("ceidg_id"), e)
                    continue

                # Szczegóły
                try:
                    detail_json = ceidg.company_detail_by_link(base["link"]) if base.get("link") else {}
                    details = map_detail_record(detail_json.get("firma")[0])
                    details["ceidg_id"] = base["ceidg_id"]
                    update_details(conn, details)
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    logging.exception("Błąd aktualizacji szczegółów %s: %s", base.get("ceidg_id"), e)

                # Wzbogacenie ALEO po NIP
                try:
                    nip = base.get("nip")
                    if nip:
                        aleo_json = fetch_aleo_json(nip)
                    else:
                        aleo_json = None
                    update_aleo(conn, base["ceidg_id"], aleo_json)
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    logging.exception("Błąd aktualizacji ALEO %s: %s", base.get("ceidg_id"), e)

                if inserted:
                    total_inserted += 1
                    logging.info("Dodano nowy rekord (%d/%d): %s", total_inserted, NEW_RECORDS_TARGET, base.get("name"))
                    if total_inserted >= NEW_RECORDS_TARGET:
                        break

            # Zapisz postęp strony
            with conn.cursor() as cur:
                cur.execute("UPDATE ceidg_crawl_state SET last_page=%s, last_run_at=now() WHERE id=1", (current_page,))
            conn.commit()

            current_page += 1
            if count_total is not None and current_page * PAGE_SIZE >= int(count_total):
                logging.info("Osiągnięto koniec wyników (%s)", count_total)
                break

        logging.info("Zakończono. Nowe rekordy dodane: %d", total_inserted)

    finally:
        conn.close()


if __name__ == "__main__":
    run_etl()
