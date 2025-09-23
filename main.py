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

import math
import os
import time
import logging
from typing import Any, Dict, Optional

import requests
import psycopg2
import psycopg2.extras as pgx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

RATE_SLEEP_SECONDS = 3.7  # zachowaj margines bezpieczeństwa dla CEIDG
CONTACTABLE_TARGET = 18
PAGE_SIZE = 25

CITY_FILTER = "Wrocław"  # np. "Wrocław"


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
      CREATE INDEX IF NOT EXISTS idx_contactable
          ON ceidg_companies(ceidg_id)
          WHERE email IS NOT NULL OR phone IS NOT NULL;

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
        url = f"{self.base}/firmy?page={page}" + (f"&miasto={CITY_FILTER}" if CITY_FILTER else "")
        for attempt in range(3):
            try:
                r = self.session.get(url, timeout=30)
                if r.status_code == 429:
                    logging.warning("429 Too Many Requests – czekam i ponawiam")
                    time.sleep(RATE_SLEEP_SECONDS * (attempt + 2))
                    continue
                r.raise_for_status()

                self._sleep()

                return r.json()
            except Exception:
                logging.exception("Błąd przy tworzeniu sesji CEIDG API")

        raise RuntimeError("CEIDG /firmy: zbyt wiele nieudanych prób albo timeout")

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


def map_base_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    owner = rec.get("wlasciciel") or {}

    return {
        "ceidg_id": rec.get("id"),
        "name": rec.get("nazwa"),
        "date_start": rec.get("dataRozpoczecia"),
        "status": rec.get("status"),
        "nip": owner.get("nip"),
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


def existing_ids(conn, ids: list[str]) -> set[str]:
    if not ids:
        return set()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT ceidg_id FROM ceidg_companies WHERE ceidg_id = ANY(%s)",
            (ids,)
        )

        return {row[0] for row in cur.fetchall()}


def is_contactable_mapped(mapped: dict) -> bool:
    return bool(mapped.get("phone")) or bool(mapped.get("email"))


def run_etl():
    conn = connect_db()
    try:
        ensure_schema(conn)

        base_url = get_env("CEIDG_BASE_URL")
        api_key = os.getenv("CEIDG_API_KEY")
        ceidg = CEIDGClient(base_url, api_key)

        contactable_added = 0
        current_page = 0

        # Opcjonalne: jeśli przez X kolejnych stron wszystkie 25 firm już jest w bazie,
        # to przerywamy, bo najpewniej nic nowego nie ma
        all_known_pages_limit = 300
        all_known_streak = 0

        while contactable_added < CONTACTABLE_TARGET:
            data = ceidg.companies(page=current_page)
            count_total = data.get("count")
            if current_page == 0:
                logging.info("Pobieram /firmy, firm ogółem=%s, strony=0-%s", count_total, math.floor(count_total/PAGE_SIZE))

            logging.info("Pobieram /firmy, strona=%s", current_page)
            items = data.get("firmy") or data.get("items") or data.get("data") or []
            if not items:
                logging.info("Brak elementów na stronie %s – przerywam.", current_page)
                break

            page_known_only = True  # założenie: wszystkie znane, obalimy gdy znajdziemy nową

            ids = [r.get("id") for r in items if r.get("id")]
            known = existing_ids(conn, ids)
            for rec in items:
                ceidg_id = rec.get("id")
                if not ceidg_id or ceidg_id in known:
                    continue

                page_known_only = False  # jednak trafiliśmy nową

                # 1) Insert podstawy
                base = map_base_record(rec)
                try:
                    inserted = insert_base(conn, base)
                    conn.commit()
                except Exception:
                    conn.rollback()
                    logging.exception("Błąd insertu podstawy dla %s", ceidg_id)
                    continue

                if not inserted:
                    # Rzadki przypadek wyścigu – ktoś wstawił równolegle
                    continue

                # 2) Pobierz szczegóły i zaktualizuj
                try:
                    detail_json = ceidg.company_detail_by_link(base["link"]) if base.get("link") else {}

                    firmy_list = detail_json.get("firma") or []   # może być None, więc zamieniamy na []
                    if not firmy_list:
                        logging.warning("Brak szczegółów firmy w API dla ceidg_id=%s", ceidg_id)
                        continue   # pomijamy ten rekord bez aktualizacji

                    details = map_detail_record(firmy_list[0])
                    details["ceidg_id"] = ceidg_id
                    update_details(conn, details)
                    conn.commit()
                except Exception:
                    conn.rollback()
                    logging.exception("Błąd aktualizacji szczegółów %s", ceidg_id)
                    continue

                # 3) Licznik tylko dla rekordów z telefonem lub emailem
                try:
                    if is_contactable_mapped(details):
                        contactable_added += 1
                        logging.info(
                            "Nowy kontaktowalny (%d/%d): %s",
                            contactable_added, CONTACTABLE_TARGET, base.get("name")
                        )
                        if contactable_added >= CONTACTABLE_TARGET:
                            break
                except Exception:
                    logging.exception("Błąd przy sprawdzaniu kontaktowalności %s", ceidg_id)

            # zarządzanie końcem / przejściem do kolejnej strony
            if page_known_only:
                all_known_streak += 1
            else:
                all_known_streak = 0

            if all_known_streak >= all_known_pages_limit:
                logging.info(
                    "Ostatnie %d stron zawierało wyłącznie znane rekordy – kończę.",
                    all_known_pages_limit
                )
                break

            current_page += 1
            if count_total is not None and current_page * PAGE_SIZE >= int(count_total):
                logging.info("Osiągnięto koniec wyników (%s)", count_total)
                break

        logging.info("Zakończono. Dodano nowych kontaktowalnych: %d", contactable_added)

    finally:
        conn.close()


if __name__ == "__main__":
    run_etl()
