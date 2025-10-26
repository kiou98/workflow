#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import time
import hashlib
from typing import Optional, List, Dict
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from tqdm import tqdm
from supabase import create_client, Client

# =============== CONFIG ===============

START_SOURCES = [
    ("Portail Marchés Publics NC", "https://portail.marchespublics.nc/?page=Entreprise.EntrepriseAdvancedSearch&searchAnnCons"),
    ("BOAMP – 988", "https://www.boamp.fr/pages/recherche/?refine.code_departement=988"),
    ("e-MarchesPublics – NC", "https://www.e-marchespublics.com/appel-offre/outre-mer/nouvelle-caledonie/"),
    ("Province Sud – AOPs", "https://www.province-sud.nc/aops/"),
    ("Province Nord", "https://marchespublics.province-nord.nc/sallemarche.aspx"),
    ("Province des Îles", "https://www.province-iles.nc/appel-offres"),
    ("Mont-Dore", "https://www.mont-dore.nc/marches-publics"),
    ("Dumbéa", "https://www.ville-dumbea.nc/dumbea-pratique/marches-publics"),
    ("IFAP", "https://www.ifap.nc/consultations"),
    ("CCI NC", "https://www.cci.nc/la-cci-nc/appels-d-offres-et-consultations"),
    ("UNC", "https://unc.nc/utile/appel-doffres/")
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; NC-Tenders-Extractor/1.0; +https://example.com/contact)"
}

MAX_DETAIL_PER_SITE = 200
DELAY_BETWEEN_REQUESTS = 1.2

LINK_KEYWORDS = [
    "appel", "offre", "march", "consult", "avis", "soumission", "aop", "aops",
    "notice", "detail", "fiche", "consultation"
]

# =============== SUPABASE ===============

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

def get_supabase() -> Client:
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise RuntimeError("SUPABASE_URL et SUPABASE_SERVICE_KEY doivent être définis.")
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

def stable_hash(*parts: Optional[str]) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update((p or "").strip().encode("utf-8", errors="ignore"))
        h.update(b"|")
    return h.hexdigest()

def upsert_url_source(sb: Client, nom: str, url: str) -> str:
    res = sb.table("urls_a_checker").upsert(
        {"nom": nom, "url": url, "actif": True},
        on_conflict="url"
    ).select("id").execute()
    return res.data[0]["id"]

def upsert_appel(
    sb: Client,
    url_source_id: str,
    detail_url: str,
    titre: str,
    organisme: Optional[str],
    reference: Optional[str],
    date_publication: Optional[str],
    date_limite: Optional[str],
    statut: str,
    extrait: Optional[str],
) -> str:
    chash = stable_hash(titre, organisme, detail_url, date_publication, date_limite, extrait)
    rec = {
        "url_source_id": url_source_id,
        "detail_url": detail_url,
        "titre": titre or "(Sans titre)",
        "organisme": organisme,
        "reference": reference,
        "date_publication": date_publication,
        "date_limite": date_limite,
        "statut": statut,
        "extrait": extrait,
        "content_hash": chash,
    }
    res = sb.table("appels_offres").upsert(rec, on_conflict="detail_url").select("id").execute()
    return res.data[0]["id"]

# =============== HTTP helpers ===============

def safe_get(session: requests.Session, url: str, retries: int = 2, timeout: int = 25) -> Optional[requests.Response]:
    for attempt in range(retries + 1):
        try:
            r = session.get(url, headers=HEADERS, timeout=timeout)
            if r.status_code == 200:
                return r
        except Exception:
            pass
        time.sleep(1 + attempt)
    return None

def clean_text(s: Optional[str]) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip()

def likely_offer_link(href: str, text: str) -> bool:
    l = (href or "") + " " + (text or "")
    l = l.lower()
    return any(k in l for k in LINK_KEYWORDS)

def find_candidate_links(session: requests.Session, listing_url: str) -> List[str]:
    r = safe_get(session, listing_url)
    if not r:
        print(f"[WARN] échec listing: {listing_url}")
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    anchors = soup.find_all("a", href=True)
    base = listing_url
    candidates, seen = [], set()
    for a in anchors:
        href = a.get("href", "").strip()
        text = a.get_text(" ", strip=True)
        abs_url = urljoin(base, href)
        if not abs_url.startswith(("http://", "https://")):
            continue
        if abs_url.startswith(("mailto:", "tel:")):
            continue
        parsed = urlparse(abs_url)
        if likely_offer_link(abs_url, text) or any(p in parsed.path.lower() for p in ("detail", "avis", "consult", "offre", "notice", "fiche")):
            if abs_url not in seen:
                seen.add(abs_url)
                candidates.append(abs_url)
        elif abs_url.lower().endswith(".pdf") and likely_offer_link(abs_url, text):
            if abs_url not in seen:
                seen.add(abs_url)
                candidates.append(abs_url)
    return candidates[:MAX_DETAIL_PER_SITE]

def parse_dates_from_text(text: str):
    patt = re.findall(
        r"\b(?:\d{1,2}[\/\-\.\s]\d{1,2}[\/\-\.\s]\d{2,4}|\d{4}[\/\-]\d{1,2}[\/\-]\d{1,2}|\d{1,2}\s+[A-Za-zéûôàâîïùç]+\.?\s+\d{4})\b",
        text, flags=re.IGNORECASE
    )
    parsed = []
    for d in patt:
        try:
            dt = dateparser.parse(d, dayfirst=True, fuzzy=True)
            parsed.append(dt.date().isoformat())
        except Exception:
            pass
    if parsed:
        parsed.sort()
        pub = parsed[0]
        lim = parsed[-1] if len(parsed) > 1 else None
        return pub, lim
    m = re.search(
        r"(date limite|clôture|date de dépôt|délais)[^\n\r]{0,120}(\d{1,2}[\/\-\.\s]\d{1,2}[\/\-\.\s]\d{2,4}|\d{1,2}\s+[A-Za-zéûôàâîïùç]+\.?\s+\d{4}|\d{4}-\d{2}-\d{2})",
        text, flags=re.IGNORECASE
    )
    if m:
        try:
            dt = dateparser.parse(m.group(2), dayfirst=True, fuzzy=True)
            return None, dt.date().isoformat()
        except Exception:
            pass
    return None, None

def parse_detail(session: requests.Session, detail_url: str) -> Dict[str, Optional[str]]:
    r = safe_get(session, detail_url)
    result = {"detail_url": detail_url, "titre": None, "organisme": None, "reference": None, "date_publication": None, "date_limite": None, "extrait": None, "statut": "unknown"}
    if not r:
        return result

    content_type = (r.headers.get("Content-Type") or "").lower()
    if "application/pdf" in content_type or detail_url.lower().endswith(".pdf"):
        result["titre"] = "Document PDF (voir pièce)"
        result["extrait"] = "PDF détecté — lecture manuelle conseillée."
        return result

    soup = BeautifulSoup(r.text, "html.parser")
    text = soup.get_text(" ", strip=True)

    # titre
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        result["titre"] = clean_text(h1.get_text(" ", strip=True))
    else:
        meta_og = soup.find("meta", attrs={"property": "og:title"})
        if meta_og and meta_og.get("content"):
            result["titre"] = clean_text(meta_og.get("content"))
        else:
            title_tag = soup.find("title")
            if title_tag:
                result["titre"] = clean_text(title_tag.get_text())

    # organisme
    for p in [r"Organisme[:\s\-]{0,30}([A-ZÀ-Ÿ][A-Za-z0-9 \-&'’]{3,120})",
              r"Ma[iî]tre d['’ ]ouvrage[:\s\-]{0,30}([A-ZÀ-Ÿ][A-Za-z0-9 \-&'’]{3,120})",
              r"Collectivit[eé][: \-]{0,30}([A-ZÀ-Ÿ][A-Za-z0-9 \-&'’]{3,120})",
              r"Pouvoir adjudicateur[:\s\-]{0,30}([A-ZÀ-Ÿ][A-Za-z0-9 \-&'’]{3,120})"]:
        m = re.search(p, text, flags=re.IGNORECASE)
        if m:
            result["organisme"] = clean_text(m.group(1)); break

    # référence
    mref = re.search(r"(Réf(?:érence)?|Ref\.?|N°|No)\s*[:\-\s]{0,5}([A-Z0-9\-/\.]{3,60})", text, flags=re.IGNORECASE)
    if mref:
        result["reference"] = mref.group(2)

    # dates
    pub, lim = parse_dates_from_text(text)
    result["date_publication"] = pub
    result["date_limite"] = lim

    # extrait
    for p in soup.find_all(["p", "div"]):
        t = clean_text(p.get_text(" ", strip=True))
        if t and len(t) > 60:
            result["extrait"] = t[:600]
            break

    # statut basique
    try:
        if result["date_limite"]:
            from datetime import date
            result["statut"] = "open" if date.fromisoformat(result["date_limite"]) >= date.today() else "closed"
        else:
            result["statut"] = "published"
    except Exception:
        pass

    return result

# =============== MAIN ===============

def main():
    sb = get_supabase()
    session = requests.Session()

    for nom, listing_url in START_SOURCES:
        source_id = upsert_url_source(sb, nom=nom, url=listing_url)
        print(f"[INFO] Source: {nom} | {listing_url} | id={source_id}")
        cand = find_candidate_links(session, listing_url)
        print(f"[INFO] {len(cand)} liens candidats détectés")

        for u in tqdm(cand, desc=f"{nom}", unit="fiche"):
            try:
                detail = parse_detail(session, u)
                appel_id = upsert_appel(
                    sb=sb,
                    url_source_id=source_id,
                    detail_url=u,
                    titre=detail.get("titre") or "(Sans titre)",
                    organisme=detail.get("organisme"),
                    reference=detail.get("reference"),
                    date_publication=detail.get("date_publication"),
                    date_limite=detail.get("date_limite"),
                    statut=detail.get("statut") or "unknown",
                    extrait=detail.get("extrait"),
                )
            except Exception as e:
                print(f"[ERROR] {u} -> {e}")
            time.sleep(DELAY_BETWEEN_REQUESTS)

    print("[DONE] Scrape + upsert terminés.")

if __name__ == "__main__":
    main()
