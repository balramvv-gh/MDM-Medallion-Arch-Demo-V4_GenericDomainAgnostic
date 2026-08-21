"""
Generates synthetic source system extracts for the MDM demo:
  - CRM system export (data/sources/crm_customers.csv)  -- ~100 rows
  - ERP system export (data/sources/erp_customers.csv)  -- ~100 rows

Design intent (deterministic: random.seed + Faker.seed are both fixed, so this
script produces byte-identical output every run):
  - CURATED_PEOPLE (18 hand-picked, real-city people) are kept as-is for
    continuity with the original demo -- they exercise the original set of
    validation/exception cases by hand-picked index.
  - GENERATED_PEOPLE tops the population up to a scale where each source system
    lands at ~100 rows, split across:
      * BOTH, exact-match pairs   -- same person in CRM+ERP, email/phone are the
        same underneath reformatting (caught by the deterministic match tier)
      * BOTH, fuzzy-match pairs   -- same person in CRM+ERP, but email AND phone
        both genuinely differ between systems while name/address stay similar
        (only findable by the embedding-similarity tier in
        scripts/generate_matches.py -- exact tier will legitimately miss these)
      * CRM-only / ERP-only       -- no counterpart in the other system at all
  - A subset of rows get deliberately dirty data (bad email, missing required
    field, invalid state/country code, missing phone) so the cleansing/
    validation layer and the stewardship exception queue have real work to do.

Two modes, controlled by CLI flag (default is --full):

  --full (default)  Writes a full extract to data/sources/{crm,erp}_customers.csv.
                     First-ever run: this is the deterministic baseline above,
                     persisted as generator state (data/sources/_state/generator_state.json).
                     Any later run: dumps the CURRENT persisted state in full
                     (baseline + every incremental change since), NOT a fresh
                     re-roll of the baseline -- this is what a real source
                     system re-exporting "everything as of now" would send,
                     and what scripts/load_bronze.py --force-full expects to
                     read. To truly reset back to the pure seed=42 baseline,
                     delete data/sources/_state/generator_state.json first.

  --incremental      Advances the persisted state and writes ONLY the changed
                     rows (a handful of updates, one or two logical deletes
                     via is_deleted=true, one or two brand-new customers) to
                     the same CSV paths, each stamped with today's date as
                     modified_date/last_updated so scripts/load_bronze.py's
                     watermark filter picks them up. Only mutates records from
                     the plain "both sources, exact match, clean data" pool
                     (see ELIGIBLE_FOR_INCREMENTAL below) so it never disturbs
                     the deliberately calibrated curated/fuzzy/dirty test
                     cases. Safe to run repeatedly -- each call is a new batch
                     on top of the last. If no state exists yet, bootstraps
                     the baseline first (bootstrapping does NOT write baseline
                     rows to the CSV -- only the incremental batch on top of it
                     does; run --full first if you actually want the pipeline
                     to see the baseline as a full load).

is_deleted: every row (baseline and incremental) now carries this column,
defaulting to false. Only --incremental can flip it to true. bronze/silver
propagate it; see scripts/load_bronze.py and dbt_project/models/silver/.
"""
import argparse
import csv
import json
import os
import random
import re
from datetime import datetime, timedelta

from faker import Faker

random.seed(42)
fake = Faker("en_US")
Faker.seed(42)

BASE_DIR = os.path.dirname(__file__)
OUT_DIR = os.path.join(BASE_DIR, "sources")

# ---------------------------------------------------------------------------
# Curated people (original 18) -- kept verbatim for continuity. Indices 1-18.
# ---------------------------------------------------------------------------
CURATED_PEOPLE = [
    ("Maria", "Gonzalez", "maria.gonzalez@example.com", "410-555-0142", "142 Oak St", "Bel Air", "MD", "21014", "US"),
    ("James", "Whitfield", "james.whitfield@example.com", "443-555-0199", "88 Birchwood Ave", "Baltimore", "MD", "21201", "US"),
    ("Ling", "Zhao", "ling.zhao@example.com", "202-555-0117", "500 Constitution Ave", "Washington", "DC", "20001", "US"),
    ("Aditya", "Rao", "aditya.rao@example.com", "212-555-0163", "9 Wall St", "New York", "NY", "10005", "US"),
    ("Sofia", "Moretti", "sofia.moretti@example.com", "617-555-0180", "22 Beacon St", "Boston", "MA", "02108", "US"),
    ("David", "Okafor", "david.okafor@example.com", "312-555-0111", "1 State St", "Chicago", "IL", "60602", "US"),
    ("Emily", "Carter", "emily.carter@example.com", "215-555-0155", "300 Market St", "Philadelphia", "PA", "19106", "US"),
    ("Noah", "Kim", "noah.kim@example.com", "512-555-0177", "77 Congress Ave", "Austin", "TX", "78701", "US"),
    ("Fatima", "Haidari", "fatima.haidari@example.com", "703-555-0122", "14 Duke St", "Alexandria", "VA", "22314", "US"),
    ("Liam", "OBrien", "liam.obrien@example.com", "404-555-0133", "10 Peachtree St", "Atlanta", "GA", "30303", "US"),
    ("Ava", "Thompson", "ava.thompson@example.com", "303-555-0144", "1 Civic Center Dr", "Denver", "CO", "80202", "US"),
    ("Mohammed", "Al-Sayed", "mohammed.alsayed@example.com", "602-555-0166", "200 W Jefferson St", "Phoenix", "AZ", "85003", "US"),
    ("Grace", "Nguyen", "grace.nguyen@example.com", "206-555-0188", "600 4th Ave", "Seattle", "WA", "98104", "US"),
    ("Ethan", "Brooks", "ethan.brooks@example.com", "614-555-0121", "90 W Broad St", "Columbus", "OH", "43215", "US"),
    ("Olivia", "Schmidt", "olivia.schmidt@example.com", "651-555-0139", "15 Kellogg Blvd", "St Paul", "MN", "55102", "US"),
    ("Lucas", "Ferreira", "lucas.ferreira@example.com", "305-555-0114", "111 SW 1st St", "Miami", "FL", "33130", "US"),
    ("Chloe", "Bennett", "chloe.bennett@example.com", "702-555-0128", "500 S Grand Central Pkwy", "Las Vegas", "NV", "89106", "US"),
    ("Ryan", "Sullivan", "ryan.sullivan@example.com", "801-555-0119", "451 S State St", "Salt Lake City", "UT", "84111", "US"),
]
# curated single-source cases (unchanged from the original design)
CURATED_CRM_ONLY = {13, 14}     # missing from ERP
CURATED_ERP_ONLY = {16, 17, 18}  # missing from CRM
# curated fuzzy-only pair, folded into CURATED_PEOPLE as indices 19-20 below
CURATED_PEOPLE.append(("Benjamin", "Carter", "benjamin.carter@example.com", "617-555-0192", "5 Newbury St", "Boston", "MA", "02116", "US"))
CURATED_PEOPLE.append(("Isabella", "Martinez", "isabella.martinez@example.com", "480-555-0145", "220 Cactus Rd", "Tempe", "AZ", "85281", "US"))
CURATED_FUZZY_OVERRIDES = {
    19: {"full_name": "Carter, Ben", "email_addr": "bcarter@example-corp.com",
         "contact_phone": "6175559192", "addr1": "5 Newbury Street"},
    20: {"full_name": "Martinez, Izzy", "email_addr": "izzy.m@example-corp.com",
         "contact_phone": "4805551145", "addr1": "220 Cactus Road"},
}

# ---------------------------------------------------------------------------
# Scale knobs: each source system lands at BOTH + <source>_ONLY rows.
# ---------------------------------------------------------------------------
CRM_ONLY_TOTAL = 12
ERP_ONLY_TOTAL = 12
FUZZY_PAIR_TOTAL = 12     # includes the 2 curated ones above
TARGET_ROWS_PER_SOURCE = 100
BOTH_TOTAL = TARGET_ROWS_PER_SOURCE - CRM_ONLY_TOTAL   # = TARGET - ERP_ONLY_TOTAL too (symmetric)
assert TARGET_ROWS_PER_SOURCE - ERP_ONLY_TOTAL == BOTH_TOTAL

STREET_ABBREV = [("Street", "St"), ("Avenue", "Ave"), ("Road", "Rd"), ("Boulevard", "Blvd"), ("Drive", "Dr")]

# Must match dbt_project/seeds/ref_state_codes.csv exactly (50 states + DC) -- Faker's
# state_abbr() also returns US-affiliated jurisdictions (MH, FM, PW, ...) that aren't in
# that reference table, which would otherwise send an otherwise-clean generated person to
# the exception queue for an unrelated reason ("invalid state code") and make match-tier
# test cases unreliable.
VALID_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN",
    "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV",
    "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN",
    "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC",
}


def _valid_state():
    while True:
        s = fake.state_abbr(include_territories=False)
        if s in VALID_STATES:
            return s

_used_emails = set()
_used_names = set()


def _unique_person():
    """Draws a Faker person not already used (by name or derived email)."""
    while True:
        first, last = fake.first_name(), fake.last_name()
        key = (first.lower(), last.lower())
        if key in _used_names:
            continue
        email = f"{first.lower()}.{last.lower()}@example.com"
        if email in _used_emails:
            continue
        _used_names.add(key)
        _used_emails.add(email)
        state = _valid_state()
        area_code = random.randint(201, 989)
        return (
            first, last, email, f"{area_code}-555-{random.randint(0, 9999):04d}",
            fake.street_address(), fake.city(), state, fake.zipcode_in_state(state), "US",
        )


def _nickname_variant(first_name):
    """Cheap, generic 'looks like a nickname or typo' transform -- no real
    nickname dictionary needed, just enough drift that exact string/email/phone
    match won't catch it but the name is still recognizably close."""
    style = random.choice(["prefix", "swap", "drop"])
    if style == "prefix" and len(first_name) > 3:
        return first_name[: random.randint(3, 4)]
    if style == "swap" and len(first_name) > 3:
        i = random.randint(1, len(first_name) - 2)
        chars = list(first_name)
        chars[i], chars[i + 1] = chars[i + 1], chars[i]
        return "".join(chars)
    if style == "drop" and len(first_name) > 4:
        i = random.randint(1, len(first_name) - 2)
        return first_name[:i] + first_name[i + 1 :]
    return first_name


def _fuzzy_address_variant(addr):
    for full, abbr in STREET_ABBREV:
        if addr.endswith(" " + abbr):
            return addr[: -len(abbr)] + full
        if addr.endswith(" " + full):
            return addr[: -len(full)] + abbr
    return addr


GENERATED_PEOPLE = []
GENERATED_CRM_ONLY = set()
GENERATED_ERP_ONLY = set()
GENERATED_FUZZY = {}  # index -> ERP dirty overrides, same shape as CURATED_FUZZY_OVERRIDES

_next_idx = len(CURATED_PEOPLE) + 1  # continue numbering after curated people

n_generated_both_exact = BOTH_TOTAL - len(CURATED_PEOPLE) + len(CURATED_CRM_ONLY) + len(CURATED_ERP_ONLY) - (FUZZY_PAIR_TOTAL - len(CURATED_FUZZY_OVERRIDES))
n_generated_fuzzy = FUZZY_PAIR_TOTAL - len(CURATED_FUZZY_OVERRIDES)
n_generated_crm_only = CRM_ONLY_TOTAL - len(CURATED_CRM_ONLY)
n_generated_erp_only = ERP_ONLY_TOTAL - len(CURATED_ERP_ONLY)

for _ in range(n_generated_both_exact):
    GENERATED_PEOPLE.append(_unique_person())
    _next_idx += 1

for _ in range(n_generated_fuzzy):
    p = _unique_person()
    idx = len(CURATED_PEOPLE) + len(GENERATED_PEOPLE) + 1
    GENERATED_PEOPLE.append(p)
    first, last, email, phone, addr, city, state, zipc, country = p
    GENERATED_FUZZY[idx] = {
        "full_name": f"{last}, {_nickname_variant(first)}",
        "email_addr": f"{_nickname_variant(first).lower()}.{last.lower()}{random.randint(1,99)}@example-corp.com",
        "contact_phone": f"{random.randint(201,989)}555{random.randint(0,9999):04d}",
        "addr1": _fuzzy_address_variant(addr),
    }

for _ in range(n_generated_crm_only):
    GENERATED_PEOPLE.append(_unique_person())
    idx = len(CURATED_PEOPLE) + len(GENERATED_PEOPLE)
    GENERATED_CRM_ONLY.add(idx)

for _ in range(n_generated_erp_only):
    GENERATED_PEOPLE.append(_unique_person())
    idx = len(CURATED_PEOPLE) + len(GENERATED_PEOPLE)
    GENERATED_ERP_ONLY.add(idx)

PEOPLE = CURATED_PEOPLE + GENERATED_PEOPLE
CRM_ONLY = CURATED_CRM_ONLY | GENERATED_CRM_ONLY
ERP_ONLY = CURATED_ERP_ONLY | GENERATED_ERP_ONLY
FUZZY_OVERRIDES = {**CURATED_FUZZY_OVERRIDES, **GENERATED_FUZZY}

# ---------------------------------------------------------------------------
# Dirty (reject-severity validation failure) injection -- scaled up from the
# original 6 hand-picked cases to a proportional ~16 across the larger set.
# Picked deterministically (seeded random.sample) from indices that are NOT
# already fuzzy-override or single-source-only rows, so each violation type
# is easy to reason about in isolation.
# ---------------------------------------------------------------------------
eligible = [i for i in range(1, len(PEOPLE) + 1) if i not in FUZZY_OVERRIDES]
random.shuffle(eligible)
CRM_DIRTY_MISSING_LASTNAME = set(eligible[0:2])
CRM_DIRTY_BAD_EMAIL = set(eligible[2:4])
CRM_DIRTY_INVALID_STATE = set(eligible[4:6])
CRM_DIRTY_MESSY_CASE = set(eligible[6:8])
ERP_DIRTY_MISSING_PHONE = set(eligible[8:10])
ERP_DIRTY_BAD_EMAIL = set(eligible[10:12])
ERP_DIRTY_INVALID_COUNTRY = set(eligible[12:14])
# keep the two original hand-picked dirty cases too (indices 2,5,9,11 CRM / 3,6,12 ERP)
CRM_DIRTY_MISSING_LASTNAME.add(2)
CRM_DIRTY_BAD_EMAIL.add(5)
CRM_DIRTY_INVALID_STATE.add(9)
CRM_DIRTY_MESSY_CASE.add(11)
ERP_DIRTY_MISSING_PHONE.add(3)
ERP_DIRTY_BAD_EMAIL.add(6)
ERP_DIRTY_INVALID_COUNTRY.add(12)

# ---------------------------------------------------------------------------
# Incremental-mode mutation pool: indices safe to update/delete/leave alone
# in --incremental batches without disturbing the deliberately calibrated
# curated / fuzzy-match / dirty-data test scenarios above. Restricted to
# plain "both sources, exact match, clean data" generated people only.
# ---------------------------------------------------------------------------
_ALL_IDX = set(range(1, len(PEOPLE) + 1))
_CURATED_IDX = set(range(1, len(CURATED_PEOPLE) + 1))
_DIRTY_IDX = (
    CRM_DIRTY_MISSING_LASTNAME | CRM_DIRTY_BAD_EMAIL | CRM_DIRTY_INVALID_STATE | CRM_DIRTY_MESSY_CASE
    | ERP_DIRTY_MISSING_PHONE | ERP_DIRTY_BAD_EMAIL | ERP_DIRTY_INVALID_COUNTRY
)
ELIGIBLE_FOR_INCREMENTAL = sorted(
    _ALL_IDX - _CURATED_IDX - set(FUZZY_OVERRIDES.keys()) - CRM_ONLY - ERP_ONLY - _DIRTY_IDX
)


def rand_date(days_back_max=400):
    d = datetime(2025, 1, 1) + timedelta(days=random.randint(0, days_back_max))
    return d.strftime("%Y-%m-%d")


def crm_row(idx, p, dirty=None):
    first, last, email, phone, addr, city, state, zipc, country = p
    dirty = dirty or {}
    return {
        "customer_id": f"CRM-{idx:04d}",
        "first_name": dirty.get("first_name", first),
        "last_name": dirty.get("last_name", last),
        "email": dirty.get("email", email),
        "phone": dirty.get("phone", phone),
        "address_line1": dirty.get("address_line1", addr),
        "city": dirty.get("city", city),
        "state": dirty.get("state", state),
        "zip": dirty.get("zip", zipc),
        "country": dirty.get("country", country),
        "created_date": rand_date(600),
        "modified_date": rand_date(150),
        "is_deleted": False,
    }


def erp_row(idx, p, dirty=None):
    first, last, email, phone, addr, city, state, zipc, country = p
    dirty = dirty or {}
    full_name = dirty.get("full_name", f"{last}, {first}")
    return {
        "customer_number": f"ERP-{idx:05d}",
        "full_name": full_name,
        "email_addr": dirty.get("email_addr", email.upper() if idx % 3 == 0 else email),
        "contact_phone": dirty.get("contact_phone", phone.replace("-", "")),
        "addr1": dirty.get("addr1", addr),
        "addr2": dirty.get("addr2", ""),
        "city_code": dirty.get("city_code", city),
        "state_code": dirty.get("state_code", state),
        "postal_code": dirty.get("postal_code", zipc),
        "country_code": dirty.get("country_code", country),
        "last_updated": rand_date(150),
        "is_deleted": False,
    }


crm_rows, erp_rows = [], []

for i, p in enumerate(PEOPLE, start=1):
    in_crm = i not in ERP_ONLY
    in_erp = i not in CRM_ONLY

    if in_crm:
        dirty = {}
        if i in CRM_DIRTY_MISSING_LASTNAME:
            dirty["last_name"] = ""
        if i in CRM_DIRTY_BAD_EMAIL:
            dirty["email"] = f"{p[0].lower()}.{p[1].lower()}[at]example.com"
        if i in CRM_DIRTY_INVALID_STATE:
            dirty["state"] = "ZZ"
        if i in CRM_DIRTY_MESSY_CASE:
            dirty["first_name"] = f"  {p[0].lower()} "
            dirty["last_name"] = p[1].upper()
        crm_rows.append(crm_row(i, p, dirty))

    if in_erp:
        dirty = {}
        if i in ERP_DIRTY_MISSING_PHONE:
            dirty["contact_phone"] = ""
        if i in ERP_DIRTY_BAD_EMAIL:
            dirty["email_addr"] = f"{p[0].lower()}.{p[1].lower()}@@example"
        if i in ERP_DIRTY_INVALID_COUNTRY:
            dirty["country_code"] = "USA1"
        if i in FUZZY_OVERRIDES:
            dirty.update(FUZZY_OVERRIDES[i])
        erp_rows.append(erp_row(i, p, dirty))

CRM_FIELDNAMES = [
    "customer_id", "first_name", "last_name", "email", "phone", "address_line1",
    "city", "state", "zip", "country", "created_date", "modified_date", "is_deleted",
]
ERP_FIELDNAMES = [
    "customer_number", "full_name", "email_addr", "contact_phone", "addr1", "addr2",
    "city_code", "state_code", "postal_code", "country_code", "last_updated", "is_deleted",
]

STATE_DIR = os.path.join(OUT_DIR, "_state")
STATE_PATH = os.path.join(STATE_DIR, "generator_state.json")


def _load_state():
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH) as f:
            return json.load(f)
    return None


def _save_state(state):
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(STATE_PATH, "w") as f:
        json.dump(state, f, indent=2)


def _baseline_state():
    """First-ever run: persists the deterministic baseline (crm_rows/erp_rows,
    already built above at module level) as the generator's starting state."""
    return {
        "next_idx": len(PEOPLE) + 1,
        "batch_counter": 0,
        "crm": {r["customer_id"]: dict(r) for r in crm_rows},
        "erp": {r["customer_number"]: dict(r) for r in erp_rows},
    }


def _bool_str(v):
    """csv.DictWriter writes Python bools as 'True'/'False'; normalize to
    lowercase so DuckDB's try_cast(... AS BOOLEAN) in the staging models
    (which also has to parse hand-typed 'true'/'false' consistently) is
    unambiguous either way."""
    return "true" if v else "false"


def _write_csv(path, rows, fieldnames):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            out = dict(row)
            out["is_deleted"] = _bool_str(out.get("is_deleted", False))
            w.writerow(out)


def _generate_incremental_batch(state, today_str):
    """Mutates `state` in place (updates, logical deletes, new customers) and
    returns (changed_crm_rows, changed_erp_rows) -- only the rows that changed
    in this batch, which is what actually gets written to the incremental CSV.
    Only touches the ELIGIBLE_FOR_INCREMENTAL pool (plain both-source,
    exact-match, clean-data generated people) so curated/fuzzy/dirty test
    scenarios are never disturbed by incremental churn."""
    batch_seed = 1000 + state["batch_counter"]
    random.seed(batch_seed)
    Faker.seed(batch_seed)

    active_eligible = [
        i for i in ELIGIBLE_FOR_INCREMENTAL
        if state["crm"].get(f"CRM-{i:04d}", {}).get("is_deleted") is False
    ]
    random.shuffle(active_eligible)
    n_update = min(3, len(active_eligible))
    n_delete = min(1, max(0, len(active_eligible) - n_update))
    update_idx = active_eligible[:n_update]
    delete_idx = active_eligible[n_update:n_update + n_delete]

    changed_crm, changed_erp = [], []

    for i in update_idx:
        ck, ek = f"CRM-{i:04d}", f"ERP-{i:05d}"
        crm = state["crm"].get(ck)
        if crm is not None:
            crm["address_line1"] = _fuzzy_address_variant(crm["address_line1"])
            crm["modified_date"] = today_str
            changed_crm.append(dict(crm))
        erp = state["erp"].get(ek)
        if erp is not None:
            erp["addr2"] = f"Apt {random.randint(1, 40)}"
            erp["last_updated"] = today_str
            changed_erp.append(dict(erp))

    for i in delete_idx:
        ck, ek = f"CRM-{i:04d}", f"ERP-{i:05d}"
        crm = state["crm"].get(ck)
        if crm is not None:
            crm["is_deleted"] = True
            crm["modified_date"] = today_str
            changed_crm.append(dict(crm))
        erp = state["erp"].get(ek)
        if erp is not None:
            erp["is_deleted"] = True
            erp["last_updated"] = today_str
            changed_erp.append(dict(erp))

    for _ in range(2):
        idx = state["next_idx"]
        state["next_idx"] += 1
        p = _unique_person()
        new_crm = crm_row(idx, p)
        new_crm["created_date"] = today_str
        new_crm["modified_date"] = today_str
        new_erp = erp_row(idx, p)
        new_erp["last_updated"] = today_str
        state["crm"][new_crm["customer_id"]] = new_crm
        state["erp"][new_erp["customer_number"]] = new_erp
        changed_crm.append(dict(new_crm))
        changed_erp.append(dict(new_erp))

    state["batch_counter"] += 1
    return changed_crm, changed_erp


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--full", action="store_true", help="Write a full extract (default).")
    mode.add_argument("--incremental", action="store_true", help="Write a delta batch on top of persisted state.")
    args = parser.parse_args()

    os.makedirs(OUT_DIR, exist_ok=True)
    today_str = datetime.now().strftime("%Y-%m-%d")
    state = _load_state()
    crm_path = os.path.join(OUT_DIR, "crm_customers.csv")
    erp_path = os.path.join(OUT_DIR, "erp_customers.csv")

    if args.incremental:
        if state is None:
            print("No generator state found -- bootstrapping the baseline first "
                  "(not written to CSV; run --full separately if the pipeline "
                  "still needs to see the baseline as a full load).")
            state = _baseline_state()
        changed_crm, changed_erp = _generate_incremental_batch(state, today_str)
        _save_state(state)
        _write_csv(crm_path, changed_crm, CRM_FIELDNAMES)
        _write_csv(erp_path, changed_erp, ERP_FIELDNAMES)
        n_deleted = sum(1 for r in changed_crm + changed_erp if r.get("is_deleted"))
        print(f"Incremental batch #{state['batch_counter']}: "
              f"CRM changes={len(changed_crm)}, ERP changes={len(changed_erp)}, "
              f"logical deletes={n_deleted}, stamped {today_str}")
    else:
        if state is None:
            state = _baseline_state()
            _save_state(state)
            out_crm, out_erp = crm_rows, erp_rows
            print("First run: writing the deterministic baseline and saving it as generator state.")
        else:
            out_crm = list(state["crm"].values())
            out_erp = list(state["erp"].values())
            print("Generator state already exists: writing the CURRENT full state "
                  "(baseline + every incremental change since), not a fresh baseline re-roll.")
        _write_csv(crm_path, out_crm, CRM_FIELDNAMES)
        _write_csv(erp_path, out_erp, ERP_FIELDNAMES)
        n_both = len(PEOPLE) - len(CRM_ONLY) - len(ERP_ONLY)
        print(f"Full extract: CRM rows={len(out_crm)}  ERP rows={len(out_erp)}")
        print(f"(baseline was: {len(PEOPLE)} people, both={n_both}, "
              f"crm_only={len(CRM_ONLY)}, erp_only={len(ERP_ONLY)}, "
              f"fuzzy_pairs={len(FUZZY_OVERRIDES)})")


if __name__ == "__main__":
    main()
