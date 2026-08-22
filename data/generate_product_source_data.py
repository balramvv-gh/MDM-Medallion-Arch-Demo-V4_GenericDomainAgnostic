"""
Generates synthetic source system extracts for the MDM demo's Product domain:
  - ERP export      (data/sources/erp_products.csv)        -- commercial + operational attributes
  - PLM export      (data/sources/plm_products.csv)        -- engineering attributes
  - Supplier export (data/sources/supplier_products.xlsx)  -- the supplier's own spec sheet
    (a spreadsheet, not a CSV -- see scripts/load_bronze.py's file_type='xlsx' handling)

Design intent (deterministic: random.seed + Faker.seed are both fixed, so this
script produces byte-identical output every run), directly implementing the
user's own spec for onboarding the Product domain (mirrors data/
generate_source_data.py's Customer-domain conventions where they apply):

Every master product is assigned a `pattern` describing which of the three
sources carry it, and how well its data agrees across the sources that do:

  ALL3_EXACT            in ERP+PLM+Supplier. manufacturer_part_number is the
                         SAME underlying code in every source, just formatted
                         differently (dashes/case/blank-in-supplier) -- caught
                         by tier-1 exact match once 'normalize_code' strips
                         punctuation/case. product_name and the supplier's own
                         part-number cross-reference are also deliberately
                         reformatted/stale across sources (not just the MPN),
                         per the user's "slightly mismatched...product name,
                         manufacturer/supplier Id" requirement -- these are
                         genuinely the same product, just not byte-identical
                         across systems, which is exactly what survivorship
                         rules exist to resolve.
  ALL3_FUZZY_CONFIDENT   in ERP+PLM+Supplier, but manufacturer_part_number is
                         genuinely DIFFERENT per source (not just reformatted
                         -- a different revision suffix), so tier-1 misses it
                         entirely. product_name is reworded (words reordered)
                         rather than reformatted -- close enough in the tier-2
                         TF-IDF character-n-gram similarity for an auto-merge.
  ALL3_FUZZY_SUSPECT     same MPN-genuinely-differs setup as above, but
                         product_name is more loosely worded (material/spec
                         dropped, replaced with a generic descriptor) --
                         similar enough to be a real candidate pair, not
                         similar enough to auto-merge, so it lands in the
                         Match Review queue for a steward to confirm/reject.
  ERP_PLM                in ERP+PLM only, no Supplier counterpart.
  SUPPLIER_PLM           in Supplier+PLM only, NOT in ERP -- the "product is
                         in the supplier list but not the ERP system" case
                         the user called out by name.
  SUPPLIER_ONLY          in Supplier only -- same case, more extreme (also
                         not in PLM). A singleton golden record.
  ERP_ONLY               in ERP only -- a singleton golden record.
  PLM_ONLY               in PLM only -- a singleton golden record.

A subset of rows in each source (drawn only from that source's *_ONLY pool,
so invalid data never disturbs the matching scenarios calibrated above) gets
deliberately invalid data -- missing required field, malformed number,
invalid country code -- so bus_rules.column_rules R100-R114 have real work
to do and the stewardship exception queue gets populated for Product too.

Does not support --incremental: the user's own spec (point 10) explicitly
defers incremental loads to later work. This always writes a fresh, full
extract to all three source files.

Usage:
    python data/generate_product_source_data.py
"""
import os
import string
from datetime import datetime, timedelta
import random

import pandas as pd
from faker import Faker

random.seed(4200)
fake = Faker("en_US")
Faker.seed(4200)

BASE_DIR = os.path.dirname(__file__)
OUT_DIR = os.path.join(BASE_DIR, "sources")

# ---------------------------------------------------------------------------
# Reference vocabulary
# ---------------------------------------------------------------------------
CATEGORIES = [
    ("FAST1", ["Hex Bolt", "Socket Head Cap Screw"]),
    ("FAST2", ["Wing Nut", "Flat Washer"]),
    ("FAST3", ["Lock Washer", "Threaded Rod", "Carriage Bolt"]),
    ("BRNG1", ["Ball Bearing", "Roller Bearing"]),
    ("BRNG2", ["Thrust Bearing", "Pillow Block Bearing", "Needle Bearing"]),
    ("ELEC1", ["Terminal Block", "Ring Terminal"]),
    ("ELEC2", ["Butt Connector", "Circular Connector", "Spade Terminal"]),
    ("HYDR1", ["Hydraulic Elbow Fitting", "Hydraulic Tee Fitting"]),
    ("HYDR2", ["Quick-Disconnect Coupler", "Hose Adapter", "Hydraulic Union"]),
    ("SENS1", ["Proximity Sensor", "Temperature Sensor"]),
    ("SENS2", ["Pressure Transducer", "Photoelectric Sensor", "Level Sensor"]),
    ("PACK1", ["Stretch Wrap Film", "Corrugated Box"]),
    ("PACK2", ["Packing Tape", "Void Fill Foam", "Strapping Band"]),
    ("SAFE1", ["Safety Goggles", "Hard Hat"]),
    ("SAFE2", ["Cut-Resistant Gloves", "Ear Protection Muffs", "Respirator Mask"]),
    ("PNEU1", ["Solenoid Valve", "Ball Valve"]),
    ("PNEU2", ["Check Valve", "Flow Control Valve", "Needle Valve"]),
]
SPECS = ["M6x20", "M8x40", "M10x50", "1/4in", "3/8in", "1/2in", "608ZZ", "6205", "NPT-1/2", "DN25", "5/16in", "M12x60"]
MATERIALS = ["Stainless Steel", "Carbon Steel", "Aluminum", "Brass", "Nylon", "Titanium", "Zinc-Plated Steel", "PVC", "Bronze", "ABS Plastic"]
COLORS = ["Silver", "Black", "Natural", "Zinc", "Blue", "Yellow", "Red", "Clear", "Gray", "Green"]
UOM_ERP = ["EA", "BOX", "CS", "PK"]
UOM_PLM = ["EACH", "BOX", "CASE", "PACK"]  # same meaning, different vocabulary than ERP's -- resolved by 'most_common' survivorship (SR104)
MANUFACTURERS = [
    "Ferrotech Industries", "Meridian Fasteners Co", "BrightBearing Corp", "Voltaic Connectors LLC",
    "HydroFlow Systems", "SenTech Instruments", "PackRight Supply", "GuardianPro Safety",
    "AeroValve Dynamics", "Summit Industrial Group", "Precision Components Inc", "Ironclad Manufacturing",
    "Apex Motion Systems", "Coastal Supply Corp", "Vertex Engineering",
]
# Must be active rows in ref.ref_country_codes (see scripts/ensure_governed_schemas.py).
COUNTRIES = ["US", "MX", "CN", "DE", "IN", "JP"]
LIFECYCLE_STATUSES = ["Active", "Prototype", "Obsolete"]
PRODUCT_STATUSES = ["Active", "Discontinued", "Pending"]

# 30 (erp_vendor_id, supplier_account_id) identity pairs -- the same
# real-world supplier deliberately coded differently by ERP (its own
# internal vendor master) vs by the supplier's own systems (its own account
# id), per source_field_mappings SFM108/SFM132's docstring.
SUPPLIER_IDENTITIES = [(f"VEN-{1000 + i}", f"ACCT-{5000 + i}") for i in range(30)]

_used_mpn = set()


def _mpn_base(manufacturer):
    initials = "".join(w[0] for w in manufacturer.split()[:2]).upper()
    while True:
        candidate = f"{initials}{random.randint(1000, 9999)}{random.choice(string.ascii_uppercase[:8])}"
        if candidate not in _used_mpn:
            _used_mpn.add(candidate)
            return candidate


def _mpn_reformat_dashed_upper(mpn):
    """'FE4471A' -> 'FE-4471-A' -- same underlying code, ERP's own display
    convention. normalize_code (strip non-alnum, uppercase) recovers the
    identical value regardless. _mpn_base always builds mpn as exactly
    2 manufacturer-initial letters + 4 digits + 1 revision letter -- this
    slices on that fixed shape, not on len(mpn), so it never silently
    produces an empty trailing segment for a differently-sized manufacturer
    initials string."""
    return f"{mpn[:2]}-{mpn[2:6]}-{mpn[6:]}"


def _mpn_reformat_lower_nodash(mpn):
    """'FER4471A' -> 'fer4471a' -- Supplier's own lowercase convention, no
    separators. Still normalizes identically to the other two."""
    return mpn.lower()


def _mpn_genuinely_different(mpn):
    """Bumps the trailing revision letter -- a REAL identity difference (not
    just formatting), so normalize_code does NOT bring this back in line with
    the original. Used for ALL3_FUZZY_* patterns, where tier-1 exact match on
    manufacturer_part_number must genuinely fail."""
    base, suffix = mpn[:-1], mpn[-1]
    letters = string.ascii_uppercase[:8]
    new_suffix = letters[(letters.index(suffix) + 1) % len(letters)]
    return base + new_suffix


def _name_reordered_variant(name):
    """Word-reordered restatement of the same product name -- textually
    different but every word (and therefore almost every character n-gram)
    is preserved, so TF-IDF char n-gram cosine similarity stays high (empirically
    ~1.0 against the original). Used for ALL3_FUZZY_CONFIDENT, where the
    intent is "clearly the same product" via similarity alone."""
    words = name.split()
    if len(words) >= 3:
        words[0], words[-1] = words[-1], words[0]
    return " ".join(words)


def _name_loose_variant(component, tag):
    """A supplier catalog entry that drops the material/spec qualifiers
    entirely and restates just the component with a generic descriptor --
    still recognizably the same item (it keeps the unique #tag, see
    product_name's own docstring below), but with much less textual overlap
    against the ERP/PLM name (empirically ~0.5-0.6 cosine similarity: above
    the review_lower_threshold, below auto_merge_threshold). Used for
    ALL3_FUZZY_SUSPECT, where the intent is "plausible candidate, needs a
    steward's judgment call"."""
    return f"{component} - Industrial Grade #{tag}"


def rand_date(days_back_min=1, days_back_max=500):
    d = datetime(2025, 6, 1) - timedelta(days=random.randint(days_back_min, days_back_max))
    return d.strftime("%Y-%m-%d")


def _bool_str(v):
    """Matches data/generate_source_data.py's convention: lowercase
    'true'/'false' strings, unambiguous for DuckDB's try_cast(... AS BOOLEAN)
    whether the value is hand-typed or comes from csv.DictWriter/pandas."""
    return "true" if v else "false"


# ---------------------------------------------------------------------------
# Master product population
# ---------------------------------------------------------------------------
PATTERN_COUNTS = [
    ("ALL3_EXACT", 30),
    ("ALL3_FUZZY_CONFIDENT", 8),
    ("ALL3_FUZZY_SUSPECT", 7),
    ("ERP_PLM", 15),
    ("SUPPLIER_PLM", 12),
    ("SUPPLIER_ONLY", 30),
    ("ERP_ONLY", 25),
    ("PLM_ONLY", 18),
]

MASTER = []
_idx = 1
for pattern, count in PATTERN_COUNTS:
    for _ in range(count):
        cat_code, components = random.choice(CATEGORIES)
        component = random.choice(components)
        material = random.choice(MATERIALS)
        color = random.choice(COLORS)
        spec = random.choice(SPECS)
        manufacturer = random.choice(MANUFACTURERS)
        mpn = _mpn_base(manufacturer)
        erp_vendor_id, supplier_account_id = random.choice(SUPPLIER_IDENTITIES)
        supplier_part_number = f"SPN-{_idx:05d}"
        # ERP's own cross-reference copy of the supplier's part number --
        # stale (off by a digit) for ~30% of multi-source products, per
        # SFM109's docstring ("occasionally stale/mismatched"). A pure
        # cosmetic/survivorship-facing mismatch, not a matching field.
        erp_supplier_part_number = supplier_part_number
        if pattern != "SUPPLIER_ONLY" and random.random() < 0.30:
            erp_supplier_part_number = f"SPN-{_idx:05d}X"
        MASTER.append({
            "idx": _idx,
            "pattern": pattern,
            "category": cat_code,
            "component": component,
            # #<idx> is a unique-per-master-product tag baked into the name
            # itself (a real catalog convention -- many industrial listings
            # include the manufacturer's own part/model number in the
            # description). Without it, two DIFFERENT products sharing the
            # same category + component word (e.g. two different "Hex Bolt"
            # listings from different manufacturers) would look deceptively
            # similar to the TF-IDF character-n-gram similarity scorer,
            # flooding the Match Review queue with false-positive candidate
            # pairs that aren't actually the same real-world product. The tag
            # is preserved by every product_name variant below (including the
            # loose/reordered ones used for ALL3_FUZZY_*), so it keeps
            # genuinely-the-same-product pairs correctly similar while
            # sharply reducing accidental cross-product collisions.
            "product_name": f"{material} {component} {spec} #{_idx:04d}",
            "manufacturer": manufacturer,
            "mpn": mpn,
            "material": material,
            "color": color,
            "weight_kg": round(random.uniform(0.05, 12.5), 2),
            "price": round(random.uniform(1.5, 450.0), 2),
            "lead_time_days": random.randint(3, 45),
            "moq": random.choice([1, 5, 10, 25, 50, 100]),
            "country": random.choice(COUNTRIES),
            "lifecycle_status": random.choice(LIFECYCLE_STATUSES),
            "product_status": random.choice(PRODUCT_STATUSES),
            "spec_version": f"REV-{random.randint(1, 9)}.{random.randint(0, 9)}",
            "erp_vendor_id": erp_vendor_id,
            "supplier_account_id": supplier_account_id,
            "supplier_part_number": supplier_part_number,
            "erp_supplier_part_number": erp_supplier_part_number,
        })
        _idx += 1


def _erp_mpn(m):
    if m["pattern"] in ("ALL3_FUZZY_CONFIDENT", "ALL3_FUZZY_SUSPECT"):
        return _mpn_reformat_dashed_upper(_mpn_genuinely_different(m["mpn"]))
    return _mpn_reformat_dashed_upper(m["mpn"])


def _plm_mpn(m):
    # PLM's mpn is always the canonical, undecorated code -- engineering is
    # this demo's system of record for the manufacturer part number
    # (SR101), so its own value never gets a "genuinely different" variant.
    return m["mpn"]


def _supplier_mpn(m):
    if m["pattern"] in ("ALL3_FUZZY_CONFIDENT", "ALL3_FUZZY_SUSPECT"):
        return _mpn_reformat_lower_nodash(_mpn_genuinely_different(m["mpn"]) + "X")
    return _mpn_reformat_lower_nodash(m["mpn"])


def _erp_name(m):
    if m["pattern"] == "ALL3_FUZZY_CONFIDENT":
        return _name_reordered_variant(m["product_name"])
    if m["pattern"] == "ALL3_FUZZY_SUSPECT":
        return _name_loose_variant(m["component"], f"{m['idx']:04d}")
    return m["product_name"]


def _supplier_name(m):
    if m["pattern"] == "ALL3_FUZZY_CONFIDENT":
        return _name_reordered_variant(m["product_name"])
    if m["pattern"] == "ALL3_FUZZY_SUSPECT":
        return _name_loose_variant(m["component"], f"{m['idx']:04d}")
    return m["product_name"]


def erp_row(m, dirty=None):
    dirty = dirty or {}
    return {
        "item_number": f"ERP-{m['idx']:05d}",
        "item_description": dirty.get("item_description", _erp_name(m)),
        "mfr_part_no": dirty.get("mfr_part_no", _erp_mpn(m)),
        "category_code": m["category"],
        "uom": dirty.get("uom", random.choice(UOM_ERP)),
        "unit_price": dirty.get("unit_price", f"{m['price']:.2f}"),
        "currency": "USD",
        "item_status": dirty.get("item_status", m["product_status"]),
        "gross_weight": dirty.get("gross_weight", f"{m['weight_kg']:.2f} kg"),
        "vendor_id": dirty.get("vendor_id", m["erp_vendor_id"]),
        "vendor_part_no": dirty.get("vendor_part_no", m["erp_supplier_part_number"]),
        "created_on": rand_date(200, 700),
        "last_modified": rand_date(1, 150),
        "is_deleted": False,
    }


def plm_row(m, dirty=None):
    dirty = dirty or {}
    return {
        "part_id": f"PLM-{m['idx']:05d}",
        "part_name": dirty.get("part_name", m["product_name"]),
        "mpn": dirty.get("mpn", _plm_mpn(m)),
        "mfr_name": dirty.get("mfr_name", m["manufacturer"]),
        "product_family": m["category"],
        "uom_code": dirty.get("uom_code", random.choice(UOM_PLM)),
        "lifecycle_stage": dirty.get("lifecycle_stage", m["lifecycle_status"]),
        "weight_kg": dirty.get("weight_kg", f"{m['weight_kg']:.2f}"),
        "material_spec": dirty.get("material_spec", m["material"]),
        "color_spec": dirty.get("color_spec", m["color"]),
        "origin_country": dirty.get("origin_country", m["country"]),
        "spec_rev": dirty.get("spec_rev", m["spec_version"]),
        "date_created": rand_date(200, 700),
        "date_modified": rand_date(1, 150),
        "is_deleted": False,
    }


def supplier_row(m, dirty=None):
    dirty = dirty or {}
    return {
        "supplier_sku": f"SUP-{m['idx']:05d}",
        "product_title": dirty.get("product_title", _supplier_name(m)),
        "cross_ref_mpn": dirty.get("cross_ref_mpn", _supplier_mpn(m)),
        "mfg_name": dirty.get("mfg_name", m["manufacturer"]),
        "commodity_code": m["category"],
        "material_type": dirty.get("material_type", m["material"]),
        "color_desc": dirty.get("color_desc", m["color"]),
        "country_of_mfg": dirty.get("country_of_mfg", m["country"]),
        "vendor_account_id": dirty.get("vendor_account_id", m["supplier_account_id"]),
        "supplier_part_no": dirty.get("supplier_part_no", m["supplier_part_number"]),
        "lead_time": dirty.get("lead_time", str(m["lead_time_days"])),
        "moq": dirty.get("moq", str(m["moq"])),
        "date_added": rand_date(200, 700),
        "date_updated": rand_date(1, 150),
        "is_deleted": False,
    }


IN_ERP = {"ALL3_EXACT", "ALL3_FUZZY_CONFIDENT", "ALL3_FUZZY_SUSPECT", "ERP_PLM", "ERP_ONLY"}
IN_PLM = {"ALL3_EXACT", "ALL3_FUZZY_CONFIDENT", "ALL3_FUZZY_SUSPECT", "ERP_PLM", "SUPPLIER_PLM", "PLM_ONLY"}
IN_SUPPLIER = {"ALL3_EXACT", "ALL3_FUZZY_CONFIDENT", "ALL3_FUZZY_SUSPECT", "SUPPLIER_PLM", "SUPPLIER_ONLY"}

# ---------------------------------------------------------------------------
# Invalid-record injection: only from each source's *_ONLY pool, so dirty
# data never disturbs the matching scenarios calibrated above. Deterministic
# (seeded random.sample over that pool), same convention as
# data/generate_source_data.py's CRM_DIRTY_*/ERP_DIRTY_* sets.
# ---------------------------------------------------------------------------
_erp_only_idx = [m["idx"] for m in MASTER if m["pattern"] == "ERP_ONLY"]
_plm_only_idx = [m["idx"] for m in MASTER if m["pattern"] == "PLM_ONLY"]
_supplier_only_idx = [m["idx"] for m in MASTER if m["pattern"] == "SUPPLIER_ONLY"]

random.shuffle(_erp_only_idx)
random.shuffle(_plm_only_idx)
random.shuffle(_supplier_only_idx)

ERP_DIRTY_MISSING_NAME = set(_erp_only_idx[0:2])
ERP_DIRTY_MISSING_MPN = set(_erp_only_idx[2:4])
ERP_DIRTY_BAD_PRICE = set(_erp_only_idx[4:7])
ERP_DIRTY_MISSING_VENDOR = set(_erp_only_idx[7:9])
ERP_DIRTY_MESSY_STATUS = set(_erp_only_idx[9:11])

PLM_DIRTY_MISSING_NAME = set(_plm_only_idx[0:2])
PLM_DIRTY_MISSING_MPN = set(_plm_only_idx[2:4])
PLM_DIRTY_BAD_WEIGHT = set(_plm_only_idx[4:6])
PLM_DIRTY_INVALID_COUNTRY = set(_plm_only_idx[6:8])
PLM_DIRTY_MESSY_MFR_NAME = set(_plm_only_idx[8:10])

SUPPLIER_DIRTY_MISSING_TITLE = set(_supplier_only_idx[0:2])
SUPPLIER_DIRTY_MISSING_VENDOR = set(_supplier_only_idx[2:4])
SUPPLIER_DIRTY_BAD_LEAD_TIME = set(_supplier_only_idx[4:6])
SUPPLIER_DIRTY_BAD_MOQ = set(_supplier_only_idx[6:8])
SUPPLIER_DIRTY_INVALID_COUNTRY = set(_supplier_only_idx[8:10])

erp_rows, plm_rows, supplier_rows = [], [], []

for m in MASTER:
    idx = m["idx"]
    if m["pattern"] in IN_ERP:
        dirty = {}
        if idx in ERP_DIRTY_MISSING_NAME:
            dirty["item_description"] = ""
        if idx in ERP_DIRTY_MISSING_MPN:
            dirty["mfr_part_no"] = ""
        if idx in ERP_DIRTY_BAD_PRICE:
            dirty["unit_price"] = random.choice(["N/A", "TBD", "-49.99"])
        if idx in ERP_DIRTY_MISSING_VENDOR:
            dirty["vendor_id"] = ""
        if idx in ERP_DIRTY_MESSY_STATUS:
            dirty["item_status"] = f"  {m['product_status'].lower()}  "
        erp_rows.append(erp_row(m, dirty))

    if m["pattern"] in IN_PLM:
        dirty = {}
        if idx in PLM_DIRTY_MISSING_NAME:
            dirty["part_name"] = ""
        if idx in PLM_DIRTY_MISSING_MPN:
            dirty["mpn"] = ""
        if idx in PLM_DIRTY_BAD_WEIGHT:
            dirty["weight_kg"] = random.choice(["unknown", "12,5"])
        if idx in PLM_DIRTY_INVALID_COUNTRY:
            dirty["origin_country"] = "ZZ"
        if idx in PLM_DIRTY_MESSY_MFR_NAME:
            dirty["mfr_name"] = f"  {m['manufacturer'].upper()}  "
        plm_rows.append(plm_row(m, dirty))

    if m["pattern"] in IN_SUPPLIER:
        dirty = {}
        if idx in SUPPLIER_DIRTY_MISSING_TITLE:
            dirty["product_title"] = ""
        if idx in SUPPLIER_DIRTY_MISSING_VENDOR:
            dirty["vendor_account_id"] = ""
        if idx in SUPPLIER_DIRTY_BAD_LEAD_TIME:
            dirty["lead_time"] = random.choice(["2-3 weeks", "approx 10 days"])
        if idx in SUPPLIER_DIRTY_BAD_MOQ:
            dirty["moq"] = random.choice(["N/A", "min 25"])
        if idx in SUPPLIER_DIRTY_INVALID_COUNTRY:
            dirty["country_of_mfg"] = "XX"
        supplier_rows.append(supplier_row(m, dirty))

ERP_FIELDNAMES = [
    "item_number", "item_description", "mfr_part_no", "category_code", "uom", "unit_price",
    "currency", "item_status", "gross_weight", "vendor_id", "vendor_part_no",
    "created_on", "last_modified", "is_deleted",
]
PLM_FIELDNAMES = [
    "part_id", "part_name", "mpn", "mfr_name", "product_family", "uom_code", "lifecycle_stage",
    "weight_kg", "material_spec", "color_spec", "origin_country", "spec_rev",
    "date_created", "date_modified", "is_deleted",
]
SUPPLIER_FIELDNAMES = [
    "supplier_sku", "product_title", "cross_ref_mpn", "mfg_name", "commodity_code",
    "material_type", "color_desc", "country_of_mfg", "vendor_account_id", "supplier_part_no",
    "lead_time", "moq", "date_added", "date_updated", "is_deleted",
]


def _write_csv(path, rows, fieldnames):
    import csv
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            out = dict(row)
            out["is_deleted"] = _bool_str(out.get("is_deleted", False))
            w.writerow(out)


def _write_xlsx(path, rows, fieldnames):
    out_rows = []
    for row in rows:
        out = dict(row)
        out["is_deleted"] = _bool_str(out.get("is_deleted", False))
        out_rows.append(out)
    df = pd.DataFrame(out_rows, columns=fieldnames)
    df.to_excel(path, index=False, sheet_name="products")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    erp_path = os.path.join(OUT_DIR, "erp_products.csv")
    plm_path = os.path.join(OUT_DIR, "plm_products.csv")
    supplier_path = os.path.join(OUT_DIR, "supplier_products.xlsx")

    _write_csv(erp_path, erp_rows, ERP_FIELDNAMES)
    _write_csv(plm_path, plm_rows, PLM_FIELDNAMES)
    _write_xlsx(supplier_path, supplier_rows, SUPPLIER_FIELDNAMES)

    pattern_counts = {}
    for m in MASTER:
        pattern_counts[m["pattern"]] = pattern_counts.get(m["pattern"], 0) + 1

    print(f"Product source extracts written: ERP rows={len(erp_rows)}, "
          f"PLM rows={len(plm_rows)}, Supplier rows={len(supplier_rows)}")
    print(f"Master products: {len(MASTER)} -- by pattern: {pattern_counts}")


if __name__ == "__main__":
    main()
