import re
from dateutil import parser as date_parser
import psycopg2


#ocrden extract edilen keyleri valuelara eşlemek icin dict
COLUMNS = {
    "file no":         "file_no",
    "date of birth":   "date_of_birth",
    "date of birth(dd-mm-yyyy)": "date_of_birth",
    "place of birth":  "place_of_birth",
    "sex":             "sex",
    "forename":        "forenames",
    "forenames":       "forenames",
    "forename(s)":     "forenames",
    "family name":     "family_name",
    "nationality":     "nationality",
    "nationalities":   "nationality",
    # type/number buraya gelmiyor pipe‑case ve global fallback kullanacağım 
    "casetown":        "casetown",
    "case town":       "casetown",
    "casecountry":     "casecountry",
    "case country":    "casecountry",
    "casedate":        "casedate",
    "case date":       "casedate",
    "case date(dd-mm-yyyy)":      "casedate",
}

def normalize_raw(raw: str) -> str:
    
    #raw OCR çıktısında her anahtar kelimeyi yeni bir satıra çekme
    
    for key in COLUMNS.keys():
        pat = re.compile(
            rf"(?i)\b{re.escape(key)}"   # key kelimesi
            r"(?:\s*\([^)]*\))?"         # opsiyonel parantez içeriği
            r"\s*:"                      # ardından iki nokta
        )
        raw = pat.sub(lambda m: "\n" + m.group(0), raw)
    return raw
#normalize_raw ile satırlara bölünen metinden attribute dictionary çıkaran func
def parse_attributes(raw_text: str) -> dict:
    #normalize ve wrap‑merge
    norm = normalize_raw(raw_text)#metin key kelimeler oncesi newline ile bölünüyor
    raw_lines = norm.splitlines()#satırları ayırarak listeye atma
    lines = []
    for ln in raw_lines:
        if (ln.startswith(" ") or ln.startswith("\t")) and lines:
            lines[-1] += " " + ln.strip()#eğer bir önceki satırın devamıysa mevcut satıra ekleme
        else:
            lines.append(ln.strip())

    print("🔍 DEBUG normalized lines:")
    for l in lines:
        print("   ►", repr(l))

    # ——— Yeni ekleme: Key:Value satırlarını da yaz ———
    print("🔍 DEBUG Key:Value candidates:")
    kv_re = re.compile(r'^\s*([^:]+?)\s*:\s*(.+)$')#burdaki deseni re.compile ile compile ettim oluşan nesneyi kv_re ye atadım
    for l in lines:
        m = kv_re.match(l)#match objesi m.group(1) le keyleri 2 ile valueları tutcam
        if m:
            print(f"   • raw_key={m.group(1).strip().lower()!r}, raw_val={m.group(2).strip()!r}")

    record = {}
    kv_re      = re.compile(r'^\s*([^:]+?)\s*:\s*(.+)$')
    list_entry = re.compile(r'^\d+\.\s*(\S+)\s+(\S+)\s+(.+)$')

    for idx, ln in enumerate(lines):
        ln = ln.strip()
        if not ln:
            continue

        
        if re.match(r'^\s*town\s+country\s+date\s*$', ln, re.IGNORECASE):
            # alt satırı tablo olarak işle
            if idx + 1 < len(lines):
                vals = re.split(r'\s{2,}', lines[idx+1].strip())
                if len(vals) >= 3:
                    record['casetown']    = vals[0].strip()
                    record['casecountry'] = vals[1].strip()
                    # Date hücresinden ilk tarihi al
                    mdate = re.search(r'\d{1,2}\s+\w+\s+\d{4}', vals[2])
                    if mdate:
                        try:
                            record['casedate'] = date_parser.parse(
                                mdate.group(0), dayfirst=True
                            ).date()
                        except:
                            record['casedate'] = None
            continue

        # TYPE|NUMBER ───
        if '|' in ln:
            left, right = [p.strip() for p in ln.split('|', 1)]
            if re.fullmatch(r"[A-Za-z ]+", left) and re.match(r"^\d", right):
                record['type']   = left
                record['number'] = right
                continue

        #list‑entry’den sadece nationality 
        m_list = list_entry.match(ln)
        if m_list and 'nationality' not in record:
            record['nationality'] = m_list.group(1)
            continue

        #key value satırları
        m = kv_re.match(ln)
        if not m:
            continue

        raw_key = m.group(1).strip().lower()
        raw_val = m.group(2).strip()

        if raw_key.startswith("Date and place of birth:"):
            
            parts = re.split(r'\s{2,}', raw_val, maxsplit=1)
            date_part  = parts[0].split()[0]
            place_part = parts[1] if len(parts) > 1 else ""
            try:
                record["date_of_birth"]  = date_parser.parse(date_part, dayfirst=True).date()
            except:
                record["date_of_birth"] = None
            record["place_of_birth"] = place_part.strip() or None
            continue
              
        if raw_key.startswith("date and place of birth"):
            
            # - işaretleri etrafındaki boşluklara göre bölüyoruz
            parts = [p.strip() for p in re.split(r'\s*[-–]\s*', raw_val)]
            # parts == ["16 June 2006", "Istanbul", "Turkey"]
            #tarihi parse et
            try:
                #ingilizce ay adı var dayfirst=False uygun
                record['date_of_birth'] = date_parser.parse(parts[0], dayfirst=False).date()
            except:
                record['date_of_birth'] = None
            # 2) Şehir ve ülkeyi birleştir
            if len(parts) >= 3:
                record['place_of_birth'] = f"{parts[1]}, {parts[2]}"
            elif len(parts) == 2:
                record['place_of_birth'] = parts[1]
            else:
                record['place_of_birth'] = None
            continue

        
        if "file" in raw_key and "no" in raw_key:
            
            record["file_no"] = raw_val
            continue

        
        if raw_key == "date":
            dates = re.findall(r'\d{1,2}[/-]\d{1,2}[/-]\d{4}', raw_val)
            record['casedate'] = (
                date_parser.parse(dates[0], dayfirst=True).date()
                if dates else None
            )
            continue

        
        if raw_key == "place":
            parts = [p.strip() for p in raw_val.split(",")]
            if len(parts) > 1:
                record['casetown']    = ", ".join(parts[:-1])
                record['casecountry'] = parts[-1]
            else:
                record['casetown'] = raw_val
            continue

        #generic COLUMNS veya substring‑fallback
        col = None
        if raw_key in COLUMNS:
            col = COLUMNS[raw_key]
        else:
            if 'forename' in raw_key:
                col = 'forenames'
            elif 'type' in raw_key:
                col = 'type'
            elif 'number' in raw_key:
                col = 'number'

        if not col:
            continue

        clean_val = re.split(r'\s{2,}', raw_val)[0].strip()

        if "date" in col:
            try:
                record[col] = date_parser.parse(clean_val, dayfirst=True).date()
            except:
                f2 = re.search(r'\d{1,2}[/-]\d{1,2}[/-]\d{4}', raw_text)
                record[col] = (
                    date_parser.parse(f2.group(0), dayfirst=True).date()
                    if f2 else None
                )
        else:
            record.setdefault(col, clean_val)

    #Global fallback: raw_text içinde Passport
    if not record.get('type') or not record.get('number'):
        m = re.search(
            r'\b(Passport|Visa|ID)\b[^\d\n\r]*(\d[\d\s№]+)',
            raw_text, re.IGNORECASE
        )
        if m:
            record.setdefault('type',   m.group(1))
            record.setdefault('number', m.group(2).strip())

        #Global fallback: File No
    if 'file_no' not in record:
        mfn = re.search(
            r'file\s*(?:no|number)\s*[:#]?\s*([\d/]+)',
            raw_text, re.IGNORECASE
        )
        if mfn:
            record['file_no'] = mfn.group(1)

    #Global fallback: Date of Birth 
    if 'date_of_birth' not in record:
        mdb = re.search(
            r'date\s*of\s*birth[^\d]*(\d{1,2}[/-]\d{1,2}[/-]\d{4})',
            raw_text, re.IGNORECASE
        )
        if mdb:
            try:
                record['date_of_birth'] = date_parser.parse(
                    mdb.group(1), dayfirst=True
                ).date()
            except:
                record['date_of_birth'] = None


    return record

def save_to_db(record: dict, raw_text: str):
    """
    Elde edilen sözlüğü ve ham OCR çıktısını veri tabanına kaydeder.
    """
    print("DEBUG save_to_db called with record:", record)
    conn = psycopg2.connect(
        host="localhost",
        database="ocr_db",
        user="postgres",
        password="datateam1907"
    )
    cur = conn.cursor()

    cols = list(record.keys()) + ["raw_text"]
    vals = [record[c] for c in record.keys()] + [raw_text]
    ph = ", ".join(["%s"] * len(vals))
    sql = f"INSERT INTO pdf_common_datas ({', '.join(cols)}) VALUES ({ph})"

    cur.execute(sql, vals)
    conn.commit()
    cur.close()
    conn.close() 