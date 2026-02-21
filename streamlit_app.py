# streamlit_app.py
# -*- coding: utf-8 -*-

import os
import re
import json
import html
from datetime import datetime, date, time
from typing import Any, Dict, List, Optional, Tuple
from io import BytesIO
import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components
import gspread


# =========================
# CONFIG / CONSTANTS
# =========================

SHEET_COLUMNS = [
    "record_id",
    "unit_name",
    "app_title",
    "event_date",                # YYYY-MM-DD
    "event_time",                # HH:MM
    "process_step",              # สั่งใช้ยา / จัด/จ่ายยา / ให้ยา / ผู้ป่วยใช้ยาผิดวิธี
    "drug_name",
    "severity_level",            # A-I
    "incident_detail",
    "timeline_text",             # เพิ่ม
    "initial_correction",        # เพิ่ม
    "rca_text",                  # เพิ่ม
    "rca_image_filename",        # ชื่อไฟล์ภาพ (ไม่เก็บ binary ลง GSheet)
    "development_plan",          # เพิ่ม
    "created_at",                # ISO datetime
    "created_by",                # login username (optional)
]

PROCESS_OPTIONS = ["สั่งใช้ยา", "จัด/จ่ายยา", "ให้ยา", "ผู้ป่วยใช้ยาผิดวิธี"]
SEVERITY_OPTIONS = list("ABCDEFGHI")


# =========================
# PAGE SETUP
# =========================

st.set_page_config(
    page_title="PHOIR",
    page_icon="🏡",
    layout="wide",
)


# =========================
# HELPER: READ CONFIG (ENV ONLY for Render)
# =========================

def _get_env(
    key: str,
    default: Optional[str] = None,
    aliases: Optional[List[str]] = None,
) -> Optional[str]:
    """ดึงค่าจาก Environment Variables เท่านั้น (ไม่ใช้ st.secrets)"""
    keys = [key] + (aliases or [])
    for k in keys:
        v = os.getenv(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return default


def get_app_config() -> Dict[str, Any]:
    app_title = _get_env("APP_TITLE", "PHOIR_DEMO")
    unit_name = _get_env("UNIT_NAME", "unknown-unit")
    login_user = _get_env("APP_LOGIN_USERNAME", "")
    login_pass = _get_env("APP_LOGIN_PASSWORD", "")

    # รองรับชื่อที่ถูกต้อง + เผื่อพิมพ์ผิด (GHEET_WORKSHEET)
    gsheet_url = _get_env("GSHEET_URL", "")
    worksheet_name = _get_env("GSHEET_WORKSHEET", "PHOIR_DEMO", aliases=["GHEET_WORKSHEET"])

    # รองรับ alias เผื่อใช้ชื่อเก่า
    gcp_sa_json = _get_env("GCP_SERVICE_ACCOUNT_JSON", "", aliases=["GSHEET_CREDENTIALS_JSON"])
    gemini_api_key = _get_env("GEMINI_API_KEY", "")

    return {
        "APP_TITLE": app_title,
        "UNIT_NAME": unit_name,
        "APP_LOGIN_USERNAME": login_user,
        "APP_LOGIN_PASSWORD": login_pass,
        "GSHEET_URL": gsheet_url,
        "GSHEET_WORKSHEET": worksheet_name,
        "GCP_SERVICE_ACCOUNT_JSON": gcp_sa_json,
        "GEMINI_API_KEY": gemini_api_key,
    }


CFG = get_app_config()


# =========================
# STYLING
# =========================

st.markdown(
    """
<style>
.block-container { padding-top: 1.2rem; }
.small-muted { color: #6b7280; font-size: 0.88rem; }
.card {
    border: 1px solid #e5e7eb;
    border-radius: 14px;
    padding: 14px;
    background: #ffffff;
}
.section-title {
    font-size: 1.05rem;
    font-weight: 700;
    margin-bottom: .5rem;
}
.fishbone-wrap {
    border: 1px solid #e5e7eb;
    border-radius: 12px;
    padding: 8px;
    background: white;
    overflow-x: auto;
}
</style>
    """,
    unsafe_allow_html=True,
)


# =========================
# LOGIN
# =========================

def ensure_auth_state():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    if "login_username" not in st.session_state:
        st.session_state.login_username = ""


def render_login():
    ensure_auth_state()

    st.markdown(f"# 🏡 {CFG['APP_TITLE']}")
    st.markdown(f"<div class='small-muted'>บันทึกอุบัติการณ์ในสถานพยาบาลปฐมภูมิ</div>", unsafe_allow_html=True)
    st.markdown("---")

    c1, c2, c3 = st.columns([1, 1.6, 1])
    with c2:
        st.markdown("## 🔐 เข้าสู่ระบบ")
        st.caption(f"หน่วยงาน: **{CFG['UNIT_NAME']}**")

        username = st.text_input("ชื่อผู้ใช้", key="login_user_input")
        password = st.text_input("รหัสผ่าน", type="password", key="login_pass_input")

        if st.button("เข้าสู่ระบบ", use_container_width=True):
            expected_user = CFG["APP_LOGIN_USERNAME"]
            expected_pass = CFG["APP_LOGIN_PASSWORD"]

            # ถ้ายังไม่ได้ตั้งค่า login ใน env ให้ bypass แบบ dev
            if not expected_user or not expected_pass:
                st.session_state.authenticated = True
                st.session_state.login_username = username or "dev-user"
                st.warning("ยังไม่ได้ตั้งค่า APP_LOGIN_USERNAME / APP_LOGIN_PASSWORD ใน ENV → เข้าแบบ dev mode")
                st.rerun()

            if username == expected_user and password == expected_pass:
                st.session_state.authenticated = True
                st.session_state.login_username = username
                st.success("เข้าสู่ระบบสำเร็จ ✅")
                st.rerun()
            else:
                st.error("ชื่อผู้ใช้หรือรหัสผ่านไม่ถูกต้อง")


# =========================
# GOOGLE SHEETS
# =========================

@st.cache_resource(show_spinner=False)
def get_gspread_client():
    sa_json_str = CFG["GCP_SERVICE_ACCOUNT_JSON"]
    if not sa_json_str:
        raise ValueError("ไม่พบ GCP_SERVICE_ACCOUNT_JSON ใน Environment Variables")

    try:
        creds_dict = json.loads(sa_json_str)
    except json.JSONDecodeError as e:
        raise ValueError(f"GCP_SERVICE_ACCOUNT_JSON ไม่ใช่ JSON ที่ถูกต้อง: {e}")

    client = gspread.service_account_from_dict(creds_dict)
    return client


@st.cache_resource(show_spinner=False)
def get_worksheet():
    gsheet_url = CFG["GSHEET_URL"]
    worksheet_name = CFG["GSHEET_WORKSHEET"]

    if not gsheet_url:
        raise ValueError("ไม่พบ GSHEET_URL ใน Environment Variables")

    client = get_gspread_client()
    sh = client.open_by_url(gsheet_url)

    try:
        ws = sh.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet_name, rows=1000, cols=40)

    # ensure header row
    header = ws.row_values(1)
    if not header:
        ws.append_row(SHEET_COLUMNS, value_input_option="USER_ENTERED")
    else:
        # ถ้าหัวตารางยังไม่ครบ ให้เติมเฉพาะคอลัมน์ที่ขาดท้ายแถว (ไม่ทำ destructive)
        missing_cols = [c for c in SHEET_COLUMNS if c not in header]
        if missing_cols:
            # อ่านข้อมูลทั้งหมดแล้วจัดโครงสร้างใหม่แบบปลอดภัย
            all_vals = ws.get_all_values()
            if all_vals:
                df_old = pd.DataFrame(all_vals[1:], columns=all_vals[0])
            else:
                df_old = pd.DataFrame(columns=[])

            for col in SHEET_COLUMNS:
                if col not in df_old.columns:
                    df_old[col] = ""

            df_old = df_old[SHEET_COLUMNS]

            ws.clear()
            ws.append_row(SHEET_COLUMNS, value_input_option="USER_ENTERED")
            if not df_old.empty:
                ws.append_rows(df_old.fillna("").astype(str).values.tolist(), value_input_option="USER_ENTERED")

    return ws


def append_record_to_sheet(record: Dict[str, Any]) -> None:
    ws = get_worksheet()

    row = []
    for col in SHEET_COLUMNS:
        val = record.get(col, "")
        if val is None:
            val = ""
        row.append(str(val))

    ws.append_row(row, value_input_option="USER_ENTERED")


def load_sheet_df() -> pd.DataFrame:
    ws = get_worksheet()
    records = ws.get_all_records(expected_headers=SHEET_COLUMNS)

    if not records:
        return pd.DataFrame(columns=SHEET_COLUMNS)

    df = pd.DataFrame(records)

    # ให้แน่ใจว่ามีทุกคอลัมน์
    for c in SHEET_COLUMNS:
        if c not in df.columns:
            df[c] = ""

    return df[SHEET_COLUMNS]


# =========================
# GEMINI / RCA ASSISTANT
# =========================

def call_gemini_json(
    prompt: str,
    api_key: str,
    image_file: Optional[Any] = None,
    timeout_sec: int = 60,
) -> Dict[str, Any]:
    """
    เรียก Gemini ผ่าน REST และบังคับ response เป็น JSON
    รองรับแนบภาพ (optional)
    """
    if not api_key:
        raise ValueError("ยังไม่ได้ตั้งค่า GEMINI_API_KEY ใน Environment Variables")

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key}"

    parts: List[Dict[str, Any]] = [{"text": prompt}]

    if image_file is not None:
        try:
            img_bytes = image_file.getvalue()
            mime_type = getattr(image_file, "type", None) or "image/png"
            import base64
            parts.append({
                "inline_data": {
                    "mime_type": mime_type,
                    "data": base64.b64encode(img_bytes).decode("utf-8")
                }
            })
        except Exception:
            # ถ้าอ่านรูปไม่ได้ ยังไม่ให้พังทั้ง flow
            pass

    payload = {
        "contents": [{"parts": parts}],
        "generationConfig": {
            "responseMimeType": "application/json"
        },
        "safetySettings": [
            {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
        ],
    }

    resp = requests.post(url, json=payload, timeout=timeout_sec)
    data = resp.json()

    if not resp.ok:
        err_msg = data.get("error", {}).get("message", f"Gemini API error ({resp.status_code})")
        raise RuntimeError(err_msg)

    text = (
        data.get("candidates", [{}])[0]
        .get("content", {})
        .get("parts", [{}])[0]
        .get("text", "")
    )

    if not text:
        raise RuntimeError("Gemini ไม่ส่งผลลัพธ์กลับมา")

    # clean code fences if any
    cleaned = re.sub(r"^```(?:json)?\s*", "", text.strip(), flags=re.I)
    cleaned = re.sub(r"\s*```$", "", cleaned.strip())

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Gemini ส่ง JSON ไม่ถูกต้อง: {e}\n\nRaw response:\n{cleaned[:2000]}")


def build_analysis_prompt(incident_text: str) -> str:
    return f"""
คุณคือผู้เชี่ยวชาญด้านความปลอดภัยผู้ป่วยและ RCA ในโรงพยาบาล
โปรดวิเคราะห์เหตุการณ์ต่อไปนี้เป็นภาษาไทย และส่งกลับเป็น JSON เท่านั้น (ห้ามมี markdown ห้ามมีข้อความอื่นนอก JSON)

เหตุการณ์:
\"\"\"{incident_text}\"\"\"

โครงสร้าง JSON ที่ต้องการ:
{{
  "event_summary": "สรุปเหตุการณ์แบบกระชับ 2-4 บรรทัด",
  "timeline": [
    "เหตุการณ์ลำดับที่ 1 ...",
    "เหตุการณ์ลำดับที่ 2 ..."
  ],
  "fishbone": {{
    "effect": "เหตุการณ์/ผลลัพธ์สั้นๆ",
    "categories": [
      {{
        "label": "คน",
        "items": ["...", "..."]
      }},
      {{
        "label": "วิธีการ",
        "items": ["...", "..."]
      }}
    ]
  }},
  "five_whys": [
    "ทำไม 1: ...",
    "ทำไม 2: ...",
    "ทำไม 3: ...",
    "ทำไม 4: ...",
    "ทำไม 5: ... (รากสาเหตุ)"
  ],
  "swiss_cheese": [
    {{
      "layer": "นโยบายองค์กร",
      "type": "latent/active",
      "hole": "ช่องโหว่",
      "prevention": "ข้อเสนอป้องกัน"
    }}
  ],
  "contributing_factors": [
    "ปัจจัยเอื้อ 1",
    "ปัจจัยเอื้อ 2"
  ]
}}

ข้อกำหนด:
- fishbone.categories มีได้สูงสุด 6 หมวด
- แต่ละหมวด items สูงสุด 5 ข้อ
- swiss_cheese อย่างน้อย 4 แถว
- five_whys ให้ครบ 5 ข้อ
- ใช้ภาษาไทยล้วน
    """.strip()


def build_plan_prompt(incident_text: str, analysis_json: Dict[str, Any]) -> str:
    analysis_text = json.dumps(analysis_json, ensure_ascii=False)
    return f"""
คุณคือผู้จัดการความปลอดภัยของโรงพยาบาล
จากเหตุการณ์และผลวิเคราะห์ RCA ด้านล่าง โปรดสร้างแผนปฏิบัติการ และส่งกลับเป็น JSON เท่านั้น

เหตุการณ์:
\"\"\"{incident_text}\"\"\"

ผลวิเคราะห์:
{analysis_text}

โครงสร้าง JSON:
{{
  "pdsa": {{
    "plan": ["...","..."],
    "do": ["...","..."],
    "study": ["...","..."],
    "act": ["...","..."]
  }},
  "action_plan": [
    {{
      "measure": "มาตรการ",
      "owner": "ผู้รับผิดชอบ",
      "due": "กำหนดเสร็จ",
      "kpi": "ตัวชี้วัด",
      "risk_control": "ความเสี่ยงและแนวทางลดเสี่ยง"
    }}
  ],
  "initiative_ideas": {{
    "quick_wins_0_30_days": ["...","..."],
    "mid_term_1_3_months": ["...","..."],
    "long_term_3_12_months": ["...","..."]
  }},
  "conclusion_recommendations": [
    "ข้อเสนอแนะสำคัญข้อ 1",
    "ข้อเสนอแนะสำคัญข้อ 2",
    "ข้อเสนอแนะสำคัญข้อ 3",
    "ข้อเสนอแนะสำคัญข้อ 4",
    "ข้อเสนอแนะสำคัญข้อ 5"
  ],
  "next_72_hours": [
    "ก้าวถัดไปภายใน 72 ชั่วโมง ข้อ 1",
    "ก้าวถัดไปภายใน 72 ชั่วโมง ข้อ 2"
  ]
}}

ข้อกำหนด:
- action_plan 3-8 แถว
- recommendation ให้ 5 ข้อพอดี
- ใช้ภาษาไทย
    """.strip()


def fishbone_svg(effect: str, categories: List[Dict[str, Any]]) -> str:
    """
    Executive-friendly fishbone:
    - เน้นอ่านง่ายสำหรับผู้บริหาร
    - แสดง 4 หมวดหลัก (บน 2 / ล่าง 2)
    - แสดงสาเหตุหลักหมวดละ 1-2 ข้อบนภาพ
    - รายละเอียดเต็มให้ดูใน expander ด้านล่าง
    """
    # ===== helper =====
    def esc(s: str) -> str:
        return html.escape(str(s or ""))

    def wrap_text(s: str, n: int = 22, max_lines: int = 4) -> List[str]:
        s = str(s or "").strip()
        if not s:
            return []
        lines, i = [], 0
        while i < len(s) and len(lines) < max_lines:
            lines.append(s[i:i+n])
            i += n
        if i < len(s) and lines:
            lines[-1] = lines[-1][:-1] + "…"
        return lines

    # ===== เตรียมหมวด (คัดให้เหลือ 4 หมวด + top 2 items/หมวด) =====
    raw = categories or []
    if not raw:
        raw = [{"label": "ยังไม่มีข้อมูล", "items": []}]

    # เอาแค่ 4 หมวดแรก (เวอร์ชันผู้บริหาร)
    raw = raw[:4]

    cats = []
    for c in raw:
        items = [str(x) for x in (c.get("items", []) or []) if str(x).strip()]
        cats.append({
            "label": str(c.get("label", "")).strip() or "ไม่ระบุ",
            "items": items[:2],  # แสดงบนภาพแค่ 2 ข้อ
        })

    # ถ้าน้อยกว่า 4 หมวด เติมช่องว่างให้ layout คงรูป
    while len(cats) < 4:
        cats.append({"label": "", "items": []})

    # ===== canvas =====
    W, H = 1500, 820
    spine_y = 410
    spine_x1 = 140

    head_x = 1090
    head_y = 305
    head_w = 350
    head_h = 210

    # จุดต่อกระดูกกับแกนหลัก (fixed layout 4 ช่อง)
    anchors = [
        {"x": 460, "y": 250, "top": True},   # บนซ้าย
        {"x": 810, "y": 250, "top": True},   # บนขวา
        {"x": 560, "y": 570, "top": False},  # ล่างซ้าย
        {"x": 910, "y": 570, "top": False},  # ล่างขวา
    ]

    # ปลายกระดูกชี้ไปทางซ้าย
    end_dx = 220

    # ===== layers =====
    lines_layer = []
    text_layer = []

    # วาดกระดูกแต่ละหมวด
    for i, c in enumerate(cats):
        if not c["label"]:  # ช่องว่าง (padding)
            continue

        a = anchors[i]
        x = a["x"]
        end_y = a["y"]
        is_top = a["top"]
        end_x = x - end_dx

        # กระดูกหลักของหมวด
        lines_layer.append(
            f'<line x1="{x}" y1="{spine_y}" x2="{end_x}" y2="{end_y}" stroke="#334155" stroke-width="3"/>'
        )

        # เวคเตอร์
        dx = end_x - x
        dy = end_y - spine_y
        ln = (dx**2 + dy**2) ** 0.5 or 1
        ux, uy = dx / ln, dy / ln
        px, py = -uy, ux
        if is_top:
            px, py = -px, -py

        # ===== กล่องหัวหมวด (วางปลายสุด) =====
        label_w = 260
        label_h = 40
        label_x = end_x - label_w - 10
        label_y = end_y - 52 if is_top else end_y + 12

        text_layer.append(
            f'<rect x="{label_x}" y="{label_y}" width="{label_w}" height="{label_h}" rx="12" '
            f'fill="#ffffff" stroke="#94a3b8" stroke-width="2"/>'
            f'<text x="{label_x+14}" y="{label_y+26}" font-size="15" font-weight="700" '
            f'font-family="Sarabun, Noto Sans Thai, sans-serif" fill="#0f172a">{esc(c["label"])}</text>'
        )

        # ===== ribs (แสดง 1-2 ข้อแบบอ่านง่าย) =====
        ribs_f = [0.35, 0.58]
        rib_len = 42

        for j, item in enumerate(c["items"][:2]):
            f = ribs_f[j]
            sx = x + dx * f
            sy = spine_y + dy * f
            ex = sx + px * rib_len
            ey = sy + py * rib_len

            # เส้น rib
            lines_layer.append(
                f'<line x1="{sx}" y1="{sy}" x2="{ex}" y2="{ey}" stroke="#64748b" stroke-width="2"/>'
            )

            # กล่องข้อความ rib (กันเส้นทับ)
            tx = ex + px * 8
            ty = ey + (-8 if is_top else 16)

            # จำกัดความยาวข้อความบนรูป
            item_short = str(item).strip()
            if len(item_short) > 38:
                item_short = item_short[:37] + "…"

            bg_w = min(340, max(170, len(item_short) * 7 + 16))
            bg_h = 24
            bg_x = tx - 6
            bg_y = ty - 17

            text_layer.append(
                f'<rect x="{bg_x}" y="{bg_y}" width="{bg_w}" height="{bg_h}" rx="8" '
                f'fill="#ffffff" opacity="0.95"/>'
                f'<text x="{tx}" y="{ty}" font-size="12" '
                f'font-family="Sarabun, Noto Sans Thai, sans-serif" fill="#0f172a">{esc(item_short)}</text>'
            )

    # ===== effect box =====
    effect_lines = wrap_text(effect or "เหตุการณ์ / ผลลัพธ์", n=20, max_lines=5)
    effect_tspan = "".join(
        [f'<tspan x="{head_x + head_w/2}" dy="{0 if idx==0 else 20}">{esc(line)}</tspan>'
         for idx, line in enumerate(effect_lines)]
    )

    svg = f"""
    <svg viewBox="0 0 {W} {H}" width="100%" height="650" xmlns="http://www.w3.org/2000/svg">
      <defs>
        <marker id="arrowHead" markerWidth="14" markerHeight="14" refX="12" refY="7" orient="auto">
          <path d="M0,0 L14,7 L0,14 Z" fill="#0ea5e9"/>
        </marker>
      </defs>

      <!-- spine -->
      <circle cx="{spine_x1}" cy="{spine_y}" r="10" fill="#0f172a"/>
      <line x1="{spine_x1}" y1="{spine_y}" x2="{head_x}" y2="{spine_y}"
            stroke="#0f172a" stroke-width="6" marker-end="url(#arrowHead)"/>

      <!-- lines first -->
      {''.join(lines_layer)}

      <!-- head -->
      <rect x="{head_x}" y="{head_y}" width="{head_w}" height="{head_h}" rx="18"
            fill="#ffffff" stroke="#0f172a" stroke-width="3"/>
      <text x="{head_x + head_w/2}" y="{head_y + 44}" text-anchor="middle"
            font-size="15" font-weight="800"
            font-family="Sarabun, Noto Sans Thai, sans-serif" fill="#0f172a">เหตุการณ์ / ผลลัพธ์</text>

      <text x="{head_x + head_w/2}" y="{head_y + 84}" text-anchor="middle"
            font-size="15" font-weight="700"
            font-family="Sarabun, Noto Sans Thai, sans-serif" fill="#0f172a">
        {effect_tspan}
      </text>

      <!-- text last -->
      {''.join(text_layer)}

      <text x="{spine_x1 - 8}" y="{spine_y - 20}" text-anchor="middle"
            font-size="12" font-weight="700"
            font-family="Sarabun, Noto Sans Thai, sans-serif" fill="#475569">สาเหตุ</text>
    </svg>
    """
    return svg


def render_analysis_result(analysis: Dict[str, Any]):
    st.subheader("🔎 ผลวิเคราะห์ RCA")

    # 1) Summary
    st.markdown("### 1) สรุปเหตุการณ์")
    st.write(analysis.get("event_summary", "-"))

    # 2) Timeline
    st.markdown("### 2) ไทม์ไลน์เหตุการณ์")
    timeline = analysis.get("timeline", []) or []
    if timeline:
        for i, item in enumerate(timeline, 1):
            st.markdown(f"- **{i}.** {item}")
    else:
        st.write("-")

    # 3) Fishbone
    st.markdown("### 3) แผนผังก้างปลา (Ishikawa)")
    fishbone = analysis.get("fishbone", {}) or {}
    effect = fishbone.get("effect", "") or analysis.get("event_summary", "เหตุการณ์ / ผลลัพธ์")
    categories = fishbone.get("categories", []) or []

    svg = fishbone_svg(effect, categories)
    st.markdown("<div class='fishbone-wrap'>", unsafe_allow_html=True)
    components.html(svg, height=580, scrolling=True)
    st.markdown("</div>", unsafe_allow_html=True)
        # ปุ่มดาวน์โหลดแผนผังก้างปลาเป็น JPG
    try:
        jpg_bytes = fishbone_svg_to_jpg_bytes(svg, output_width=2200, jpg_quality=95)
        st.download_button(
            "🖼️ ดาวน์โหลดแผนผังก้างปลา (JPG)",
            data=jpg_bytes,
            file_name=f"fishbone_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg",
            mime="image/jpeg",
            use_container_width=False,
        )
    except Exception as e:
        st.caption(f"ยังไม่สามารถสร้างไฟล์ JPG ได้: {e}")
    def fishbone_svg_to_jpg_bytes(svg_str: str, output_width: int = 2200, jpg_quality: int = 95) -> bytes:
    """
    แปลง SVG (string) -> JPG bytes สำหรับดาวน์โหลดใน Streamlit
    ต้องมีแพ็กเกจ: cairosvg, Pillow
    """
    try:
        import cairosvg
    except ImportError:
        raise RuntimeError("ยังไม่ได้ติดตั้ง cairosvg (เพิ่มใน requirements.txt)")

    try:
        from PIL import Image
    except ImportError:
        raise RuntimeError("ยังไม่ได้ติดตั้ง Pillow (เพิ่มใน requirements.txt)")

    # แปลง SVG -> PNG bytes ก่อน
    png_bytes = cairosvg.svg2png(
        bytestring=svg_str.encode("utf-8"),
        output_width=output_width
    )

    # PNG -> JPG (JPEG ไม่มี transparency จึงแปลงเป็น RGB)
    img = Image.open(BytesIO(png_bytes)).convert("RGB")
    out = BytesIO()
    img.save(out, format="JPEG", quality=jpg_quality, optimize=True)
    out.seek(0)
    return out.getvalue()

    if categories:
        with st.expander("ดูรายละเอียดสาเหตุทั้งหมด (ฉบับเต็ม)"):
            cols = st.columns(2)
            for idx, c in enumerate(categories):
                with cols[idx % 2]:
                    st.markdown(f"**{c.get('label','-')}**")
                    items = c.get("items", []) or []
                    for item in items:
                        st.markdown(f"- {item}")

    # 4) 5 Whys
    st.markdown("### 4) วิเคราะห์ทำไม-ทำไม (5 Whys)")
    whys = analysis.get("five_whys", []) or []
    if whys:
        for i, w in enumerate(whys, 1):
            st.markdown(f"{i}. {w}")
    else:
        st.write("-")

    # 5) Swiss cheese
    st.markdown("### 5) โมเดลสวิสชีส")
    swiss = analysis.get("swiss_cheese", []) or []
    if swiss:
        df_swiss = pd.DataFrame(swiss)
        # rename for display
        display_cols = {
            "layer": "ชั้นระบบ",
            "type": "ประเภท",
            "hole": "รู (ช่องโหว่)",
            "prevention": "มาตรการป้องกัน",
        }
        df_swiss = df_swiss.rename(columns=display_cols)
        st.dataframe(df_swiss, use_container_width=True, hide_index=True)
    else:
        st.write("-")

    # contributing factors
    factors = analysis.get("contributing_factors", []) or []
    if factors:
        st.markdown("### 6) ปัจจัยเอื้อ/ปัจจัยร่วม")
        for f in factors:
            st.markdown(f"- {f}")


def render_plan_result(plan: Dict[str, Any]):
    st.subheader("🎯 แผนปฏิบัติการ / PDSA")

    # PDSA table
    pdsa = plan.get("pdsa", {}) or {}
    pdsa_rows = [
        ["วางแผน (Plan)", "\n".join([f"- {x}" for x in (pdsa.get("plan", []) or [])])],
        ["ทำ (Do)", "\n".join([f"- {x}" for x in (pdsa.get("do", []) or [])])],
        ["ศึกษา (Study)", "\n".join([f"- {x}" for x in (pdsa.get("study", []) or [])])],
        ["ปรับปรุง (Act)", "\n".join([f"- {x}" for x in (pdsa.get("act", []) or [])])],
    ]
    st.markdown("### 1) PDSA")
    st.dataframe(
        pd.DataFrame(pdsa_rows, columns=["ขั้นตอน", "รายละเอียด"]),
        use_container_width=True,
        hide_index=True,
    )

    # Action plan
    st.markdown("### 2) Action Plan")
    ap = plan.get("action_plan", []) or []
    if ap:
        df_ap = pd.DataFrame(ap)
        df_ap = df_ap.rename(columns={
            "measure": "มาตรการ",
            "owner": "ผู้รับผิดชอบ",
            "due": "กำหนดเสร็จ",
            "kpi": "KPI(ตัวชี้วัดผลลัพธ์)",
            "risk_control": "ความเสี่ยงและแนวทางลดเสี่ยง",
        })
        st.dataframe(df_ap, use_container_width=True, hide_index=True)
    else:
        st.write("-")

    # Initiative ideas
    st.markdown("### 3) Initiative Ideas")
    ideas = plan.get("initiative_ideas", {}) or {}
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("**Quick Wins (0–30 วัน)**")
        for x in ideas.get("quick_wins_0_30_days", []) or []:
            st.markdown(f"- {x}")
    with col2:
        st.markdown("**ระยะกลาง (1–3 เดือน)**")
        for x in ideas.get("mid_term_1_3_months", []) or []:
            st.markdown(f"- {x}")
    with col3:
        st.markdown("**ระยะยาว (3–12 เดือน)**")
        for x in ideas.get("long_term_3_12_months", []) or []:
            st.markdown(f"- {x}")

    # Conclusion & next 72h
    st.markdown("### 4) Conclusion & Recommendations")
    for i, x in enumerate(plan.get("conclusion_recommendations", []) or [], 1):
        st.markdown(f"{i}. {x}")

    st.markdown("**ก้าวถัดไป (ภายใน 72 ชั่วโมง)**")
    for x in plan.get("next_72_hours", []) or []:
        st.markdown(f"- {x}")


def build_prefill_texts_from_ai(analysis: Dict[str, Any], plan: Dict[str, Any]) -> Dict[str, str]:
    timeline_lines = analysis.get("timeline", []) or []
    timeline_text = "\n".join([f"{i+1}. {x}" for i, x in enumerate(timeline_lines)])

    # rca_text รวมสาระสำคัญ
    rca_parts = []
    if analysis.get("event_summary"):
        rca_parts.append("สรุปเหตุการณ์:\n" + str(analysis["event_summary"]))

    whys = analysis.get("five_whys", []) or []
    if whys:
        rca_parts.append("5 Whys:\n" + "\n".join([f"- {x}" for x in whys]))

    factors = analysis.get("contributing_factors", []) or []
    if factors:
        rca_parts.append("Contributing Factors:\n" + "\n".join([f"- {x}" for x in factors]))

    swiss = analysis.get("swiss_cheese", []) or []
    if swiss:
        swiss_txt = []
        for row in swiss:
            swiss_txt.append(
                f"- [{row.get('layer','')}] {row.get('hole','')} | ป้องกัน: {row.get('prevention','')}"
            )
        rca_parts.append("Swiss Cheese:\n" + "\n".join(swiss_txt))

    rca_text = "\n\n".join(rca_parts)

    # development plan
    dev_parts = []
    recs = plan.get("conclusion_recommendations", []) or []
    if recs:
        dev_parts.append("ข้อเสนอแนะหลัก:\n" + "\n".join([f"{i+1}. {x}" for i, x in enumerate(recs)]))

    next72 = plan.get("next_72_hours", []) or []
    if next72:
        dev_parts.append("ก้าวถัดไปภายใน 72 ชั่วโมง:\n" + "\n".join([f"- {x}" for x in next72]))

    action_plan = plan.get("action_plan", []) or []
    if action_plan:
        ap_lines = []
        for i, row in enumerate(action_plan, 1):
            ap_lines.append(
                f"{i}) {row.get('measure','')} | ผู้รับผิดชอบ: {row.get('owner','')} | กำหนดเสร็จ: {row.get('due','')}"
            )
        dev_parts.append("Action Plan (สรุป):\n" + "\n".join(ap_lines))

    development_plan_text = "\n\n".join(dev_parts)

    # initial correction (ดึง quick wins เป็นเบื้องต้น)
    qwin = (((plan.get("initiative_ideas") or {}).get("quick_wins_0_30_days")) or [])
    initial_correction = "\n".join([f"- {x}" for x in qwin[:5]])

    return {
        "timeline_text": timeline_text,
        "rca_text": rca_text,
        "development_plan": development_plan_text,
        "initial_correction": initial_correction,
    }


# =========================
# FORM / SAVE
# =========================

def init_form_state_defaults():
    defaults = {
        "form_event_date": date.today(),
        "form_event_time": datetime.now().time().replace(second=0, microsecond=0),
        "form_process_step": PROCESS_OPTIONS[0],
        "form_drug_name": "",
        "form_severity": "A",
        "form_incident_detail": "",
        "form_timeline_text": "",
        "form_initial_correction": "",
        "form_rca_text": "",
        "form_development_plan": "",
        "rca_analysis_json": None,
        "rca_plan_json": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def validate_required_form() -> Tuple[bool, List[str]]:
    errs = []
    if not st.session_state.get("form_drug_name", "").strip():
        errs.append("กรุณากรอกชื่อยา")
    if not st.session_state.get("form_incident_detail", "").strip():
        errs.append("กรุณากรอกรายละเอียดเหตุการณ์")
    return (len(errs) == 0, errs)


def create_record_from_form(uploaded_rca_image: Optional[Any]) -> Dict[str, Any]:
    now = datetime.now()
    event_date_val = st.session_state.get("form_event_date")
    event_time_val = st.session_state.get("form_event_time")

    if isinstance(event_date_val, datetime):
        event_date_str = event_date_val.date().isoformat()
    elif isinstance(event_date_val, date):
        event_date_str = event_date_val.isoformat()
    else:
        event_date_str = str(event_date_val)

    if isinstance(event_time_val, datetime):
        event_time_str = event_time_val.strftime("%H:%M")
    elif isinstance(event_time_val, time):
        event_time_str = event_time_val.strftime("%H:%M")
    else:
        event_time_str = str(event_time_val)

    record = {
        "record_id": now.strftime("%Y%m%d%H%M%S%f"),
        "unit_name": CFG["UNIT_NAME"],
        "app_title": CFG["APP_TITLE"],
        "event_date": event_date_str,
        "event_time": event_time_str,
        "process_step": st.session_state.get("form_process_step", ""),
        "drug_name": st.session_state.get("form_drug_name", "").strip(),
        "severity_level": st.session_state.get("form_severity", ""),
        "incident_detail": st.session_state.get("form_incident_detail", "").strip(),
        "timeline_text": st.session_state.get("form_timeline_text", "").strip(),
        "initial_correction": st.session_state.get("form_initial_correction", "").strip(),
        "rca_text": st.session_state.get("form_rca_text", "").strip(),
        "rca_image_filename": getattr(uploaded_rca_image, "name", "") if uploaded_rca_image else "",
        "development_plan": st.session_state.get("form_development_plan", "").strip(),
        "created_at": now.isoformat(timespec="seconds"),
        "created_by": st.session_state.get("login_username", ""),
    }
    return record


def clear_form_after_save():
    st.session_state.form_drug_name = ""
    st.session_state.form_incident_detail = ""
    st.session_state.form_timeline_text = ""
    st.session_state.form_initial_correction = ""
    st.session_state.form_rca_text = ""
    st.session_state.form_development_plan = ""
    st.session_state.form_process_step = PROCESS_OPTIONS[0]
    st.session_state.form_severity = "A"
    st.session_state.form_event_date = date.today()
    st.session_state.form_event_time = datetime.now().time().replace(second=0, microsecond=0)
    st.session_state.rca_analysis_json = None
    st.session_state.rca_plan_json = None


def render_entry_tab():
    init_form_state_defaults()

    st.markdown("## 📝 บันทึกข้อมูล Medication Error")

    left, right = st.columns([1.15, 1], gap="large")

    # ใช้อัปโหลดภาพ RCA เป็นตัวแปรเดียว เพื่อใช้ทั้งในฟอร์มและส่งเข้า Gemini ได้
    uploaded_rca_image = None

    with left:
        st.markdown("### ข้อมูลเหตุการณ์")

        c1, c2 = st.columns(2)
        with c1:
            st.date_input("วันที่เกิดเหตุ", key="form_event_date")
        with c2:
            st.time_input("เวลาเกิดเหตุ", key="form_event_time")

        st.selectbox("กระบวนการที่เกิด", PROCESS_OPTIONS, key="form_process_step")
        st.text_input("ชื่อยา", key="form_drug_name")
        st.selectbox("ระดับความรุนแรง", SEVERITY_OPTIONS, key="form_severity")
        st.text_area("รายละเอียดเหตุการณ์", height=140, key="form_incident_detail")

        st.markdown("---")
        st.markdown("### ข้อมูลเสริม (ก่อนบันทึก)")

        st.text_area("1) ไทม์ไลน์", height=120, key="form_timeline_text")
        st.text_area("2) การแก้ไขเบื้องต้น", height=100, key="form_initial_correction")

        st.markdown("**3) RCA (ข้อความ + ภาพ)**")
        st.text_area("RCA (ข้อความ)", height=180, key="form_rca_text")
        uploaded_rca_image = st.file_uploader(
            "แนบภาพ RCA (เช่น ก้างปลา / แผนภาพ) - *จะเก็บชื่อไฟล์ในชีต, ไม่เก็บไฟล์ภาพลง Google Sheets*",
            type=["png", "jpg", "jpeg", "webp"],
            key="form_rca_image",
        )

        if uploaded_rca_image is not None:
            st.image(uploaded_rca_image, caption=f"ภาพ RCA: {uploaded_rca_image.name}", use_container_width=True)

        st.text_area("4) แผนพัฒนา", height=140, key="form_development_plan")

        st.markdown("---")
        if st.button("💾 บันทึกข้อมูล", type="primary", use_container_width=True):
            ok, errs = validate_required_form()
            if not ok:
                for e in errs:
                    st.error(e)
            else:
                try:
                    record = create_record_from_form(uploaded_rca_image=uploaded_rca_image)
                    append_record_to_sheet(record)
                    # clear cache so history refreshes
                    load_sheet_df.clear()
                    st.success("บันทึกข้อมูลสำเร็จ ✅")
                    clear_form_after_save()
                    st.rerun()
                except Exception as e:
                    st.exception(e)

    with right:
        st.markdown("### 🤖 RCA Assistant")
        st.caption("ระบบจะวิเคราะห์จากรายละเอียดเหตุการณ์ แล้วแสดงผลให้คัดลอก/กดเติมลงช่องฟอร์มก่อนบันทึก")

        st.info(
            "หลักการใช้งาน: ปุ่ม RCA Assistant จะ **ไม่บันทึกลง Google Sheets** โดยอัตโนมัติ\n"
            "→ ผู้ใช้ตรวจทานผลลัพธ์ แล้วค่อยกด **บันทึกข้อมูล**"
        )

        # ปุ่ม AI
        if st.button("🧠 RCA Assistant", use_container_width=True):
            incident_text = st.session_state.get("form_incident_detail", "").strip()
            if not incident_text:
                st.warning("กรุณากรอกรายละเอียดเหตุการณ์ก่อน")
            else:
                try:
                    with st.spinner("กำลังวิเคราะห์ RCA..."):
                        analysis = call_gemini_json(
                            prompt=build_analysis_prompt(incident_text),
                            api_key=CFG["GEMINI_API_KEY"],
                            image_file=uploaded_rca_image,
                            timeout_sec=90,
                        )
                        plan = call_gemini_json(
                            prompt=build_plan_prompt(incident_text, analysis),
                            api_key=CFG["GEMINI_API_KEY"],
                            timeout_sec=90,
                        )

                        st.session_state.rca_analysis_json = analysis
                        st.session_state.rca_plan_json = plan

                    st.success("วิเคราะห์เสร็จแล้ว ✅")
                except Exception as e:
                    st.error(f"RCA Assistant error: {e}")

        # แสดงผล AI ถ้ามี
        analysis = st.session_state.get("rca_analysis_json")
        plan = st.session_state.get("rca_plan_json")

        if analysis:
            render_analysis_result(analysis)

        if plan:
            st.markdown("---")
            render_plan_result(plan)

        # ปุ่มเติมค่าลงฟอร์ม
        if analysis and plan:
            st.markdown("---")
            if st.button("⬅️ เติมผลลัพธ์ AI ลงช่องฟอร์ม (ไทม์ไลน์ / RCA / แผนพัฒนา)", use_container_width=True):
                filled = build_prefill_texts_from_ai(analysis, plan)
                # เติมเฉพาะถ้ายังว่าง หรือเติมทับ? ที่นี่เลือกเติมทับเพื่อความชัดเจน
                st.session_state.form_timeline_text = filled.get("timeline_text", "")
                st.session_state.form_rca_text = filled.get("rca_text", "")
                st.session_state.form_development_plan = filled.get("development_plan", "")
                # initial correction ดึงจาก quick wins
                if not st.session_state.get("form_initial_correction", "").strip():
                    st.session_state.form_initial_correction = filled.get("initial_correction", "")
                st.success("เติมข้อมูลจาก AI ลงฟอร์มแล้ว ✨")
                st.rerun()


# =========================
# HISTORY TAB (with date fixes)
# =========================

def parse_event_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    แก้ปัญหา date:
    - NaT ใน date_input
    - dtype datetime64[ns] เทียบกับ date ไม่ได้
    """
    out = df.copy()

    # normalize strings
    out["event_date"] = out.get("event_date", "").astype(str).str.strip()
    out["event_time"] = out.get("event_time", "").astype(str).str.strip()

    # parse date safely
    out["_event_date_dt"] = pd.to_datetime(out["event_date"], errors="coerce")

    # parse time (optional)
    # รวมเป็น datetime สำหรับ sort
    out["_event_datetime"] = pd.to_datetime(
        out["event_date"].astype(str) + " " + out["event_time"].astype(str),
        errors="coerce",
    )

    # สำคัญ: ใช้ .dt.date เพื่อเอาไปเทียบกับ st.date_input (ซึ่งเป็น datetime.date)
    out["_event_date_only"] = out["_event_date_dt"].dt.date

    return out


def render_history_tab():
    st.markdown("## 📚 ดูข้อมูลย้อนหลัง")

    try:
        df = load_sheet_df()
    except Exception as e:
        st.error(f"โหลดข้อมูลจาก Google Sheets ไม่สำเร็จ: {e}")
        return

    if df.empty:
        st.info("ยังไม่มีข้อมูลใน Google Sheets")
        return

    df = parse_event_datetime_columns(df)

    # ========== DATE FIXES ==========
    valid_dates_series = df["_event_date_dt"].dropna()
    if valid_dates_series.empty:
        min_d = date.today()
        max_d = date.today()
    else:
        min_d = valid_dates_series.min().date()
        max_d = valid_dates_series.max().date()

    # เผื่อ max < min (ข้อมูลพิลึก)
    if max_d < min_d:
        min_d, max_d = max_d, min_d

    # Filters
    st.markdown("### ตัวกรอง")
    c1, c2, c3, c4 = st.columns([1, 1, 1, 1.4])

    with c1:
        start_date = st.date_input("วันที่เริ่ม", value=min_d, key="hist_start")
    with c2:
        end_date = st.date_input("วันที่สิ้นสุด", value=max_d, key="hist_end")
    with c3:
        sev_selected = st.multiselect(
            "ระดับความรุนแรง",
            options=sorted([x for x in df["severity_level"].dropna().astype(str).unique() if x]),
            default=[],
            key="hist_sev",
        )
    with c4:
        keyword = st.text_input("ค้นหา (ชื่อยา/รายละเอียด)", key="hist_kw").strip()

    proc_selected = st.multiselect(
        "กระบวนการที่เกิด",
        options=sorted([x for x in df["process_step"].dropna().astype(str).unique() if x]),
        default=[],
        key="hist_proc",
    )

    # normalize range
    if start_date > end_date:
        st.warning("วันที่เริ่มมากกว่าวันที่สิ้นสุด ระบบจะสลับให้โดยอัตโนมัติ")
        start_date, end_date = end_date, start_date

    # filter (เปรียบเทียบ date กับ date — แก้ dtype error)
    m = pd.Series(True, index=df.index)

    m &= df["_event_date_only"].notna()
    m &= (df["_event_date_only"] >= start_date) & (df["_event_date_only"] <= end_date)

    if sev_selected:
        m &= df["severity_level"].astype(str).isin(sev_selected)

    if proc_selected:
        m &= df["process_step"].astype(str).isin(proc_selected)

    if keyword:
        kw = keyword.lower()
        m &= (
            df["drug_name"].astype(str).str.lower().str.contains(kw, na=False)
            | df["incident_detail"].astype(str).str.lower().str.contains(kw, na=False)
            | df["rca_text"].astype(str).str.lower().str.contains(kw, na=False)
            | df["development_plan"].astype(str).str.lower().str.contains(kw, na=False)
        )

    filtered = df[m].copy()

    # sort by event datetime desc (fallback created_at)
    filtered["_created_at_dt"] = pd.to_datetime(filtered.get("created_at", ""), errors="coerce")
    filtered = filtered.sort_values(by=["_event_datetime", "_created_at_dt"], ascending=False, na_position="last")

    st.markdown(f"**ผลลัพธ์ทั้งหมด:** {len(filtered):,} รายการ")

    # summary chips
    if not filtered.empty:
        s1, s2, s3 = st.columns(3)
        with s1:
            st.metric("จำนวนรายการ", f"{len(filtered):,}")
        with s2:
            st.metric("จำนวนยาไม่ซ้ำ", f"{filtered['drug_name'].astype(str).replace('', pd.NA).dropna().nunique():,}")
        with s3:
            st.metric("หน่วยงาน", str(filtered["unit_name"].astype(str).replace('', pd.NA).dropna().nunique()))

    display_cols = [
        "event_date", "event_time", "process_step", "drug_name", "severity_level",
        "incident_detail", "timeline_text", "initial_correction", "rca_text",
        "rca_image_filename", "development_plan", "created_at", "created_by"
    ]

    for c in display_cols:
        if c not in filtered.columns:
            filtered[c] = ""

    st.dataframe(
        filtered[display_cols],
        use_container_width=True,
        hide_index=True,
        column_config={
            "event_date": "วันที่",
            "event_time": "เวลา",
            "process_step": "กระบวนการ",
            "drug_name": "ชื่อยา",
            "severity_level": "ระดับ",
            "incident_detail": "รายละเอียดเหตุการณ์",
            "timeline_text": "ไทม์ไลน์",
            "initial_correction": "การแก้ไขเบื้องต้น",
            "rca_text": "RCA (ข้อความ)",
            "rca_image_filename": "ไฟล์ภาพ RCA",
            "development_plan": "แผนพัฒนา",
            "created_at": "เวลาบันทึก",
            "created_by": "ผู้บันทึก",
        }
    )

    # download csv
    csv_bytes = filtered[display_cols].to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        "⬇️ ดาวน์โหลดผลลัพธ์ (CSV)",
        data=csv_bytes,
        file_name=f"med_error_history_{CFG['UNIT_NAME']}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
        mime="text/csv",
        use_container_width=False,
    )

    # detail viewer
    with st.expander("🔍 ดูรายละเอียดรายรายการ (เลือกจากตารางด้านล่างสุด 20 รายการ)"):
        preview = filtered.head(20).copy()
        if preview.empty:
            st.write("ไม่มีข้อมูล")
        else:
            labels = []
            for _, r in preview.iterrows():
                labels.append(
                    f"{r.get('event_date','')} {r.get('event_time','')} | {r.get('drug_name','-')} | ระดับ {r.get('severity_level','-')}"
                )
            selected_idx = st.selectbox("เลือกเหตุการณ์", options=list(range(len(labels))), format_func=lambda i: labels[i])
            row = preview.iloc[int(selected_idx)]

            st.markdown("### รายละเอียดเหตุการณ์")
            st.write(row.get("incident_detail", ""))

            st.markdown("### ไทม์ไลน์")
            st.write(row.get("timeline_text", ""))

            st.markdown("### การแก้ไขเบื้องต้น")
            st.write(row.get("initial_correction", ""))

            st.markdown("### RCA")
            st.write(row.get("rca_text", ""))

            st.markdown("### แผนพัฒนา")
            st.write(row.get("development_plan", ""))

            if str(row.get("rca_image_filename", "")).strip():
                st.caption(f"แนบภาพไว้ตอนบันทึก: {row.get('rca_image_filename')}")


# =========================
# MAIN
# =========================

def render_header():
    st.markdown(f"# 💊 {CFG['APP_TITLE']}")
    st.caption(f"หน่วยงาน: {CFG['UNIT_NAME']}  |  บันทึกจากหน้าเว็บ → Google Sheets (Hybrid)")

    c1, c2 = st.columns([1, 6])
    with c1:
        if st.button("🚪 Logout"):
            st.session_state.authenticated = False
            st.session_state.login_username = ""
            st.rerun()


def check_required_env():
    missing = []
    for key in ["GSHEET_URL", "GCP_SERVICE_ACCOUNT_JSON"]:
        if not CFG.get(key):
            missing.append(key)

    if missing:
        st.error("ยังตั้งค่า Environment Variables ไม่ครบ: " + ", ".join(missing))
        st.stop()


def main():
    ensure_auth_state()

    if not st.session_state.authenticated:
        render_login()
        return

    check_required_env()

    render_header()
    st.markdown("---")

    tab1, tab2 = st.tabs(["บันทึกข้อมูล", "ดูข้อมูลย้อนหลัง"])

    with tab1:
        render_entry_tab()

    with tab2:
        render_history_tab()


if __name__ == "__main__":
    main()
