import requests
import csv
import time
import random
from datetime import datetime
from pathlib import Path

# =========================
# 🔥 수집 대상 (송파구 + 세부동)
# =========================
LOCAL_CODES = [
    "11710101","11710102","11710103","11710104","11710105",
    "11710106","11710107","11710108","11710109","11710110",
    "11710111","11710112","11710113"
]

LOCAL_NAME = "송파구"
PAGE_LIMIT = 20

# =========================
# 📁 파일 설정
# =========================
BASE_DIR = Path("data")
BASE_DIR.mkdir(parents=True, exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
OUT_CSV   = BASE_DIR / f"zigbang_{LOCAL_NAME}_{timestamp}.csv"
LOG_FILE  = BASE_DIR / f"zigbang_{LOCAL_NAME}_{timestamp}_log.txt"

# =========================
# 🔤 매핑
# =========================
TRAN_TYPE_KR = {"trade": "매매", "charter": "전세", "rental": "월세"}
DIRECTION_KR = {
    "e": "동향","w": "서향","s": "남향","n": "북향",
    "se": "남동향","sw": "남서향","ne": "북동향","nw": "북서향",
}

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "ko-KR,ko;q=0.9",
    "origin": "https://www.zigbang.com",
    "referer": "https://www.zigbang.com/",
    "sdk-version": "0.112.0",
    "user-agent": "Mozilla/5.0",
    "x-zigbang-platform": "www",
}

# =========================
# 로그
# =========================
def log(msg):
    stamp = datetime.now().strftime("%H:%M:%S")
    line = f"[{stamp}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def sleep():
    time.sleep(1 + random.uniform(0, 0.3))

# =========================
# 🔥 API 호출
# =========================
def fetch_by_local(local_code):

    url = f"https://apis.zigbang.com/apt/locals/{local_code}/item-catalogs"

    all_items = []
    offset = 0

    while True:

        params = [
            ("tranTypeIn[]", "trade"),
            ("tranTypeIn[]", "charter"),
            ("tranTypeIn[]", "rental"),
            ("includeOfferItem", "true"),
            ("offset", offset),
            ("limit", PAGE_LIMIT),
        ]

        try:
            res = requests.get(url, headers=HEADERS, params=params, timeout=15)
        except Exception as e:
            log(f"❌ 요청 오류: {e}")
            break

        if res.status_code != 200:
            log(f"❌ {local_code} 실패: {res.status_code}")
            break

        data = res.json()
        items = data.get("list") or []

        if offset == 0:
            total = data.get("count", 0)
            log(f"{local_code} 전체 매물: {total}")

        if not items:
            break

        all_items.extend(items)

        log(f"{local_code} offset={offset} → {len(items)}개 (누적 {len(all_items)})")

        if len(items) < PAGE_LIMIT:
            break

        offset += PAGE_LIMIT
        sleep()

    return all_items

# =========================
# 🔧 데이터 정제
# =========================
def parse_row(item):

    size_m2 = item.get("sizeM2") or 0
    deposit = item.get("depositMin", 0) or 0
    rent = item.get("rentMin", 0) or 0

    rt = item.get("roomTypeTitle") or {}
    tran = item.get("tranType", "")

    item_id = ""
    item_list = item.get("itemIdList") or [{}]
    if item_list and isinstance(item_list[0], dict):
        item_id = item_list[0].get("itemId", "")

    return {
        "수집시간": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "단지명": item.get("areaDanjiName"),
        "구": item.get("local2"),
        "동": item.get("local3"),
        "거래유형": TRAN_TYPE_KR.get(tran, ""),
        "층": item.get("floor"),
        "면적(m2)": size_m2,
        "평": round(size_m2/3.3058,2) if size_m2 else "",
        "보증금": deposit,
        "월세": rent,
        "가격(억)": round(deposit/10000,2) if deposit else 0,
        "타입": rt.get("p") if isinstance(rt, dict) else "",
        "방향": DIRECTION_KR.get(item.get("direction","")),
        "매물ID": item_id,
        "URL": f"https://www.zigbang.com/home/apt/items/{item_id}"
    }

# =========================
# 💾 저장
# =========================
def save_csv(rows):

    rows = sorted(rows, key=lambda r: (r["동"], r["단지명"], r["가격(억)"]))

    with open(OUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    log(f"💾 저장 완료: {OUT_CSV}")

# =========================
# 🚀 실행
# =========================
def main():

    log("==== 송파구 매물 수집 시작 ====")

    all_items = []

    for code in LOCAL_CODES:
        log(f"\n📍 {code} 수집 시작")
        items = fetch_by_local(code)
        all_items.extend(items)

    if not all_items:
        log("❌ 데이터 없음")
        return

    # 중복 제거
    seen = set()
    rows = []

    for item in all_items:
        row = parse_row(item)
        uid = row["매물ID"]

        if uid and uid not in seen:
            seen.add(uid)
            rows.append(row)

    log(f"\n총 수집: {len(all_items)} → 중복 제거: {len(rows)}")

    save_csv(rows)

    log("✅ 완료!")

if __name__ == "__main__":
    main()