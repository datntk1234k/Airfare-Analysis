#!/usr/bin/env python3
"""
Thu thập giá rẻ nhất mỗi ngày của Vietjet Air
Ví dụ mặc định: SGN  →  HAN  (01/06/2025 → 01/06/2026)
"""

import os, csv, time, requests
from datetime import date
from dateutil.rrule import rrule, DAILY
from tqdm import tqdm

# ========================= CẤU HÌNH =========================
ORIGIN      = "SGN"
DESTINATION = "HAN"

from datetime import timedelta, date
START_DATE  = date(2025, 6, 1)
END_DATE    = START_DATE + timedelta(days=4)   # 5 ngày liên tiếp

OUTFILE = (
    "/Users/nthanhdat/Documents/Fpt_Polytechnic/"
    "Dự án tốt nghiệp/Dataset/vietjet_SGN_HAN_5days.csv"
)
# ============================================================

def get_token():
    """Lấy OAuth2 access-token (hết hạn ~30-60 phút)."""
    r = requests.post(
        "https://api.amadeus.com/v1/security/oauth2/token",
        data={
            "grant_type":    "client_credentials",
            "client_id":     os.getenv("AMA_CLIENT"),
            "client_secret": os.getenv("AMA_SECRET"),
        },
        timeout=15,
    )
    r.raise_for_status()
    return r.json()["access_token"]

def flight_offers(token, fly_date):
    """Gọi Flight Offers Search cho một ngày."""
    params = {
        "originLocationCode":      ORIGIN,
        "destinationLocationCode": DESTINATION,
        "departureDate":           fly_date.isoformat(),
        "adults":                  1,
        "currencyCode":            "VND",
        "max":                     50,
    }
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(
        "https://api.amadeus.com/v2/shopping/flight-offers",
        headers=headers, params=params, timeout=20
    )
    # token hết hạn ⇒ 401
    if r.status_code == 401:
        return None
    r.raise_for_status()
    return r.json().get("data", [])

def daterange(start, end):
    for d in rrule(DAILY, dtstart=start, until=end):
        yield d.date()

def main():
    token = get_token()
    fields = ["search_date", "flight", "depart", "arrive",
              "price_vnd", "cabin", "seats", "booking_code"]

    with open(OUTFILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fields)
        writer.writeheader()

        for day in tqdm(list(daterange(START_DATE, END_DATE)), ncols=90):
            data = flight_offers(token, day)
            if data is None:           # token cũ hết hạn
                token = get_token()
                data  = flight_offers(token, day)

            for offer in data:
                if "VJ" not in offer["validatingAirlineCodes"]:
                    continue           # bỏ hãng khác

                seg  = offer["itineraries"][0]["segments"][0]
                fare = offer["travelerPricings"][0]["fareDetailsBySegment"][0]

                writer.writerow({
                    "search_date": day.isoformat(),
                    "flight":       seg["carrierCode"] + seg["number"],
                    "depart":       seg["departure"]["at"][11:16],
                    "arrive":       seg["arrival"]["at"][11:16],
                    "price_vnd":    offer["price"]["total"],
                    "cabin":        fare["cabin"],
                    "seats":        offer["numberOfBookableSeats"],
                    "booking_code": fare["fareBasis"],
                })
            time.sleep(0.3)            # 3 req/s  < quota

    print(f"✅ Đã xong! File lưu tại: {OUTFILE}")

if __name__ == "__main__":
    main()
