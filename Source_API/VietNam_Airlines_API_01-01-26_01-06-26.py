import requests
import csv
from datetime import datetime, timedelta
import sys
import time
import random
import os


def safe_print(msg):
    try:
        print(msg, flush=True)
    except UnicodeEncodeError:
        ascii_msg = msg.encode('ascii', errors='replace').decode('ascii')
        print(ascii_msg)
    except Exception:
        print("[ERROR] Could not display message")


start_date = datetime.strptime("2025-06-12", "%Y-%m-%d")
end_date = datetime.strptime("2025-12-31", "%Y-%m-%d")
output_path = "CXR_HUI_12-06-25_31-12-25.csv"

headers = {"accept": "application/json",
           "accept-encoding": "gzip, deflate, br, zstd",
           "accept-language": "vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5",
           "ama-client-facts": "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJzdWIiOiJmYWN0IiwiY291bnRyeUNvZGUiOiJWTiIsImxhbmd1YWdlIjoidmktVk4ifQ.",
           "ama-client-ref": "8614e81e-2123-4ca4-aab4-2edf64071285:2",
           "authorization": "Bearer sdryTJHw231qwy4LZZ8xNyj7qv9N",
           "cache-control": "max-age=0",
           "content-type": "application/json",
           "origin": "https://booking.vietnamairlines.com",
           "priority": "u=1, i",
           "referer": "https://booking.vietnamairlines.com/",
           "sec-ch-ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
           "sec-ch-ua-mobile": "?0",
           "sec-ch-ua-platform": '"Windows"',
           "sec-fetch-dest": "empty",
           "sec-fetch-mode": "cors",
           "sec-fetch-site": "same-site",
           "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
           "x-d-token": "3:eNnby3fVncxtIb2RYZmcJw==:/1XFVyDJo8RSGO2r9jebfVlF2luDO42RPa+CgggB9/bH18kCQxfbMzRDwpXN6prMVAYCoJjI+iUL//ftXF6C6TzryN5EgWGx2YTTtpEU5w1EHc/pa7o3dgpg7YUpdN3WaeVP4aJ/qL4B3JydBfGipj8zDUzD2BKWKfqHgXVCnhspLDwEm2tJEhak7Zv24ICetEDSktI/XJwKHqChxX4TnGZnaQ2TArPZT+hnjdGCsi//ZfxTn4cH/bpIOhnrhmPzbjSXmLwsgpz562kasjb/DC1/6y1p7FaLEL/KkNTNfrsmYvqggjW7zquj17pS1neL3jZq20OdKvNS9RndborYovCa1RAaleL3ToNZC4yoKvA9XjSZ05WsWZvSEluxkUsHQJxI9yMUUSPIj4ji61HwuUOctfOuaLVL1o3ojy2kryRuuObbuQ27QO3uV7o+sENafnMJHo2Q4oNhiJ1+Q5i+0PfcozNIF9rTsfVpssTHUlBd8ATfgjeF1ljF4bIf/skxJZ3Ecd8rw+tKSjPFX5UGSmp1K5qVVxLQrouIqcZipzTSVGEsTz5G4ksYvhFzQc+uXCiteG2aoXhTWxzQJxbRUHS8e9BG8wDO1tr44iltYWTSPzZo8v1CBcU3exMtoFbf22kh+6aDEf3bijasswQpvv63nwR3/V2mGuBdG3Qxf0ji6P1fnLOncPvwRC+aoujgtni1UGr0ed6lTcEvF7lJdL0/ObcSwsd2wklJwntWAW30PynAkVhrstUY9RYaopY26f3+FPCGqWrjmQ0IY4OXGRz7S5yMX1FbLJHJe+y4BNkOAOoMzPBc/Sgt3j/lV+AtH9E2cyarVQPsk5nfevfXA8pcWCvS51jA45lFqdmBQBNb8k0KzFX0llpG2vHBtW+IZOnPPoYCQ+7eK3g9CsJs6w==:m5SqwOkMvzlVhD5xi2de191/WxPkx6FF2dgMfWsUy5s="}


def get_fresh_session():
    session = requests.Session()
    session.headers.update(headers)
    return session


def make_request_with_retry(session, payload, max_retries=3):
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                delay = (2 ** attempt) + random.uniform(1, 3)
                safe_print(f"Retrying in {delay:.1f} seconds...")
                time.sleep(delay)

            response = session.post(
                "https://api-des.vietnamairlines.com/v2/search/air-bounds",
                json=payload,
                timeout=30
            )

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 403:
                safe_print(f"ERROR 403: Access forbidden. Token may have expired.")
                return None
            elif response.status_code == 429:
                safe_print(f"ERROR 429: Rate limited. Waiting longer...")
                time.sleep(60)
                continue
            else:
                safe_print(f"ERROR {response.status_code}: {response.text[:200]}")

        except requests.exceptions.RequestException as e:
            safe_print(f"Request error (attempt {attempt + 1}): {e}")

    return None


dates_to_check = []
curr = start_date
while curr <= end_date:
    dates_to_check.append(curr.strftime("%Y-%m-%dT00:00:00.000"))
    curr += timedelta(days=1)

routes = [
    ("CXR", "HUI")
]
session = get_fresh_session()

try:
    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Date", "Route", "Flight_ID", "Origin", "Destination", "Fare_Class", "Fare_Code", "Price_VND", "Taxes",
            "Departure_Time", "Arrival_Time", "Booking_Class", "Cabin", "Quota", "Status", "Service_Code",
            "Fare_Conditions", "Operating_Airline_Code", "Operating_Airline_Name"
        ])

        total_requests = len(routes) * len(dates_to_check)
        current_request = 0
        successful_requests = 0
        failed_requests = 0

        for origin, dest in routes:
            for date_str in dates_to_check:
                current_request += 1
                safe_print(f"[{current_request}/{total_requests}] Processing {date_str[:10]} {origin}->{dest}")

                payload = {
                    "commercialFareFamilies": ["WEB"],
                    "itineraries": [{
                        "departureDateTime": date_str,
                        "originLocationCode": origin,
                        "destinationLocationCode": dest,
                        "isRequestedBound": True
                    }],
                    "travelers": [{"passengerTypeCode": "ADT"}],
                    "searchPreferences": {"showMilesPrice": False}
                }

                data = make_request_with_retry(session, payload)

                if data is None:
                    failed_requests += 1
                    continue

                if 'data' not in data or 'airBoundGroups' not in data['data']:
                    continue

                try:
                    for group in data['data']['airBoundGroups']:
                        bound = group['boundDetails']
                        segment = bound['segments'][0]
                        flight_id = segment.get('flightId', '')
                        dep_time = segment.get('departureDateTime', '')
                        arr_time = segment.get('arrivalDateTime', '')
                        airline_code = "VN"
                        airline_name = "VietNam Airlines"

                        for offer in group['airBounds']:
                            try:
                                price = offer['prices']['totalPrices'][0]['total']
                                taxes = offer['prices']['totalPrices'][0].get('totalTaxes', '')
                                fare_code = offer.get('fareFamilyCode', '')

                                booking_class = ''
                                cabin = ''
                                quota = ''
                                status = ''
                                fare_class = ''
                                service_code = ''
                                fare_conditions = ''

                                if 'availabilityDetails' in offer and offer['availabilityDetails']:
                                    details = offer['availabilityDetails'][0]
                                    booking_class = details.get('bookingClass', '')
                                    cabin = details.get('cabin', '')
                                    quota = details.get('quota', '')
                                    status = details.get('statusCode', '')

                                if 'fareInfos' in offer and offer['fareInfos']:
                                    fare_class = offer['fareInfos'][0].get('fareClass', '')

                                if 'services' in offer and offer['services']:
                                    service_code = offer['services'][0].get('serviceCode', '')

                                if 'fareConditionsCodes' in offer:
                                    fare_conditions = ','.join(offer['fareConditionsCodes'])

                                writer.writerow([
                                    date_str[:10], f"{origin}_{dest}", flight_id, origin, dest, fare_code, fare_class,
                                    price, taxes, dep_time, arr_time, booking_class, cabin, quota, status, service_code,
                                    fare_conditions, airline_code, airline_name
                                ])

                            except (KeyError, IndexError):
                                continue

                    successful_requests += 1

                except Exception as e:
                    failed_requests += 1

                time.sleep(random.uniform(2, 4))

        safe_print("-" * 60)
        safe_print(f"SCRAPING COMPLETED!")
        safe_print(f"Total requests: {total_requests}")
        safe_print(f"Successful: {successful_requests}")
        safe_print(f"Failed: {failed_requests}")
        safe_print(f"Success rate: {(successful_requests / total_requests) * 100:.1f}%")
        safe_print(f"Data saved to: {output_path}")

except IOError as file_err:
    safe_print(f"ERROR: File operation failed: {file_err}")
except KeyboardInterrupt:
    safe_print("\nScript interrupted by user")
except Exception as general_err:
    safe_print(f"ERROR: General error: {general_err}")

safe_print("Script execution completed.")